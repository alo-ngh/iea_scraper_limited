import re
import pytz
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT, EXT_DB_STR
from iea_scraper.core.job import BaseJob

URL = 'https://meritindia.in/Dashboard/BindAllIndiaMap'

FILE_PREFIX = "in_meritindia"
ROOT_DIR = FILE_STORE_PATH / FILE_PREFIX
HISTORY_FILE = ROOT_DIR / f'{FILE_PREFIX}_history.csv'

COLUMNS_TO_KEEP = ['timestamps',
                   'demand_met',
                   'thermal_generation',
                   'gas_generation',
                   'nuclear_generation',
                   'hydro_generation',
                   'renewable_generation']

logger = logging.getLogger(__name__)
india_tz = pytz.timezone('Asia/Kolkata')

TABLE_SCHEMA = 'main'
FINAL_TABLE_NAME = 'meritindia_data'
TEMP_TABLE_NAME = '#meritindia_tomerge'


class InElecConsJob(BaseJob):
    """
        Class for loading data from meritindia.in.
        History is loaded from a file (in_meritindia_history.csv).
    """
    title: str = "India - Real-time India electricity consumption"

    def __init__(self, **kwargs):
        """"
        Constructor.
        @param **kwargs: pass-through parameters to super.__init__().
        """
        super().__init__(**kwargs)
        self.file = None
        self.data = None

    def download_file(self):
        """
        Download the latest data from http://meridindia.in.
        :return: None.
        """
        logger.info(f"Downloading data from {URL}")
        response = requests.get(URL, proxies=PROXY_DICT)

        if not response.ok:
            raise Exception(f"Download Error \n url: {response.url} \n status_code: {response.status_code}")

        # build current filename
        # thing to take into account: Indian time zone is GMT+5:30
        dt_now = datetime.now(india_tz).replace(second=0, microsecond=0)
        filename = f"{FILE_PREFIX}_{dt_now.strftime('%Y%m%d%H%M')}.html"

        daily_dir = ROOT_DIR / dt_now.strftime('%Y') / dt_now.strftime('%m') / dt_now.strftime('%d')
        # check if directory exists
        if not daily_dir.exists():
            logger.debug(f"Creating directory {daily_dir}.")
            daily_dir.mkdir(parents=True)

        self.file = daily_dir / filename
        logger.info(f"Writing site content to {str(self.file)}")
        self.file.write_bytes(response.content)

    def parse_meritindia_html(self, file):
        """
        Parses the raw HTML to extract the following indicators:
        - demand met,
        - thermal_generation,
        - gas_generation,
        - nuclear_generation,
        - hydro_generation,
        - renewable_generation
        :return: a data frame with the data
        """
        logger.info(f"Parsing the content of {str(file)}")
        content = file.read_bytes()
        df = pd.read_html(content)[1]

        df.columns = ['demand_met', 'thermal_generation', 'gas_generation', 'nuclear_generation', 'hydro_generation',
                      'renewable_generation']
        # gets only the number and remove commas from indian numeric notation
        df = df.applymap(lambda x: int(re.search(r'([\d,]+)', x).group(0).replace(',', '')))

        # get timestamp from filename (in_meritindia_yyyymmddhhmmss.html)
        ts = datetime.strptime(file.stem.split('_')[-1], '%Y%m%d%H%M')
        # maybe not needed to localize here?
        # in_date = india_tz.localize(ts)
        df['timestamps'] = ts

        # set timestamps as the first column
        df = df[df.columns[-1:].append(df.columns[:-1])]
        return df

    def read_history_file(self):
        """
        Read data from history file.
        :return: a data frame with historical data.
        """
        logger.info(f"Reading history file: {str(HISTORY_FILE)}")
        df = pd.read_csv(HISTORY_FILE,
                         parse_dates=['timestamps'],
                         date_parser=lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        logger.info(f"{len(df)} rows read from {str(HISTORY_FILE)}")
        return df[COLUMNS_TO_KEEP]

    def load_data(self, download=True):
        """
        Loads the content of the currently downloaded file to the database.
        if full_load is True, loads all existing files.
        :return: None
        """
        dfs = []
        if self.full_load:
            logger.info("Full load requested.")
            dfs = [self.read_history_file()]

            file_mask = f'**/{FILE_PREFIX}_*.html'
            logger.debug(f'Loading from file store files with mask {file_mask}')
            dfs += [self.parse_meritindia_html(file) for file in ROOT_DIR.glob(file_mask)]

        if download:
            self.download_file()
            dfs += [self.parse_meritindia_html(self.file)]

        self.data = pd.concat(dfs)

    def build_merge_query(self):
        """
        Build the merge query.
        :return: str with the merge query.
        """
        merge_difference = [f'target.{col} <> source.{col}' for col in COLUMNS_TO_KEEP if col != 'timestamps']
        merge_update = [f'target.{col} = source.{col}' for col in COLUMNS_TO_KEEP if col != 'timestamps']
        merge_insert = [f'source.{col}' for col in COLUMNS_TO_KEEP]

        query = (f'MERGE {TABLE_SCHEMA}.{FINAL_TABLE_NAME} target \n'
                 f'USING {TABLE_SCHEMA}.{TEMP_TABLE_NAME} as source \n'
                 'ON (target.timestamps = source.timestamps) \n'
                 f"WHEN MATCHED AND ({' OR '.join(merge_difference)})\n"
                 f"THEN UPDATE SET {', '.join(merge_update)}\n"
                 f'WHEN NOT MATCHED \n'
                 f"THEN INSERT ({', '.join(COLUMNS_TO_KEEP)}) \n"
                 f"VALUES ({', '.join(merge_insert)});")

        logger.debug(f'Merge query: {query}')
        return query

    def write_to_db(self):
        """
        Upserts current data into the database based on timestamps as key.
        :return: None
        """
        logger.info("Writing to database.")
        if self.data is None:
            logger.info("No data to load into the database.")
            return

        logger.debug(f'Datebase: {EXT_DB_STR}')
        engine = create_engine(
            EXT_DB_STR,
            fast_executemany=True)
        # engine.begin() starts a transaction
        with engine.begin() as con:
            if self.full_load:
                logger.debug('Sending truncate table statement')
                rs = con.execute(f"IF OBJECT_ID('{TABLE_SCHEMA}.{FINAL_TABLE_NAME}','U') is not null "
                                 f"TRUNCATE TABLE {TABLE_SCHEMA}.{FINAL_TABLE_NAME}")

                logger.info(f'Loading {len(self.data)} rows to database.')
                self.data.to_sql(FINAL_TABLE_NAME, con=con, schema=TABLE_SCHEMA, index=False, if_exists='append')
            else:
                # loads data into temporary table and then merge with final table.
                logger.debug(f'Loading {len(self.data)} rows to temporary table {TEMP_TABLE_NAME}')
                self.data.to_sql(TEMP_TABLE_NAME, con=con, schema=TABLE_SCHEMA, index=False, if_exists='append')

                logger.debug(f'Merging data from {TEMP_TABLE_NAME} into {FINAL_TABLE_NAME}')
                merge_query = self.build_merge_query()
                rs = con.execute(merge_query)

    def run(self, download=True):
        """
        This method load the data into a data frame in self.data
        and writes the data to the database.
        :param download: download the current data if True.
        :return: None
        """
        self.load_data(download)
        self.write_to_db()

    @staticmethod
    def convert_old_file(f: Path):
        """
        Convert old files that saved the whole page to the new format saving only the table.
        :param f: path to file
        :return: NoReturn
        """

        logger.info(f'reading file {f}')
        content = f.read_text()

        new_content = BeautifulSoup(content, 'html.parser').find_all(id='AllIndiaMap')[0].decode_contents()

        logger.info(f'writing back new content to: {f}')
        f.write_text(new_content)
