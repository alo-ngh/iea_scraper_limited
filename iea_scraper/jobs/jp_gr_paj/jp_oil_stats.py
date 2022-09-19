import logging
from typing import NoReturn, List

import re
import requests
from retry import retry
from copy import copy
from bs4 import BeautifulSoup
import datetime
from pathlib import Path
from requests.auth import HTTPDigestAuth
from sqlalchemy import create_engine

import pandas as pd

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize, parallelize, calc_checksum_download
from iea_scraper.settings import PAJ_USERNAME, PAJ_PASSWORD, PROXY_DICT, FILE_STORE_PATH, EXT_DB_STR

logger = logging.getLogger(__name__)


class JpOilStatsJob(ExtDbApiJob):
    """
    Scraper for PAJ Oil Statistics Weekly.
    Japanese Oil Statistics weekly from Petroleum Association of Japan.

    Source: https://stats.paj.gr.jp/en/d-member/index.html

    Authentication: HTTP Digest authentication.

    Japanese weekly data are published usually on Wednesdays lunchtime. More details and schedule can be found on
    Petroleum Association of Japan Weekly data website.

    We subscribe to Member data, Data for Analyzing course member (Charged).

    Data is separated by year (2018, 2019 and 2020) and the region (All Japan, East Japan and West Japan).
    e use All Japan data only.

    Column names are defined in the template file: https://stats.paj.gr.jp/en/d-member/csvs/FormY_EP.xlt
    """
    title: str = 'PAJ - Oil Statistics Weekly All Japan'

    # base website URL
    base_url = 'https://stats.paj.gr.jp/en/d-member'
    # maximum number of attempts to download the webpage
    max_retry = 3
    # delay between each attempt in seconds
    retry_delay = 5
    # current year
    current_year = datetime.date.today().year
    # text inside the html links (<a href='url'>text</a>) to each file. Used to filter the files of interest
    # for now, we just take All Japan
    files_scope = 'All Japan'
    # source prefix (all_japan)
    source_code = files_scope.lower().replace(' ', '_')
    # provider code: the directory just after scraper/jobs (jp_gr_paj)
    provider_code = Path(__file__).parent.parts[-1]
    # file prefix (jp_gr_paj_all_japan)
    file_prefix = f'{provider_code}_{source_code}'

    # maximum number of workers in parallel processing
    max_worker = 3

    # symbol used for null values in CSV files
    csv_null_values = 'n.a.'

    # column headers for each source file
    csv_columns = {'Current Week': str,
                   'Refinery Operations - Crude Input(kl)': float,
                   'Refinery Operations - Weekly Average Capacity(BPSD)': float,
                   'Refinery Operations - Util. Rate against BPSD': float,
                   'Refinery Operations - Designed Capacity(BPCD)': float,
                   'Refinery Operations - Util. Rate against BPCD': float,
                   'Products Stocks(kl) - Crude Oil': float,
                   'Products Stocks(kl) - Gasoline': float,
                   'Products Stocks(kl) - Naphtha': float,
                   'Products Stocks(kl) - Jet': float,
                   'Products Stocks(kl) - Kerosene': float,
                   'Products Stocks(kl) - Gas Oil(Diesel)': float,
                   'Products Stocks(kl) - LSA': float,
                   'Products Stocks(kl) - HSA': float,
                   'Products Stocks(kl) - AFO': float,
                   'Products Stocks(kl) - LSC': float,
                   'Products Stocks(kl) - HSC': float,
                   'Products Stocks(kl) - CFO': float,
                   'Products Stocks(kl) - Total': float,
                   'Unfinished Oil Stocks(kl) - Unfinished Gasoline': float,
                   'Unfinished Oil Stocks(kl) - Unfinished Kerosene': float,
                   'Unfinished Oil Stocks(kl) - Unfinished Gas Oil': float,
                   'Unfinished Oil Stocks(kl) - Unfinished AFO': float,
                   'Unfinished Oil Stocks(kl) - Feed Stocks': float,
                   'Unfinished Oil Stocks(kl) - Total': float,
                   'Refinery Production(kl) - Gasoline': float,
                   'Refinery Production(kl) - Naphtha': float,
                   'Refinery Production(kl) - Jet': float,
                   'Refinery Production(kl) - Kerosene': float,
                   'Refinery Production(kl) - Gas Oil(Diesel)': float,
                   'Refinery Production(kl) - LSA': float,
                   'Refinery Production(kl) - HSA': float,
                   'Refinery Production(kl) - AFO': float,
                   'Refinery Production(kl) - LSC': float,
                   'Refinery Production(kl) - HSC': float,
                   'Refinery Production(kl) - CFO': float,
                   'Refinery Production(kl) - Total': float,
                   'Imports(kl) - Gasoline': float,
                   'Imports(kl) - Naphtha': float,
                   'Imports(kl) - Jet': float,
                   'Imports(kl) - Kerosene': float,
                   'Imports(kl) - Gas Oil(Diesel)': float,
                   'Imports(kl) - LSA': float,
                   'Imports(kl) - HSA': float,
                   'Imports(kl) - AFO': float,
                   'Imports(kl) - LSC': float,
                   'Imports(kl) - HSC': float,
                   'Imports(kl) - CFO': float,
                   'Imports(kl) - Total': float,
                   'Exports(kl) - Gasoline': float,
                   'Exports(kl) - Naphtha': float,
                   'Exports(kl) - Jet': float,
                   'Exports(kl) - Kerosene': float,
                   'Exports(kl) - Gas Oil(Diesel)': float,
                   'Exports(kl) - LSA': float,
                   'Exports(kl) - HSA': float,
                   'Exports(kl) - AFO': float,
                   'Exports(kl) - LSC': float,
                   'Exports(kl) - HSC': float,
                   'Exports(kl) - CFO': float,
                   'Exports(kl) - Total': float}

    # key column name
    key_column_name = 'First Day of Week'

    # DB schema
    db_schema = 'main'
    db_final_table = 'paj_jp_oil_stats_weekly_data'
    db_temp_table = '#paj_jp_oil_stats_weekly_temp'

    def __init__(self, **kwargs) -> NoReturn:
        """
        Constructor.
        We authenticate into the website and keep the session object.

        It defines session object with authentication parameters

        :param kwargs: forward parameters to super class.
        """
        super().__init__(**kwargs)
        logger.debug('Creating a requests.Session() object with authentification credentials in self.session')
        self.session = requests.Session()
        self.session.auth = HTTPDigestAuth(PAJ_USERNAME, PAJ_PASSWORD)

    def get_sources(self) -> NoReturn:
        """
        This method fills in self.source with the list of source files to load.
        We get the list of existing files from the website.
        We load the current-year file.
        If full_load flag is True, we load the remaining existing files from the
        website and a local excel file containing the history.

        :return: NoReturn
        """
        logger.info('Defining list of files to load.')
        # get available files in website
        front_page_url = f'{self.base_url}/index.html'
        front_page = self._http_session_get(front_page_url).text
        file_list = self._parse_file_list(front_page)

        # not in full-load, it will load the files available in the website
        for file_url in file_list:
            # extract year from filename
            match = re.search('([0-9]{4})\.csv$', file_url)
            str_year = match.group(1)
            logger.debug(f'Extracted year {str_year} from {file_url}')

            self.sources.append(BaseSource(code=f'{self.file_prefix.upper()}_{str_year}',
                                           url=f'{self.base_url}/{file_url}',
                                           path=f'{self.file_prefix}_{str_year}.csv',
                                           long_name=f"{self.provider_code} "
                                                     f"Japan Oil Stats Weekly {str_year}"))

        if self.full_load:
            # add historical file (2008)
            self.source_complements.append(BaseSource(code=f'{self.file_prefix.upper()}_2008',
                                                      url=f'(local file)',
                                                      path=f'{self.file_prefix}_2008.csv',
                                                      long_name=f"{self.provider_code} "
                                                                f"Japan Oil Stats Weekly History from 2008"))
            # add historical file (2004)
            self.source_complements.append(BaseSource(code=f'{self.file_prefix.upper()}_2004',
                                                      url=f'(local file)',
                                                      path=f'{self.file_prefix}_2004.csv',
                                                      long_name=f"{self.provider_code} "
                                                                f"Japan Oil Stats Weekly History 2004-2008"))

        logger.info(f'{len(self.sources)} source files to load.')
        # add the list of sources as values to dimension 'source'
        self.dynamic_dim['source'] += [vars(copy(source)) for source in self.sources + self.source_complements]
        # remove existing
        self.remove_existing_dynamic_dim('source')

    def download_and_get_checksum(self, download=True, parallel_download=True) -> NoReturn:
        """
        We were kind of forced to override this function here because it was calling download_source()
        from parent, not the one we overrode.
        :param download: Flag determining whether the file should be downloaded or not. Default is True.
        :param parallel_download: Flag determining whether download should occur in parallel. Default is True.
        :return NoReturn:
        """
        if download:
            logger.debug(f"download: {download}, parallel download: {parallel_download}")
            # in this scraper, self.source_complements has hard-coded history, not downloadable
            file_for_download = self.sources  # + self.source_complements
            if parallel_download:
                parallelize(self.download_source, file_for_download, self.max_worker)
            else:
                for f in file_for_download:
                    self.download_source(f)
        parallelize(calc_checksum_download, self.sources, self.max_worker)

    def download_source(self, source, http_headers=None):
        """
        Download one given file. Overriding parent method to use session with authentication.
        :param source: BaseSource object describing the file to download.
        :param http_headers: http headers to pass to request get
        Defined as a static method to allow overloading
        """
        logger.info('Downloading files through authenticated session.')
        try:
            try:
                path, url = source.path, source.url
            except AttributeError as e:
                raise AttributeError(f"Missing an essential source attribute: {e}")

            r = self._http_session_get(url, http_headers)

            file = FILE_STORE_PATH / path
            logger.debug(f'Writing content to {file}...')
            with open(file, "wb") as fp:
                fp.write(r.content)
            setattr(source, 'last_download', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
        except Exception as e:
            logger.exception(f"Error while downloading source with code {source.code} and content {r.content}.")
            raise e

    @classmethod
    def _get_data_from_source(cls, source: BaseSource):
        """
        Loads one source file into a data frame.
        :param source: a BaseSource object describing the source file.
        :return: a Pandas DataFrame with the file content.
        """
        file = FILE_STORE_PATH / source.path
        logger.debug(f'Reading {file}')
        df = pd.read_csv(file,
                         skip_blank_lines=True,
                         names=cls.csv_columns.keys(),
                         dtype=cls.csv_columns,
                         na_values=cls.csv_null_values)
        logger.debug(f'{file}: {len(df)} rows loaded.')
        return df

    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data
        :return: NoReturn
        """
        # First we load the files from website and the history files (if needed)
        df = self._load_files(self.sources)
        df_hist = self._load_files(self.source_complements)

        # Then we filter out overlapping rows from history and concatenate with rows from files
        dfs = []
        if df_hist is not None and len(df_hist) > 0:
            if df is not None and len(df) > 0:
                logger.debug(f"Number of rows in history before filtering overlapping rows: {len(df_hist)}")
                df_hist = df_hist[df_hist[self.key_column_name] < df[self.key_column_name].min()]
                logger.debug(f"Number of rows in history after filtering overlapping rows: {len(df_hist)}")
            dfs.append(df_hist)

        if df is not None and len(df) > 0:
            dfs.append(df)

        # Finally we load the results in self.data
        if len(dfs) > 0:
            logger.debug(f"Loading {len(dfs)} rows to self.data")
            self.data = pd.concat(dfs)
        else:
            logger.debug(f"No data to process")

        return None

    @classmethod
    def _load_files(cls, file_list: List[BaseSource]) -> pd.DataFrame:
        """
        Generic method for reading a list of files.
        It reads the files in parallel, concatenates the results and add a key.
        :param file_list: list of BaseSource objects describing the files to read.
        :return: a data frame
        """
        logger.debug(f"Reading {len(file_list)} files in parallel...")
        dfs = parallelize(cls._get_data_from_source, file_list, cls.max_worker)

        df = None
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                logger.debug(f"Number of rows in data frame: {len(df)}")
                # parse current week column, keep only the first date and convert to datetime
                # can be a primary key
                df[cls.key_column_name] = pd.to_datetime(df['Current Week'].str[:11], dayfirst=True)
            except ValueError as e:
                logger.exception(f"Error while concatenating data frames, not transforming data")
        else:
            logger.warning(f'No data read from file list: {file_list}.')
        return df

    @classmethod
    def _add_key(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the first day of the week and add to data frame as index.
        :param df: the data frame to process
        :return: data frame with new column
        """
        df[cls.key_column_name] = pd.to_datetime(df['Current Week'].str[:11], dayfirst=True)
        return df

    def upsert(self) -> NoReturn:
        """
        Overrides parent method to bypass API and write results directly to specific table.
        if self.full_load = True, it truncates final table and loads the content of self.data into it,
        It loads self.data into a temporary table and runs a merge with final table otherwise.
        :return: NoReturn
        """
        logger.info("Writing to database.")
        if self.data is None or len(self.data) == 0:
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
                rs = con.execute(f"IF OBJECT_ID('{self.db_schema}.{self.db_final_table}','U') is not null "
                                 f"TRUNCATE TABLE {self.db_schema}.{self.db_final_table}")

                logger.info(f'Loading {len(self.data)} rows to database.')
                self.data['date_created'] = datetime.datetime.now()
                self.data.to_sql(self.db_final_table, con=con, schema=self.db_schema, index=False, if_exists='append')
            else:
                # loads data into temporary table and then merge with final table.
                logger.debug(f'Loading {len(self.data)} rows to temporary table {self.db_temp_table}')
                self.data.to_sql(self.db_temp_table, con=con, schema=self.db_schema, index=False, if_exists='append')

                logger.debug(f'Merging data from {self.db_temp_table} into {self.db_final_table}')
                merge_query = self.build_merge_query()
                rs = con.execute(merge_query)

    @retry(requests.HTTPError, tries=max_retry, delay=retry_delay)
    def _http_session_get(self, url: str, http_headers=None) -> requests.Response:
        """
        This method authenticates to the website and returns the front page.
        It uses the Session() object defined in self.session.

        It raises HTTPError if response is invalid.

        Notice this method name starts with '_' which means it is 'protected'.
        Protected methods are visible to child classes but not from outside.

        :return requests.Response: HTTP response object
        """
        logger.debug(f'Getting {url} through authenticated session...')
        response = self.session.get(url, proxies=PROXY_DICT, headers=http_headers)
        if not response.ok:
            logger.exception(f'Error loading URL through authenticated session: {response.status_code}')
            response.raise_for_status()

        return response

    def _parse_file_list(self, html: str) -> List[str]:
        """
        This method parses the file list for All Japan section from the front page.
        We expect one file for each year for the last 3 years.
        :return List[str]: the list of files found for All Japan in the website.
        """
        return [a['href'] for a in BeautifulSoup(html, 'html.parser').find_all('a') if a.text == self.files_scope]

    def build_merge_query(self):
        """
        Build the merge query.
        :return: str with the merge query.
        """
        # columns to compare (ignore 'Current Week')
        cols_to_compare = [*self.csv_columns][1:]
        cols_to_insert = [*self.csv_columns] + [self.key_column_name]

        merge_difference = [f'target.[{col}] <> source.[{col}]'
                            for col in cols_to_compare if col != self.key_column_name]
        merge_update = [f'target.[{col}] = source.[{col}]'
                        for col in cols_to_compare if col != self.key_column_name]
        merge_insert_cols = [f'[{col}]' for col in cols_to_insert]
        merge_insert_values = [f'source.[{col}]' for col in cols_to_insert]

        query = (f'MERGE {self.db_schema}.{self.db_final_table} target \n'
                 f'USING {self.db_schema}.{self.db_temp_table} as source \n'
                 f'ON (target.[{self.key_column_name}] = source.[{self.key_column_name}]) \n'
                 f"WHEN MATCHED AND ({' OR '.join(merge_difference)})\n"
                 f"THEN UPDATE SET {', '.join(merge_update)}, target.[date_modified] = GETDATE()\n"
                 f'WHEN NOT MATCHED \n'
                 f"THEN INSERT ({', '.join(merge_insert_cols)}, [date_created]) \n"
                 f"VALUES ({', '.join(merge_insert_values)}, GETDATE());")

        logger.debug(f'Merge query: {query}')
        return query
