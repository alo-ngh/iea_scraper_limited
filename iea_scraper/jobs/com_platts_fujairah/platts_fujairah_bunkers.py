import logging
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class PlattsFujairahBunkersJob(ExtDbApiDedicatedTableJob):
    """
    Platts Fujairah Bunkers - Monthly Data

    Provider: Platts (https://fujairah.platts.com/fujairah)
    """

    title: str = " Platts Fujairah Bunkers - Monthly Data"

    job_code = Path(__file__).parent.parts[-1]

    provider_code = job_code.upper()
    provider_long_name = 'Platts'
    provider_url = 'https://fujairah.platts.com/fujairah'
    file_prefix = 'platts_fuj_bunkers'

    delay = 10

    key_columns = ['provider', 'fuel', 'date', 'unit']
    db_schema = 'main'
    db_table_prefix = 'com_platts_fujairah_bunkers'

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.driver = get_driver()

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def get_sources(self):
        """
               Creates a source file storing the raw HTML fragment from the page containing the monthly data.
               File information is stored in self.sources list as a BaseSource object.
               If full_load is True, search for all existing files in the file system respecting the file pattern to load.
               :return: NoReturn
               """
        logger.info('Defining sources to load.')
        today_calendar = date.today()
        file_code = f"{self.file_prefix}_{today_calendar.year}M{today_calendar.month:02}"
        file_name = f"{file_code}.csv"
        long_name = f"{self.title} - {today_calendar.year} month {today_calendar.month:02}"

        source = BaseSource(code=file_code,
                            long_name=long_name,
                            path=file_name,
                            url=self.provider_url)

        self.sources.append(source)

    def __download_data(self, url):
        """
        Use selenium to download the Fujairah bunkers monthly.
        :param url: the url to download
        :return: a tuple with bunkers, converted_date, unit
        """

        self.driver.get(url)
        time.sleep(self.delay)
        table = self.driver.find_element_by_id("panel-1051-table")
        table_rows = table.find_elements_by_tag_name('tr')
        bunker = []
        for tr in table_rows:
            td = tr.find_elements_by_tag_name('td')
            row = [tr.text for tr in td]
            bunker.append(row)
        panel = self.driver.find_element_by_id("component-1065")
        converted_date = panel.find_element_by_class_name('date-updated').text
        unit = self.driver.find_element_by_id("component-1047").text
        self.driver.close()
        return bunker, converted_date, unit

    @staticmethod
    def __transform_data(bunker, converted_date, unit):
        """
        Converts the content into a dataframe.
        :param bunker: list of bunkers data
        :param converted_date: the date of publication.
        :param unit: the unit of the values.
        :return: pd.DataFrame: a pandas dataframe with this data.
        """

        bunker = pd.DataFrame(bunker)
        bunker.columns = ['fuel', 'volume']
        bunker['volume'] = bunker['volume'].str.replace(',', '')
        bunker['volume'] = pd.to_numeric(bunker['volume'])
        bunker['date'] = pd.to_datetime(converted_date)
        bunker['unit'] = unit[unit.find('(') + 1:unit.find(')')]
        return bunker

    def download_source(self, source, http_headers=None):
        """
        :param source: BaseSource: the source to download.
        :param http_headers: HTTP headers if needed.
        :return:
        """
        logger.info(f'Downloading data from {source.url}')
        bunker, converted_date, unit = self.__download_data(source.url)
        bunker_df = self.__transform_data(bunker, converted_date, unit)
        bunker_df['source'] = source.code

        file_path = FILE_STORE_PATH / source.path
        logger.info(f'Writing {len(bunker_df)} rows to {file_path}')
        bunker_df.to_csv(file_path, index=False)
        # adding attribute 'last_download' to the file
        logger.info(f'File {source.path} downloaded successfully. Adding last_download attribute to source.')
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def transform(self):
        """
        This transforms each source file in self.sources and put resulting data into self.data.
        :return: NoReturn
        """

        logger.info('Transforming sources.')
        if len(self.sources) == 0:
            logger.info('No data to load')
            return

        df = pd.concat([pd.read_csv(FILE_STORE_PATH / source.path,
                                    parse_dates=['date'],
                                    infer_datetime_format=True)
                        for source in self.sources])
        logger.debug(f'{len(df)} rows read from {len(self.sources)} files.')
        df['provider'] = self.provider_code
        self.data = df
