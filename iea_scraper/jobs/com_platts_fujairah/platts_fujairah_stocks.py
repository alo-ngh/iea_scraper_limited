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


class PlattsFujairahStocksJob(ExtDbApiDedicatedTableJob):
    """
    Platts Fujairah Stocks - Weekly Data

    Provider: Platts (https://fujairah.platts.com/fujairah)
    """

    title: str = " Platts Fujairah Stocks - Weekly Data"

    job_code = Path(__file__).parent.parts[-1]

    provider_code = job_code.upper()
    provider_long_name = 'Platts'
    provider_url = 'https://fujairah.platts.com/fujairah'

    file_prefix = 'platts_fuj_stocks'

    delay = 10

    key_columns = ['provider', 'fuel', 'date', 'unit']
    db_schema = 'main'
    db_table_prefix = 'com_platts_fujairah_stocks'

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
        Creates a source file storing the raw HTML fragment from the page containing the weekly data.
        File information is stored in self.sources list as a BaseSource object.
        If full_load is True, search for all existing files in the file system respecting the file pattern to load.
        :return: NoReturn
        """
        logger.info('Defining sources to load.')
        today_calendar = date.today().isocalendar()
        file_code = f"{self.file_prefix}_{today_calendar[0]}W{today_calendar[1]:02}"
        file_name = f"{file_code}.csv"
        long_name = f"{self.title} - {today_calendar[0]} Week {today_calendar[1]:02}"

        source = BaseSource(code=file_code,
                            long_name=long_name,
                            path=file_name,
                            url=self.provider_url)

        self.sources.append(source)

    def __download_data(self, url):
        """
        Use selenium to download the Fujairah Stocks weekly.
        :param url: the url to download
        :return: a tuple with stocks, converted_date, unit
        """
        logger.debug(f"Reading {url} with selenium. Waiting page to load for {self.delay}s")
        self.driver.get(url)
        time.sleep(self.delay)
        table = self.driver.find_element_by_id("panel-1074-table")
        table_rows = table.find_elements_by_tag_name('tr')
        stocks = []
        for tr in table_rows:
            td = tr.find_elements_by_tag_name('td')
            row = [tr.text for tr in td]
            stocks.append(row)
        panel = self.driver.find_element_by_id("component-1082")
        converted_date = panel.find_element_by_class_name('date-updated').text
        unit = self.driver.find_element_by_id("component-1070").text
        self.driver.close()

        logger.debug(f"Data extracted - stocks: {len(stocks)} rows, date: {converted_date}, unit: {unit}")

        return stocks, converted_date, unit

    @staticmethod
    def __transform_data(stocks, converted_date, unit):
        """
        Converts the content into a dataframe.
        :param stocks: list of stock data
        :param converted_date: the date of publication.
        :param unit: the unit of the values.
        :return: pd.DataFrame: a pandas dataframe with this data.
        """
        stocks = pd.DataFrame(stocks)
        stocks.columns = ['fuel', 'volume']
        stocks['volume'] = stocks['volume'].str.replace(',', '')
        stocks['volume'] = pd.to_numeric(stocks['volume'])
        stocks['date'] = pd.to_datetime(converted_date)
        stocks['unit'] = unit[unit.find('(') + 1:unit.find(')')]
        return stocks

    def download_source(self, source, http_headers=None):
        """
        :param source: BaseSource: the source to download.
        :param http_headers: HTTP headers if needed.
        :return:
        """
        logger.info(f'Downloading data from {source.url}')
        stocks, converted_date, unit = self.__download_data(source.url)
        stocks_df = self.__transform_data(stocks, converted_date, unit)
        stocks_df['source'] = source.code

        file_path = FILE_STORE_PATH / source.path
        logger.info(f'Writing {len(stocks_df)} rows to {file_path}')
        stocks_df.to_csv(file_path, index=False)
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
