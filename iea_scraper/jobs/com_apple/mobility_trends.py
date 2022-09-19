import pandas as pd
from sqlalchemy import create_engine

from iea_scraper.settings import FILE_STORE_PATH, EXT_DB_STR

import logging

import datetime
from datetime import datetime, timedelta
from iea_scraper.core.job import BaseJob
from iea_scraper.jobs.utils import get_driver, wait_file
from time import sleep

DOWNLOAD_TIMEOUT = 120
WAIT = 10
PAGE_LOAD_WAIT = 3

PUBLICATION_DELAY = 2

COLUMNS = ['geo_type', 'region', 'transportation_type', 'alternative_name', 'sub-region', 'country']

CHUNK_SIZE = 100000

# Form URL
APPLE_URL = "https://www.apple.com/covid19/mobility"

logger = logging.getLogger(__name__)


class MobilityTrendsJob(BaseJob):
    """
    Scraper for Apple Mobility Trends data.
    """
    title: str = "Apple - Mobility Trends"

    def download_file(self):
        driver = get_driver()
        driver.get(APPLE_URL)
        filename_pattern = "applemobilitytrends*"

        # remove existing file
        for f in FILE_STORE_PATH.glob(filename_pattern):
            try:
                logger.info(f'Removing file {f.name}')
                f.unlink()
            except FileNotFoundError:
                pass

        # wait the page to load
        sleep(PAGE_LOAD_WAIT)
        # download file
        download_button = driver.find_element_by_class_name('download-button-container')
        download_button.click()

        # name of the file
        elems = driver.find_elements_by_css_selector(".download-button-container [href]")
        for elem in elems:
            link = elem.get_attribute('href')
            filename = link.split('/')[-1]

        new_path = FILE_STORE_PATH / filename

        # wait for the file
        wait_file(new_path, WAIT, DOWNLOAD_TIMEOUT)

        logger.info('File download successfully finished.')
        driver.close()

        return new_path

    def load_data(self, file_path):
        # Reading the CSV file

        df = pd.read_csv(file_path)
        df = pd.melt(df,
                     id_vars=COLUMNS,
                     var_name='date',
                     value_name='value')
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['source'] = file_path.name

        logger.debug(f'File content: {df.head()}')
        logger.debug(f'Datebase: {EXT_DB_STR}')
        engine = create_engine(
            EXT_DB_STR,
            fast_executemany=True)

        with engine.connect().execution_options(autocommit=True) as con:
            logger.debug('Sending truncate table statement')
            rs = con.execute("IF OBJECT_ID('main.apple_mobility_data','U') is not null "
                             "TRUNCATE TABLE main.apple_mobility_data")
        logger.info(f'Loading {len(df)} rows to database.')
        df.to_sql('apple_mobility_data',
                  con=engine, schema='main', if_exists='append', index=False, chunksize=CHUNK_SIZE)

    def run(self, download=True):
        """
        This method runs the scraper.
        :return: NoReturn
        """
        self.load_data(self.download_file())
