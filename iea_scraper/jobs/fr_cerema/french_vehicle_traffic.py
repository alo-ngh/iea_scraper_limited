import logging
import time
from copy import copy
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class FrenchVehicleTrafficJob(ExtDbApiDedicatedTableJob):
    """
    French Vehicle Traffic - Daily Data

    Provider: Cerema (https://dataviz.cerema.fr/trafic-routier/)
    """

    title: str = "French Vehicle Traffic - Daily Data"

    job_code = Path(__file__).parent.parts[-1]
    source_prefix = f'{job_code}_traffic'

    provider_code = job_code.upper()
    provider_long_name = 'Cerema'
    provider_url = 'https://www.cerema.fr'

    base_url = "https://dataviz.cerema.fr/trafic-routier/"
    max_nb_attempts = 3
    delay = 5

    key_columns = ['provider', 'source', 'Zone', 'date']
    db_schema = 'main'
    db_table_prefix = 'fr_cerema_traffic'

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

    def get_download_link(self):
        """
        This function connects to the website to obtain a temporary link.
        If it fails, it tries a predefined number of attempts.
        """
        link = None
        nb_attempts = 0
        successful = False

        while nb_attempts < self.max_nb_attempts:
            try:
                logger.info(f'Trying to read {self.base_url}...')
                self.driver.get(self.base_url)
                time.sleep(self.delay)
                iframes = self.driver.find_elements_by_tag_name('iframe')
                self.driver.switch_to.frame(iframes[0])
                nav = self.driver.find_elements_by_tag_name('li')
                nav[6].click()
                time.sleep(self.delay)
                link = self.driver.find_element_by_id("downloadData_fr").get_attribute('href')
                logger.info(f'{self.base_url} read with success.')
                successful = True
                break
            except Exception as e:
                nb_attempts += 1
                logger.exception(f"Attempt #{nb_attempts} failed: Error while reading data from {self.base_url}")
                time.sleep(self.delay)

        if not successful:
            raise ConnectionError('Unable to download the file.')

        return link

    def get_sources(self):
        """
        Defines the data sources to be downloaded.
        This scraper don't relay on full_load as it always download the full history.
        :return: NoReturn
        """
        logger.info('Getting sources...')
        self.sources.append(BaseSource(url=self.get_download_link(),
                                       path=f'{self.source_prefix}.csv',
                                       code=self.source_prefix,
                                       long_name=self.title))
        logger.info(f'{len(self.sources)} sources to load.')

    def transform_source(self, source: BaseSource):
        """
        Transforms one data source.
        :param source: BaseSource: data source definition.
        :return: pd.DataFrame: transformed data.
        """
        file_path = FILE_STORE_PATH / source.path
        df = pd.read_csv(file_path)
        logger.info(f'{len(df)} rows read from {file_path}')
        df['source'] = source.code
        df['provider'] = self.provider_code

        df['date'] = pd.to_datetime(df['date'])
        df['ITV'] = pd.to_numeric(df['ITV'], errors='coerce')
        df['IPL'] = pd.to_numeric(df['IPL'], errors='coerce')
        df['MGL_ITV'] = pd.to_numeric(df['MGL_ITV'], errors='coerce')
        df['MGL_IPL'] = pd.to_numeric(df['MGL_IPL'], errors='coerce')

        return df

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """
        logger.info('Transforming data')
        dfs = [self.transform_source(source) for source in self.sources]

        if len(dfs) == 0:
            logger.info('No data to load.')
            return

        # in this scraper, we load directly a dataframe on data
        self.data = pd.concat(dfs)
