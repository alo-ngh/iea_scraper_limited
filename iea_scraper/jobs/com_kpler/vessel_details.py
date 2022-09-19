import base64
import logging

from pathlib import Path
from typing import Dict

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, KPLER_USERNAME, KPLER_PASSWORD

import requests
import pandas as pd
import io
from numpy import int64
from datetime import date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


class VesselDetailsJob(ExtDbApiDedicatedTableJob):
    """
    Floating Storage Data - KPLER API
    Provider: KPLER (https://api.kpler.com/)
    """

    title: str = "Kpler - Vessel Details"

    job_code = Path(__file__).parent.parts[-1]

    provider_code = job_code.upper()
    provider_long_name = "KPLER"
    provider_url = 'https://www.kpler.com/'

    base_url = "https://api.kpler.com"
    vessel_details_url = "/v1/vessels"

    source_prefix_details = f'{provider_code.lower()}_vessel'

    key_columns = ['provider', 'source', 'IMO']
    db_schema = 'kpler'
    db_table_prefix = 'vessel'

    file_delimiter = ';'

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.auth_header = self.__get_auth_header(KPLER_USERNAME, KPLER_PASSWORD)

    @staticmethod
    def __get_auth_header(login: str, pwd: str) -> Dict[str, str]:
        """
        Calculates the authentication header.
        It must be encoded in base64.

        :param login: the user login to KPLER API
        :param pwd: the user password to KPLER API
        :return: the calculated authentication header
        """
        auth_str = f"{login}:{pwd}"
        auth_b = bytes(auth_str, 'utf-8')
        b64encode_str = base64.b64encode(auth_b)
        http_header = {"Authorization": f"Basic {b64encode_str.decode('utf-8')}"}
        return http_header

    @staticmethod
    def get_params(param_list):
        """
        Transforms a dict into a list of HTTP GET parameters.
        :param param_list: a dictionary
        :return: a string with the parameters.
        """
        return '&'.join([f"{k}={v}" for k, v in param_list.items()])

    def get_vessel_details_url(self) -> str:
        """
        Returns the URL for getting vessel details.
        :return: a string with the URL for querying vessels data.
        """
        endpoint = f"{self.base_url}{self.vessel_details_url}"
        params_vessel_details = {"columns": "vessel_status,vessel_type,vessel_imo"}
        return f"{endpoint}?{self.get_params(params_vessel_details)}"

    def get_sources(self):
        """
        Defines the data sources to be downloaded.
        This scraper don't relay on full_load as it always download the full history.
        :return: NoReturn
        """
        logger.info('Getting sources...')
        source_details = BaseSource(code=self.source_prefix_details,
                                    long_name=f"KPLER - vessel details",
                                    url=self.get_vessel_details_url(),
                                    path=f'{self.source_prefix_details}.csv')
        self.sources.append(source_details)
        logger.info(f'{len(self.sources)} sources to load.')

    def download_and_get_checksum(self, download=True, parallel_download=False):
        """
        Overrides super() to ensure that it runs sequentially.
        :param download: True for downloading the file.
        :param parallel_download: True for downloading in parallel.
        :return:
        """
        super().download_and_get_checksum(download, parallel_download=parallel_download)

    def download_source(self, source: BaseSource, http_headers=None):
        """
        Overrrides super method to be able to pass the http_header for authentication.

        :param http_headers: HTTP header
        :param source: the source object describing the object
        :return:
        """
        logger.debug(f"Downloading {source.code}")
        super().download_source(source, http_headers=self.auth_header)

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """
        logger.info('Transforming data')
        if len(self.sources) == 0:
            logger.info('No data to load.')
            return

        source = self.sources[0]
        file_path = FILE_STORE_PATH / source.path
        df = pd.read_csv(file_path, sep=self.file_delimiter)
        df['provider'] = self.provider_code
        df['source'] = source.code

        logger.info(f'{len(df)} rows processed.')
        self.data = df
