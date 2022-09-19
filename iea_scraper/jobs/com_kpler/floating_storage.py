import base64
import logging

from pathlib import Path
from typing import Dict

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, KPLER_USERNAME, KPLER_PASSWORD

import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


class FloatingStorageJob(ExtDbApiDedicatedTableJob):
    """
    Floating Storage Data - KPLER API
    Provider: KPLER (https://api.kpler.com/)
    """

    title: str = "Kpler - Floating Storage - Daily Data"

    job_code = Path(__file__).parent.parts[-1]

    provider_code = job_code.upper()
    provider_long_name = "KPLER"
    provider_url = 'https://www.kpler.com/'

    base_url = "https://api.kpler.com"
    vessel_data_url = "/v1/fleet-metrics/vessels"

    source_prefix_data = f'{provider_code.lower()}_fs_data'

    key_columns = ['provider', 'source', 'Date', 'IMO', 'Product', 'Grade']
    db_schema = 'kpler'
    db_table_prefix = 'floating_storage'

    date_columns = ['Date', 'Floating Since']

    # First period with data in history
    start_history = date(2016, 1, 1)

    today = date.today()
    starting_date = today - relativedelta(months=5)
    # ending_date: the last day of the current month
    ending_date = date(today.year, today.month, 1) + relativedelta(months=1, days=-1)

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

    def get_vessel_data_url(self, date_to_process: date) -> str:
        """
        Returns the URL for getting vessel data for a given date.
        :param date_to_process: datetime.date: the date to process
        :return: a string with the URL for querying vessels data.
        """
        endpoint = f"{self.base_url}{self.vessel_data_url}"
        params_vessel_data = {"metric": "floating_storage",
                              "zones": "world",
                              "floatingStorageDurationMin": "12",
                              "floatingStorageDurationMax": "Inf",
                              "period": "daily",
                              "products": "crude/co",
                              "unit": "kb",
                              "endDate": date_to_process.strftime('%Y-%m-%d')}
        return f"{endpoint}?{self.get_params(params_vessel_data)}"

    def get_sources(self):
        """
        Defines the data sources to be downloaded.
        This scraper don't relay on full_load as it always download the full history.
        :return: NoReturn
        """
        logger.info('Getting sources...')

        starting_date = self.start_history if self.full_load else self.starting_date

        for date_to_process in pd.date_range(start=starting_date, end=self.ending_date, freq='1M'):
            str_date = date_to_process.strftime('%Y-%m-%d')
            source_code = f"{self.source_prefix_data}_{str_date}"
            source_data = BaseSource(code=source_code,
                                     long_name=f"KPLER - vessel data for {str_date}",
                                     url=self.get_vessel_data_url(date_to_process),
                                     path=f'{source_code}.csv')
            self.sources.append(source_data)

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

    def transform_source(self, source: BaseSource):
        """
        Transforms one data source.
        :param source: BaseSource: data source definition.
        :return: pd.DataFrame: transformed data.
        """
        file_path = FILE_STORE_PATH / source.path
        df = pd.read_csv(file_path, sep=self.file_delimiter, parse_dates=self.date_columns, infer_datetime_format=True)
        logger.info(f'{len(df)} rows read from {file_path}')
        df['provider'] = self.provider_code
        df['source'] = source.code
        for d in self.date_columns:
            df[d] = df[d].dt.date

        return df

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """
        logger.info('Transforming data')
        if len(self.sources) == 0:
            logger.info('No data to load.')
            return

        dfs = [self.transform_source(source) for source in self.sources]
        # in this scraper, we load directly a dataframe on data
        self.data = pd.concat(dfs)
