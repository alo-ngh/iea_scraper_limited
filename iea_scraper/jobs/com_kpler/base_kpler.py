import base64
import logging
from abc import ABCMeta
from iea_scraper.core.source import BaseSource
from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.settings import KPLER_USERNAME, KPLER_PASSWORD

logger = logging.getLogger(__name__)


class BaseKplerJob(ExtDbApiDedicatedTableJob, metaclass=ABCMeta):
    """
    Generic base class for extracting data from Kpler API.
    """

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.auth_header = self.__get_auth_header(KPLER_USERNAME, KPLER_PASSWORD)

    @staticmethod
    def __get_auth_header(login: str, pwd: str) -> dict[str, str]:
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

    def download_and_get_checksum(self, download=True, parallel_download=True):
        """
        Overrides super() to ensure that it runs sequentially.
        :param download: True for downloading the file.
        :param parallel_download: True for downloading in parallel.
        :return:
        """
        logger.debug('Kpler: always downloading sources sequentially')
        super().download_and_get_checksum(download, parallel_download=False)

    def download_source(self, source: BaseSource, http_headers=None):
        """
        Overrrides super method to be able to pass the http_header for authentication.

        :param http_headers: HTTP header
        :param source: the source object describing the object
        :return:
        """
        logger.debug(f"Downloading {source.code}")
        super().download_source(source, http_headers=self.auth_header)
