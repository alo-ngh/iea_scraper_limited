import logging
import platform
import subprocess
from pathlib import Path
from time import sleep

import pandas as pd
import requests
from datetime import datetime

from iea_scraper.core.source import BaseSource
from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT, SSL_CERTIFICATE_PATH

logger = logging.getLogger(__name__)


class GlobalMobilityJob(ExtDbApiDedicatedTableJob):
    """
    Scraper for Google's Global Mobility data.
    """

    title: str = "Google - Global Mobility data"

    page_load_wait: int = 3
    url = "https://www.google.com/covid19/mobility/"

    provider_code = 'com_google'
    provider_long_name = 'Goggle'
    provider_url = 'https://www.google.com/'

    key_columns = ['place_id', 'date']
    db_schema = 'main'
    db_table_prefix = 'google_mobility'

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
        logger.debug("Closing selenium driver.")
        self.driver.close()

    def get_sources(self):
        """
        Defines source meta data into self.sources.
        :return: NoReturn
        """
        source: BaseSource = BaseSource(code='com_google_mobility',
                                        url=self.url,
                                        path="Global_Mobility_Report.csv",
                                        long_name="Google Global Mobility Report")
        self.sources.append(source)

    def download_source(self, source, http_headers=None):
        try:
            path, url = source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        current_path = FILE_STORE_PATH / path
        previous_path = current_path.with_name(f"{current_path.stem}_previous{current_path.suffix}")

        logger.info(f'Opening {url}')
        self.driver.get(url)

        logger.debug(f'Moving {current_path.name} to {previous_path.name}')
        current_path.replace(previous_path)

        # wait the page to load
        logger.debug(f'Waiting {self.page_load_wait}s for the page to load.')
        sleep(self.page_load_wait)

        # download file
        download_button = self.driver.find_element_by_class_name('icon-link')
        href = download_button.get_attribute("href")

        logger.info(f"Downloading file {href}")
        response = requests.get(href,
                                proxies=PROXY_DICT,
                                headers=http_headers,
                                verify=SSL_CERTIFICATE_PATH)

        # raise exception if response not OK
        response.raise_for_status()

        logger.info("Download OK")
        logger.debug(f"CSV encoding: {response.encoding}")

        current_path.write_bytes(response.content)
        logger.info(f"File saved to {current_path}")

        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """
        logger.info('Transforming data')
        if len(self.sources) == 0:
            logger.info('No data to transform.')
            return

        source = self.sources[0]

        current_path = FILE_STORE_PATH / source.path

        if not self.full_load:
            current_path = self.calculate_differences_with_awk(current_path)

        # time to 'transform' data
        df = pd.read_csv(current_path, parse_dates=['date'], infer_datetime_format=True)

        if len(df) == 0:
            logger.info(f'File {current_path} empty: no data to load into the database.')
            return

        df['source'] = current_path.name

        self.data = df

    @staticmethod
    def calculate_differences_with_awk(current_path: Path) -> Path:
        """
        If platform is Linux, it will calculate a file with only the differences with awk.
        :param current_path: Path: points to current file.
        :return: Path: current_path if Windows, diff_path if Linux.
        """
        if platform.system() == 'Linux':
            logger.info('Calculating the differences with awk.')
            # Previous file
            previous_path = current_path.with_name(f"{current_path.stem}_previous{current_path.suffix}")
            # Output of awk
            diffdata_path = current_path.with_name(f"{current_path.stem}_diffdata{current_path.suffix}")
            # Final diff file
            diff_path = current_path.with_name(f"{current_path.stem}_diff{current_path.suffix}")

            command = "awk 'NR==FNR{a[$0];next} !($0 in a)' " \
                      f"{previous_path} {current_path} > {diffdata_path}"
            logger.debug(f'Running command on shell: {command}')
            completed_process = subprocess.run(command, shell=True)
            # this will raise an exception in case awk fails:
            completed_process.check_returncode()

            logger.debug("awk completed successfully. Preparing final file with header...")
            header = None
            logger.debug(f"Reading header from {current_path.name}")
            with current_path.open() as f:
                header = f.readline()
            logger.debug(f"Reading content from {diffdata_path.name}")
            content = diffdata_path.read_text() if diffdata_path.stat().st_size > 0 else ''
            logger.debug(f"Writing header and content to {diff_path.name}")
            diff_path.write_text(header + content)

            logger.debug(f"Removing {diffdata_path} and {previous_path}...")
            try:
                diffdata_path.unlink()
                previous_path.unlink()
            except FileNotFoundError:
                pass

            return diff_path
        else:
            logger.info('Platform is Windows, difference is not calculated.')
            return current_path
