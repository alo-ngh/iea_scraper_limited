import datetime
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from iea_scraper.jobs.gov_bsee.base_gov_bsee import BaseGovBseeJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import BROWSERDRIVER_PATH, FILE_STORE_PATH

import logging

logger = logging.getLogger(__name__)


class DeepQualFieldsJob(BaseGovBseeJob):
    """
    This class implement the job for loading GOM fields into entity dimension.
    """
    title: str = "USA Gulf of Mexico - Deep Qualified Fields (entity)"

    source_code = f'{BaseGovBseeJob.provider_code}_DeepQualFields'
    area = 'USA'
    environment = "offshore"

    base_url = "https://www.data.bsee.gov/Other/DataTables/DeepQualFields.aspx"

    download_timeout = 5
    original_filename = "DeepQualFields.csv"

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)

        chrome_options = Options()
        # uncomment line below to hide browser window
        chrome_options.add_argument("--headless")
        # low memory option
        chrome_options.add_argument("--disable-dev-shm-usage");
        # change default download directory
        prefs = {"download.default_directory": str(FILE_STORE_PATH)}
        chrome_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(executable_path=str(BROWSERDRIVER_PATH),
                                       options=chrome_options)
        self.__enable_download_in_headless_chrome(str(FILE_STORE_PATH))

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def get_sources(self):
        logger.debug("Entering get_sources()")
        source = BaseSource(url=f"{self.base_url}",
                            code=f"{self.source_code.lower()}",
                            path=f"{self.source_code.lower()}.csv",
                            long_name=f"{self.area} {self.provider_code} Deepwater Natural Gas and Oil Qualified Fields")
        logger.debug(f"Appending one source to sources: {vars(source)}")
        # append to self.sources
        self.sources.append(source)

    def download_source(self, source):
        """
        Uses selenium to click on the CSV button to trigger file download.
        :param source: the source object defining this source.
        """
        logger.debug(f"Going to URL: {self.base_url}")
        self.driver.get(self.base_url)

        # this will start file download
        logger.debug(f"Clicking CSV button and waiting {self.download_timeout}s")
        self.driver.find_element_by_id("ASPxFormLayout2_btnCsvExport").click()

        # wait a moment
        time.sleep(self.download_timeout)

        # if file DeepQualFields.csv exists, rename it replacing existing
        config = Path(FILE_STORE_PATH).joinpath(self.original_filename)
        logger.debug(f"renaming file {str(config)} into {source.path}")
        try:
            path = config.resolve(strict=True)
            path.replace(path.parent / source.path)
            setattr(source, 'last_download', datetime.datetime.now().strftime("%x %X"))
            logger.debug(f"Added 'last_download' attribute to source: {source.last_download}")
        except FileNotFoundError:
            raise FileNotFoundError("Error when downloading DeepQualFields.csv: timeout exceeded and no file found.")

    def transform(self):
        logger.debug(f"Transforming data...")
        for source in self.sources:
            logger.debug(f"Reading file {source.path}")

            abs_path = FILE_STORE_PATH.joinpath(source.path)
            df = pd.read_csv(abs_path)

            logger.debug(f"Number of rows read fom file: {str(len(df.index))}")

            df = (df[["Field Nickname", "Field Name Code"]].
                  drop_duplicates().
                  fillna(value={"Field Nickname": "Other"}).
                  assign(category="field").
                  rename(columns={"Field Nickname": "long_name",
                                  "Field Name Code": "code"}))

            df["meta_data"] = df.apply(lambda x: {'environment': self.environment}, axis=1)
            df["long_name"] = self.provider_code + " - " + df["long_name"] + " - " + df["code"]

            logger.info(f"Number of entities detected: {str(len(df.index))}")

            # add dictionary to dynamic dims
            self.dynamic_dim['entity'] = df.to_dict('records')
            self.remove_existing_dynamic_dim('entity')

    def __enable_download_in_headless_chrome(self, download_dir):
        """
        Enables download in headless mode in chrome.
        :param download_dir: the directory where downloaded files will be saved.
        """
        # add missing support for chrome "send_command"  to selenium webdriver
        self.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        command_result = self.driver.execute("send_command", params)
