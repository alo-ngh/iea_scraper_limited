import logging

import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import requests

from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT, SSL_CERTIFICATE_PATH, EXT_DB_STR
from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource

logger = logging.getLogger(__name__)


class UkRoadFuelSalesJob(ExtDbApiDedicatedTableJob):
    """
    Average road fuel sales at sampled filling stations, Great Britain: From January 2020.
    Daily data, published monthly.
    This scraper is based on ExtDbApiJob but writes to a separate table.

    Provider: UK Government
    """

    title: str = "UK Road Fuel Sales - Weekly Data"

    provider_code = Path(__file__).parent.parts[-1].upper()
    source_code = f'{provider_code}_road_fuel_sales'
    area_code = 'UK'

    provider_code = provider_code.upper()
    provider_long_name = 'UK Government',
    provider_url = r'https://www.gov.uk/government/statistics/average-road-fuel-sales-and-stock-levels'

    sheet_name = 'Data'
    rows_to_skip = 7
    pandas_engine = 'odf'

    key_columns = ['Date', 'Fuel Type', 'Region']
    db_schema = 'main'
    db_table_prefix = 'uk_road_fuel_sales'

    chunk_size = 10000

    def find_link_address(self):
        """
        This function scrapes the UK government energy statistics page
        to obtain the exact url of the data file.
        The website contains both an excel and an ods file, but we use the ods version.
        The excel file is not easily readable in Pandas as it contains a chart.
        """
        # page = urllib.request.urlopen(self.provider['url'])
        response = requests.get(url=self.provider_url,
                                proxies=PROXY_DICT,
                                verify=SSL_CERTIFICATE_PATH)
        # raise exception in case of HTTP error
        response.raise_for_status()
        # parse page
        soup = BeautifulSoup(response.content, 'html.parser')
        a = soup.find(lambda tag: tag.name == "a"
                      and "road fuel sales and stock levels (weekly data) – ODS" in tag.text)
        link = a['href']
        return link

    def get_sources(self):
        """
        List the sources to process (in this case, just one).
        :param self:
        :return: NoReturn
        """
        logger.info('Getting sources...')
        source = BaseSource(code=self.source_code,
                            long_name='UK Road Fuel Sales - Weekly Data',
                            url=self.find_link_address(),
                            path=f'{self.source_code}.ods')
        self.sources.append(source)

        logger.info(f'{len(self.sources)} sources to load.')

    def transform(self):
        """
        This function opens the Excel file into Pandas and reformats it.
        :param self:
        :return: NoReturn
        """
        data = []
        for source in self.sources:
            file_path = FILE_STORE_PATH / source.path
            logger.debug(f'Reading file {file_path}')
            df = pd.read_excel(file_path,
                               sheet_name=self.sheet_name,
                               skiprows=self.rows_to_skip,
                               engine=self.pandas_engine)
            logger.debug(f'{len(df)} rows read from file.')

            # non-date fields (like 'Return to top of the page') will impeach the automatic parse of 'Date'.
            # so we deal with them below
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.dropna(subset=['Date'], inplace=True)
            logger.debug(f'{len(df)} rows left after removing rows with invalid date.')

            data.append(df)

        final_df = pd.concat(data) if len(data) > 0 else pd.DataFrame()
        logger.debug(f'{len(final_df)} rows after transformations.')

        self.data = final_df
