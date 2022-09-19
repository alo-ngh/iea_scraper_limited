import json
import re
import logging
from pathlib import Path

import pandas as pd
import requests

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, SSL_CERTIFICATE_PATH

logger = logging.getLogger(__name__)


class OxfordEconomicsApiJob(ExtDbApiDedicatedTableJob):
    """
    This class primarily works with the Oxford Economics Api
    Data are retrieved from the API on the basis of selection passed to the API using the post method
    It outputs json and this will be transformed using Pandas before being loaded to the database
    """
    title: str = 'Oxford Economics API'
    JOB_CODE = Path(__file__).parent.parts[-1]
    # provider details
    provider_code: str = JOB_CODE.upper()
    provider_long_name: str = 'Oxford Economics API'
    provider_url: str = 'https://services.oxfordeconomics.com'
    # source details
    source_long_name: str = 'Oxford_Economics_Api_Data'
    source_code: str = 'com_oxfordeconomics'

    # the starting and end year to be retrieved by the api
    start_year = 2000
    end_year = start_year + 30

    # selection parameters

    # indicator definition
    # 'GDPPPP': 'GDP,PPP exchange rate'
    # 'RXD': 'Exchange rate,average period'
    # 'PGDP': 'GDP deflator'
    # RXDPPP: Exchangerate, period average, PPP
    # CSA: Consumption, private, real, LCU
    # GVAIND GVAI: Gross value added in industry, real, LCU
    # IP: Industrial production index
    # GVAAGR: Gross value added in agriculture and forestry, real, LCU

    code_names = ['GDPPPP', 'RXD', 'PGDP', 'RXDPPP', 'CSA', 'GVAIND', 'IP', 'GVAAGR']
    db_table_prefix = 'oxford_economics_api'
    db_schema = 'main'
    key_columns = ['databankcode',
                   'producttypecode',
                   'locationcode',
                   'variablecode',
                   'measurecode',
                   'frequency',
                   'period']

    regex_period_columns = '^(Annual|Quarterly|Monthly)Data'

    # api details
    api_key = 'cb-f337b53c-2279-47c5-928a-5a9b6c11a15d'
    base_url = 'https://services.oxfordeconomics.com'
    page_size = 5000

    headers = {'Accept': 'application/json',
               'Api-Key': api_key,
               'Content-Type': 'application/json; charset=utf8'}

    def get_sources(self):
        """
        This method creates the sources for each indicator
        :return:The BaseSource object for each source
        :return: NoReturn
        """
        logger.debug('adding sources to the basesource object')
        self.sources = [self.__get_sources(data_indicator) for data_indicator in self.code_names]
        logger.debug(f'{len(self.sources)} sources was successfully added to the list of sources')

    def __get_sources(self, indicator):
        """
        :param self: accepts each indicator code
        :return: base source object
        """
        code = f"{self.source_code}_{indicator}"
        long_name = f"{self.source_long_name}_for_{indicator}"
        url = f"{self.provider_url}"
        path = f"{code}.json"
        # select parameters
        data_selection = {
            'DatabankCode': 'WDMacro',
            'Frequency': 'Both',
            'GroupingMode': 'false',
            'IndicatorSortOrder': 'AlphabeticalOrder',
            'IsTemporarySelection': 'true',
            'ListingType': 'Private',
            'LocationSortOrder': 'AlphabeticalOrder',
            'Order': 'IndicatorLocation',
            'Sequence': 'EarliestToLatest',
            'StackedQuarters': 'true',
            'StartYear': self.start_year,
            'EndYear': self.end_year,
            # note: the fields below have been assigned empty lists
            'Regions': [],
            'Variables': [
                          {'Measurecodes': ['L'],
                           'ProductTypeCode': 'WMC',
                           'VariableCode': indicator}
                         ]
        }
        post_param = data_selection
        # return the base object
        return BaseSource(code=code, long_name=long_name, url=url, path=path, meta_data={"input_data": post_param})

    def transform(self):
        """
        This method transforms the json file from the source.path to the require shape and form
        :return: None
        """
        # create dataframe list
        self.data = []
        dfs = []
        for source in self.sources:
            logger.debug(f'adding data to the dataframe from {source.path}')
            file_path = FILE_STORE_PATH / source.path
            logger.debug(f'processing file with name {file_path.name}')
            with open(file_path) as file:
                data = json.load(file)
            # read into pandas
            df = pd.json_normalize(data)
            # expression to detect columns representing a data point for a given period
            prog = re.compile(self.regex_period_columns)
            id_ = [c for c in df.columns if not prog.match(c)]
            logger.debug(f"{source.path}: columns used as id to melt json: {', '.join(id_)}")

            # melting df to have periods as rows
            df = df.melt(id_vars=id_, value_name='value')
            # lowercase all column titles
            df.columns = df.columns.str.lower()
            # rename the columns
            df.rename(columns=lambda x: x.replace('metadata.', 'metadata_'), inplace=True)
            # split the column and drop previous value
            df[['frequency', 'period']] = df['variable'].str.split('.', expand=True)
            # drop variable column and rows with null values
            df = df.drop('variable', axis=1)\
                   .dropna(subset=['value'])
            # convert period to integer
            df['metadata_historicalendyear'] = pd.to_numeric(df['metadata_historicalendyear'], errors='coerce')
            df['metadata_historicalendquarter'] = pd.to_numeric(df['metadata_historicalendquarter'], errors='coerce')
            df['metadata_baseyearprice'] = pd.to_numeric(df['metadata_baseyearprice'], errors='coerce')
            df['period'] = pd.to_numeric(df['period'], errors='coerce')
            # append dataframe
            dfs.append(df)
        logger.debug(f'dataframe with {len(dfs)} dataframes  have been added')

        # check for when no new data have been added
        # check whether data was read into the dataframe list
        if len(dfs) == 0:
            logger.debug('No data to transform.')
            return
        # when data is loaded into the dataframe
        logger.debug('concatenating the dataframes into the final one')
        final_data = pd.concat(dfs)
        self.data = final_data
        logger.debug(f'a total of {len(final_data)} of rows were added')

    @classmethod
    def download_source(cls, source: BaseSource):
        """
        Download file from the API,returns json file format
        Over rides parent download method to perform the POST mehthod
        :param source: source data from the get_sources method
        :return: nothing
        """
        try:
            # collect source data from the source
            meta_data, path = source.meta_data, source.path
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        # input parameters
        inputs: dict = meta_data['input_data']

        page: int = 0
        data = None

        while True:
            logger.debug(f'{source.path}: reading page {page}...')
            response = requests.post(cls._download_url(page),
                                     headers=cls.headers,
                                     data=json.dumps(inputs), verify=SSL_CERTIFICATE_PATH)

            response.raise_for_status()
            logger.debug(f'{source.path}: successfully returned data from the API.')
            new_data = response.json()

            if data is None:
                data = new_data
            else:
                data.extend(new_data)

            logger.debug(f'{source.path}: page {page}: size {len(data)}')

            if len(new_data) < cls.page_size:
                break
            # if length is equal to page size,
            # then we probably have more pages to read
            page += 1

        logger.debug(f'{source.path}: download from the sources is completed')
        # write data to path
        file_path = FILE_STORE_PATH / path
        text = json.dumps(data)
        file_path.write_text(text)
        logger.debug(f'{source.path}: {len(data)} bytes written to disk.')

    @classmethod
    def _download_url(cls, page: int):
        """
        Returns a link of the form ../api/download?includemetadata=true&page={page}+&page_size={page_size}
        :param page: int: the page to download
        :return: str: the URL to download
        """
        return f"{cls.base_url}/api/download?includemetadata=true&page={page}+&pagesize={cls.page_size}"
