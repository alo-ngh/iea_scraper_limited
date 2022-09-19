import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from iea_scraper.jobs.utils import  get_driver

from iea_scraper.core.job import ExtDbApiJobV2
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import convert_bbl_to_kbd
from iea_scraper.settings import PROXY_DICT, SSL_CERTIFICATE_PATH, FILE_STORE_PATH

logger = logging.getLogger(__name__)


class NigerianOilSupplyJob(ExtDbApiJobV2):
    """
    Monthly Nigerian Oil Supply data scraper.
    Source: https://www.dpr.gov.ng/oil-production-status-report/
    """
    title: str = 'Monthly Nigerian Oil Supply'

    page_url: str = 'https://www.nuprc.gov.ng/oil-production-status-report/'

    source_prefix: str = 'ng_oil_supply'
    source_long_name_prefix: str = 'Monthly Nigerian Oil Supply'
    FREQUENCY = 'Monthly'
    UNIT: str = 'KBD'
    AREA = 'NIGERIA'
    FLOW = 'SUPPLY'
    ORIGINAL = True
    JOB_CODE = Path(__file__).parent.parts[-1]
    # the information of the provider
    provider_code = JOB_CODE.upper()
    provider_url = 'https://www.nuprc.gov.ng/index.php'
    provider_long_name = 'Department of Petroleum Resources'
    product_mapping = {'Crude Oil': 'CRUDEOIL',
                       'Condensate': 'COND'}

    column_mapping = {'TERMINAL/STREAM': 'entity',
                      'Liquid Type': 'product'}
    # history: 1997-2019
    history_start = 1997
    history_end = 2019

    pandas_engine = 'openpyxl'

    def __init__(self,
                 year: int = None,
                 **kwargs):
        """
        In addition to existing parent's parameters, this defines a year to allow loading a specific year file.
        :param year: int: a year within historical year range to load.
        :param kwargs: parent's parameters
        """
        super().__init__(**kwargs)
        self.year = year
        self.static_file_years = [year for year in reversed(range(self.history_start, self.history_end + 1))]
        self.driver = get_driver()

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def __get_source(self, year: int, link: str = "None") -> BaseSource:
        """
        Helper function to return a BaseSource object for a given year and link.
        :param year: int: year to load
        :param link: the URL to the file for the given year. It can be null.
        :return: BaseSource: object with details about a source
        """
        logger.debug(f'Creating source for {year} and {link}')
        file_extension = 'xlsx' if year > self.history_end else 'csv'

        _code = f'{self.source_prefix}_{year}'
        _long_name = f'{self.source_long_name_prefix} - {year}'
        _url = link
        _path = f'{_code}.{file_extension}'

        return BaseSource(code=_code, long_name=_long_name, url=_url, path=_path)

    @staticmethod
    def __extract_year_from_link(link: str) -> int:
        """
        Helper function to extract the year of a file from a given link.
        :param link: an URL for a given file.
        :return: int: year extracted from the file name from the URL.
        """
        logger.debug(f'Extracting year from URL: {link}')
        filename: str = link.split('/')[-1]
        logger.debug(f'Filename in the link: {filename}')
        year: str = re.findall('[0-9]+', filename)[0]
        logger.debug(f'Year extracted from {filename}: {year}')
        return int(year)

    def get_file_links(self):
        """
        This methods gets the online content,create the soup object and parsed it to extract the data
        @return: a list of file links
        """
        logger.debug(f'Retrieving file links from {self.page_url}')
        # create the driver object
        self.driver.get(self.page_url)
        logger.debug(f'successfully parsed pages from {self.page_url}')
        # locate the web elements to the links
        file_links = self.driver.find_elements_by_link_text('Excel format')
        # iterate to get the links to the excel files
        links = list(reversed([(link.get_attribute('href')) for link in file_links]))

        logger.debug(f'{len(links)} file links retrieved from {self.page_url}')

        # return a list of links
        return links

    def get_sources(self):
        """
        We use BeautifulSoup to parse the HTML page and get the link to each file to download.
        """
        logger.info('Loading sources')
        sources = []
        if not self.full_load and self.year:
            # load only the year passed in the constructor
            # notice that if full_load and year are defined, it will ignore year and do a full load
            logger.info(f"Loading only one year: {self.year}, assuming it will not download it.")
            sources.append(self.__get_source(self.year))
        else:
            # create a BaseSource for each link found in the webpage
            sources.extend([self.__get_source(self.__extract_year_from_link(link), link)
                            for link in self.get_file_links()])

        logger.debug('Sorting sources by date descending...')
        sources = sorted(sources, key=lambda x: x.path, reverse=True)

        if self.full_load:
            # if full load: load files available in webpage and load all static files
            logger.debug('Loading the list of history file sources.')
            sources.extend([self.__get_source(year) for year in self.static_file_years])

            self.sources = sources
        else:
            # if not full load, load only current year's file (or the requested year)
            if len(sources) == 0:
                logger.info("No source files to process.")
            else:
    
                logger.debug(f'Adding the following file to sources to process: {sources[0].path}')
                self.sources.append(sources[0])

    def transform(self):
        """
        This will transform the data each downloaded source.
        :return: NoReturn
        """

        dfs = []
        for source in self.sources:
            file_path = FILE_STORE_PATH / source.path
            # read the file into the pandas dataframe
            logger.debug(f"Processing file: {file_path.name} extension {file_path.suffix}")
            final_df = self.read_csv(file_path) if file_path.suffix == '.csv' else self.read_excel(file_path)
            # assign the constant values to the dataframe
            final_df = final_df.assign(provider=self.provider_code) \
                .assign(frequency=self.FREQUENCY) \
                .assign(unit=self.UNIT) \
                .assign(area=self.AREA) \
                .assign(flow=self.FLOW) \
                .assign(original=self.ORIGINAL) \
                .assign(source=source.code)

            # change the column to lower case
            final_df.columns = [x.lower() for x in final_df.columns]
            # create a temporary colum and use to convert value to thousand barrels
            final_df['temporary_date'] = pd.to_datetime(final_df['period'])
            # calculate the value now in KBD
            final_df['value'] = final_df.apply(
                lambda x: convert_bbl_to_kbd(x['value'], x["temporary_date"].year, x['temporary_date'].month),
                axis=1)
            # drop the column
            final_df.drop(columns='temporary_date', inplace=True)
            logger.debug('the temporary column has been dropped')
            # append the final dataframe to the list
            logger.debug(f'Dataframe with {len(final_df)} rows processed.')
            dfs.append(final_df)
        # report the number of dataframes appended
        logger.debug(f'{len(dfs)} dataframes successfully appended')

        # check whether data was read into the dataframe list
        if len(dfs) == 0:
            logger.debug('No data to transform.')
            return

        # if full load,we are loading the data
        df = pd.concat(dfs)
        logger.debug(f'{len(df)} rows added to self.data')
        # add a new column called entity to the final dataframe
        df['entity'] = self.transform_entity(df['entity'])
        df = df.drop_duplicates()
        self.data = df.to_dict('records')

    def transform_entity(self, entity: pd.Series) -> pd.Series:
        """
        Updates the entity dimension with Nigerian field data.
        @param entity: pd.Series: series containing all the Negerian fields.
        @return: pd.Series: a panda series containing the entity codes.
        """
        # do not process 'None'...
        mask = entity.values != 'None'
        non_none_entity = pd.Series(entity.values[mask], entity.index[mask])
        df = pd.DataFrame()
        df['code'] = non_none_entity.str.split('(').map(lambda x: x[0].strip().replace(' - ', '_').replace(' ', '_'))
        df['code'] = (self.provider_code + '_' + df['code']).str.upper()
        df['long_name'] = non_none_entity.str.split('(')\
            .map(lambda x: x[0].strip().replace(' - ', '_').replace(' ', '_').upper())
        df['category'] = 'field'

        # ... but make sure the 'None' values are well returned
        mask = entity.values == 'None'
        none_entity = pd.Series(entity.values[mask], entity.index[mask])

        entity_code = none_entity.append(df['code']).sort_index()

        # while we need to calculate all entity codes, we don't send duplicates to the dimension
        df.drop_duplicates(subset=['code'], inplace=True)
        self.dynamic_dim['entity'] = df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')

        return entity_code

    def read_excel(self, path):
        """
        This methods transforms the Nigeria oil supply for 2021 and 2020 data
        @return: a dataframe object
        """
        # read in the excel file into dataframe
        df = pd.read_excel(path, engine=self.pandas_engine)
        # extract the current year from the dataframe
        production_year = [int(i) for i in df.iloc[0, 0].split() if i.isdigit()][0]

        # The row with all empty rows represent the end of the table we are interested in
        clean_table_index = df[df.isnull().all(axis=1)]

        # Get the index of the table we are interested in
        clean_index = clean_table_index.index[0]

        # finally let us get the clean table
        final_df = df.iloc[1:clean_index, :]

        # Convert the second row to be the first row
        final_df.columns = final_df.iloc[0, :].values

        # slice the first row to correctly ascertain the dataframe
        final_df = final_df.iloc[1:, :]

        # remove Blend Total rows
        final_df = final_df[final_df['Liquid Type'] != 'Blend Total']

        # fill the missing values for the Terminals
        final_df['TERMINAL/STREAM'] = final_df['TERMINAL/STREAM'].fillna(method='ffill')

        # Since one of the tables contains the 'TOTAL' column,we have to drop it
        if 'TOTAL' in final_df.columns:
            final_df.drop(labels='TOTAL', axis=1, inplace=True)

        logger.debug(f"Number of rows before before melting: {len(final_df)}")

        # Melt table to take it to the desired format
        final_df = pd.melt(final_df, id_vars=['TERMINAL/STREAM', 'Liquid Type'], value_name='value',
                           var_name='period')

        logger.debug(f"Number of rows after melting: {len(final_df)}")

        # isolate the columns with zero values
        final_df = final_df[final_df['value'] > 0]

        logger.debug(f"Number of rows after removing zero values: {len(final_df)}")

        # Convert the Period to the desired format
        # Pandas reads the date column differently,so this code account for that
        if final_df['period'].dtype == np.dtype('datetime64[ns]'):
            final_df['period'] = final_df['period'].dt.strftime('%b%Y').str.upper()
        else:
            final_df['period'] = final_df.period.str[:3] + str(production_year)

        # rename the column name to the column mapping
        final_df.rename(columns=self.column_mapping, inplace=True)
        # remove trailing spaces from the code
        final_df['product'] = final_df['product'].map(lambda x: x.strip())

        # map the product to their appropriate short forms
        final_df['product'] = final_df['product'].map(self.product_mapping)
        # change the comumn title to entity
        final_df = final_df.rename({'TERMINAL/STREAM': 'entity'})

        return final_df

    def read_csv(self, path):
        """
        This method read in the csvs in the historical datasets
        @return: a Datafram object of already melted data
        """
        df = pd.read_csv(path, engine='c', dtype={'value': float}, na_values={'value': '-'}, sep=';')
        logger.debug(f'{len(df)} rows read from {path.name}')
        df.dropna(axis='index', how='any', inplace=True)
        logger.debug(f'{len(df)} rows left from {path.name} after removing null values')

        return df

    @classmethod
    def download_source(cls, source):
        """
        Over rides the download method.It downloads files for the recent ones but avoid downloading the historical data
        from the website

        :param source: the source object describing the object
        :return:
        """
        file_path = FILE_STORE_PATH / source.path
        if file_path.suffix == ' .csv':
            logger.info(f'{file_path.name} is a historical file. Not downloading it.')
        else:
            super(cls, cls).download_source(source)
