import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiJobV2
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import convert_bbl_to_kbd
from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class GhanaOilSupplyJob(ExtDbApiJobV2):
    """
    This program automatically scraps the monthly oil production of Ghana
    source: 'https://www.petrocom.gov.gh/production-figures'
    @return: It loads the data including source,provider and values into the external database
    """
    # create the constants needed for the creation of the class
    FREQUENCY: str = 'Monthly'
    AREA: str = 'GHANA'
    UNIT: str = 'KBD'
    FLOW: str = 'SUPPLY'
    ORIGINAL = True
    PRODUCT = 'CRUDEOIL'
    page_url: str = 'https://www.petrocom.gov.gh/production-figures'
    JOB_CODE = Path(__file__).parent.parts[-1]
    # specify the historical production url
    historical_url: str = 'https://www.petrocom.gov.gh/archive/'
    # product mapping
    product_mapping = {'oil production (bbl)': 'CRUDEOIL', 'gas production (mmscf)': 'NATGAS'}

    # creating the class property
    provider_url = page_url
    provider_code = JOB_CODE.upper()
    provider_long_name = 'Petroleum Commission Ghana'

    title: str = 'Ghana Monthly Oil Supply'

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

    def get_sources(self):
        """
        This methods get the links to our source data.
        In this case,it get the links to the ids of the table on the website
        @return: a list containing a list containing a link to our tables
        """

        logger.info('getting the sources to the tables')
        logger.debug(f'parsing the main page to get table links at : {self.page_url}')

        # get the page content using the driver
        self.driver.get(self.page_url)
        logger.info(f'successfully retrieved data from : {self.page_url}')

        # list to hold links to the table
        file_links = []

        # get the table links from the page source
        table_links = self.driver.find_elements_by_tag_name('table')
        for table_link in table_links:
            table_id = table_link.get_property('id')
            file_links.append(table_id)

        # show the number of links successfully retrieved
        logger.info(f'{len(file_links)} links successfully retrieved from the website')

        # source list to hold the source information
        sources = []
        # read the source links into the basesource class

        nb_links = len(file_links)

        for link in file_links:
            # get the production year
            temp_info = pd.read_html(io=self.driver.page_source, attrs={'id': link})[0]
            production_year = temp_info.iloc[1][0].split()[1]
            field_name = temp_info.columns[0].split(" Production", 1)[0]
            field_name = field_name.replace(" ", "").replace('/', '_')

            # delete the temporary dataframe
            del temp_info

            # declare the argument for the Basesource
            code = f'{self.provider_code[0:2]}_{field_name.lower()}_{production_year}'
            source_long_name = f'Ghana Monthly Oil Production for {production_year}-{field_name}'
            source_path = f'{code}.html'
            # assign it to the base source
            source = BaseSource(url=self.page_url,
                                code=code,
                                path=source_path,
                                long_name=source_long_name,
                                meta_data={'table_id': link})

            # add the link full information
            logger.info(f'adding information for the data source with link :{link}')
            sources.append(source)
        # return how many table links were added
        logger.info(f'{len(sources)} links were added')
        # check if downloading the full link
        if self.full_load:
            # if full load, load all files
            logger.info(f'creating sources for the historical data from {self.historical_url} for year 2018 to 2020 ')
            # declare the argument for the basesource object
            code = f'{self.provider_code[0:2].lower()}_oil_supply_hist'
            source_long_name = f'Ghana Monthly Oil Production for 2018 to 2020'
            source_path = f'{code}.csv'
            # create the base source object
            source = BaseSource(url=self.historical_url,
                                code=code,
                                path=source_path,
                                long_name=source_long_name)

            # assign it to the sources list
            sources.append(source)
            # assign it to the list of sources
            self.sources = sources

            # In the future, add PDF files from archives
        else:
            self.sources.extend(sources[:nb_links])

    def download_source(self, source, http_headers=None):
        """
        This method download the data from the table into an html file whose path is defined in get_sources methods
        @param source: BaseSource: the file to download_source
        @param http_headers: HTTP headers to be included in the request, if needed.
        @return: None.
        """
        logger.info(f'Downloading source {source.url} to {source.path}')
        # get the filepath for the document we want to download
        file_path = FILE_STORE_PATH / source.path
        # it should not download if it is a csv
        if file_path.suffix == '.csv':
            pass
        else:
            # open the file and write the data
            data = self.driver.find_element_by_id(source.meta_data['table_id'])
            file_path.write_text(data=data.get_attribute('innerHTML'))
            # adding attribute 'last_download' to the file
            logger.info(f'File {source.path} downloaded successfully. Adding last_download attribute to source.')
            setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def transform(self):
        """
        This method transforms the the data into the final format that will be loaded into the external db
        @return:
        """
        # a list of the dataframe to be loaded
        dfs = []
        # load each table from the ids of the table from the sources
        for source in self.sources:
            # create the path of the file
            file_path = FILE_STORE_PATH / source.path
            # read in the files based on the the file format
            df = pd.read_csv(file_path, sep=';') if file_path.suffix == '.csv' else self.transform_html_tables(
                source.meta_data['table_id'])
            # add the necessary columns
            df = df.assign(provider=self.provider_code.upper()) \
                .assign(frequency=self.FREQUENCY) \
                .assign(area=self.AREA) \
                .assign(flow=self.FLOW) \
                .assign(original=self.ORIGINAL) \
                .assign(source=source.code)

            # add to the dfs list
            dfs.append(df)

        # show the number of dataframe appended
        logger.info(f'{len(dfs)} tables were successfully added')

        # check whether data was read into the dataframe list
        if len(dfs) == 0:
            logger.debug('No data to transform.')
            return
        # if full load,we are loading the data
        df = pd.concat(dfs)
        # add a new column called entity to the final dataframe
        df['entity'] = self.transform_entity(df['entity'])
        self.data = df.to_dict('records')

    def transform_entity(self, entity: pd.Series) -> pd.Series:
        """
        This method transforms the entity field to the external db format
        @param entity: recieves a pandas series
        @return: a pandas series
        """
        df = pd.DataFrame()
        # create the columns
        df['code'] = (self.provider_code + '_' + entity).str.upper()
        df['long_name'] = entity.str.upper()
        df['category'] = 'field'

        # the entity code
        entity_code = df['code']
        df.drop_duplicates(subset=['code'], inplace=True)
        self.dynamic_dim['entity'] = df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')

        # return the entity code
        return entity_code

    def transform_html_tables(self, table_link):
        """
        This methods transforms the tables in html format from the website
        @return: a dataframe
        """
        df = pd.read_html(io=self.driver.page_source, attrs={'id': table_link})[0]

        # get the field name
        field_name = df.columns[0].split(" Production", 1)[0]
        field_name = field_name.replace(" ", "").replace('/', '_').replace('-', '_')
        # drop unnecessary rows
        df = df[~df.iloc[:, 0].str.upper().isin(['TOTAL', 'AVERAGE'])]
        # drop na values
        df.dropna(how='any', inplace=True)
        # change the columns into the right one
        df.columns = df.iloc[0, :]
        # rename column
        col_name_gas = df.columns[2].strip()
        df.rename(columns={col_name_gas: 'gas production (mmscf)'}, inplace=True)
        # the production year
        production_year = df.iloc[1][0].split()[1]
        # remove unwanted data
        df = df.iloc[1:, :]
        # drop the irrelevant columns from the dataframe
        to_delete = df.columns[3:]
        df = df.drop(labels=to_delete, axis=1)
        # reset the index
        df.reset_index(drop=True, inplace=True)
        # convert the date to the right format
        df['Period'] = df.Period.str[:3] + str(production_year)
        df['Period'] = df.Period.str.upper()

        # all column names in lower caps
        df.columns = df.columns.str.lower()

        # convert the value column to float for calculation
        df['oil production (bbl)'] = df['oil production (bbl)'].astype(float)

        # convert the unit from BBL to KBD
        # create a temporary column and use to convert value to thousand barrels
        df['temporary_date'] = pd.to_datetime(df['period'])
        # calculate the value now in KBD
        df['oil production (bbl)'] = df.apply(
            lambda x: convert_bbl_to_kbd(x['oil production (bbl)'], x["temporary_date"].year,
                                         x['temporary_date'].month),
            axis=1)
        # drop the column
        df.drop(columns='temporary_date', inplace=True)
        logger.debug('the temporary column has been dropped')

        # add the field name to the dataframe
        df['entity'] = field_name

        # apply the melt method to the dataframe
        df = pd.melt(df, id_vars=['entity', 'period'],
                     value_vars=['oil production (bbl)', 'gas production (mmscf)'], value_name='value',
                     var_name='product')
        # map the product to the standard code in the external db

        df['product'] = df['product'].map(self.product_mapping)

        # assign the units based on the product type
        df['unit'] = df['product'].apply(lambda x: 'KBD' if (x == 'CRUDEOIL') else 'MMSCF')

        return df
