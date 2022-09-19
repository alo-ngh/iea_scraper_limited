import logging
from copy import copy
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import re

from bs4 import BeautifulSoup

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class InCrudeoilProcJob(ExtDbApiJob):
    """
    Scraper for loading Crude Oil Processed at Refineries data from ppac.gov.in.
    """
    title: str = "India - Crude Oil Processed by Refineries"

    area = 'INDIA'
    flow = 'REFINOBS'
    frequency_monthly = 'Monthly'
    frequency_annual = 'Annual'
    product = 'CRUDEOIL'
    # thousands metric tonnes
    unit = 'KT'
    original = True

    main_page = 'https://www.ppac.gov.in/content/146_1_ProductionPetroleum.aspx'

    base_url = "https://www.ppac.gov.in/WriteReadData/userfiles/file/"

    provider_code = Path(__file__).parent.parts[-1].upper()
    provider_name = f'{area} - Petroleum Planning & Analysis Cell'

    filename_pattern = 'PT_crude[-,_](.*).xls'
    current_tabname = "PT_CRUDE_C"

    # Publication month where the fiscal year start
    month_for_year_shift = 5

    # one-site refineries (as they don't have a header row, we have to add them manually
    one_site_companies = pd.DataFrame([
        {'code': 'NEL', 'long_name': 'Nayara Energy Ltd.'},
        {'code': 'EOL', 'long_name': 'Essar Oil Ltd.'},
        {'code': 'BORL', 'long_name': 'Bharat Oman Refineries Ltd.'},
        {'code': 'NRL', 'long_name': 'Numaligarh Refinery Ltd.'},
        {'code': 'MRPL', 'long_name': 'Mangalore Refinery and Petrochemicals Ltd.'},
        {'code': 'HMEL', 'long_name': 'HPCL-Mittal Energy Ltd.'},
        {'code': 'KRL', 'long_name': 'Kochi Refinery Ltd.'},
        {'code': 'BRPL', 'long_name': 'Bongaigaon Refinery & Petrochemicals Ltd.'}
    ])

    # companies/location to be replaced
    companies_to_replace = {
        'NAYARA ENERGY LTD.\nVADINAR, GUJARAT, (Formerly ESSAR OIL LTD.)': 'NEL-VADINAR,GUJARAT',
        'NAYARA ENERGY LTD.\nVADINAR, GUJARAT,(Formerly ESSAR OIL LTD.)': 'NEL-VADINAR,GUJARAT',
        'ESSAR OIL LTD.,VADINAR,GUJARAT': 'EOL-VADINAR,GUJARAT',
        'Numaligarh Refinery Ltd.(NRL)\nNumaligarh, Assam': 'NRL-NUMALIGARH, ASSAM',
        'Kochi Refinery Ltd.-KOCHI, Kerala': 'KRL-KOCHI, KERALA',
        'Bongaigaon Refinery & Petrochemicals Ltd.\n(BRPL)-Bongaigaon, Assam': 'BRPL-BONGAIGAON, ASSAM'
    }

    null_values = [' -', '-']

    def get_sources(self):
        """
        Method returning the list of files to download and process.
        :return:
        """
        # TODO: check if url is updated in dimension.source
        logger.info('Getting sources...')

        # get url to files from website
        dict_files = self.parse_urls()

        # if full_load, process both files
        if self.full_load:
            self.sources.append(self.get_base_source('Historical', dict_files['Historical']))
        self.sources.append(self.get_base_source('Current', dict_files['Current']))

        logger.debug(f'Added {len(self.sources)} to sources.')

        # add dictionary to dynamic dims.
        # dynamic dims will be updated in external DB through API.
        # self.dynamic_dim expects dictionaries as elements.
        # before converting BaseSource to dictionary, we make a copy of it
        self.dynamic_dim['source'] += [vars(copy(base_source)) for base_source in self.sources]

        # run self.remove_existing_dynamic_dim to remove existing sources
        self.remove_existing_dynamic_dim('source')

    def parse_urls(self):
        """
        Helper function to get list of urls from website
        :return:
        """
        logger.info(f'Parsing file URLs to download from {self.main_page}')
        response = requests.get(self.main_page)
        if not response.ok:
            raise Exception(f"Problem accessing website {self.main_page}")

        page = response.content
        soup = BeautifulSoup(page, 'html.parser')
        crude_processing = None

        # find h5 with 'Crude Processing'
        for title in soup.find_all('h5'):
            logger.debug(f'Title found: {title.text}')
            if title.text == 'Crude Processing':
                crude_processing = title
                break

        if crude_processing is None:
            raise Exception(f"Problem accessing website {self.main_page}: section 'Crude Processing' not found.")

        # find unordered list after h5
        ul = crude_processing.find_next('ul')
        if not ul:
            raise AttributeError("Element <ul> not found after 'Crude Processing'. Check if the website has changed.")

        # gets a list of 'a' elements inside all 'li' elements under 'ul'
        # (find_all returns a list of a elements inside li, but there is always one per li)
        list_a = [li.select('a')[0] for li in ul.find_all('li')]
        logger.debug(f"List of 'a' elements: {list_a}")

        base_url = urlparse(self.main_page)

        # extract the first part of the text of a, and its url, returns as dictionary
        dict_url = {a.text.split(' ')[0]: f"{base_url.scheme}://{base_url.netloc}{a['href']}" for a in list_a}
        logger.info(f'Dictionary of file URLs: {dict_url}')

        return dict_url

    def get_base_source(self, file_type, file_url):
        """
        Helper function to generate a BaseSource object.
        :param file_type: a string describing the file type (Current, Historical)
        :param file_url: the url to the source file
        :return: a BaseSource object representing this source.
        """
        code = f'{self.provider_code}_{file_type}'

        return BaseSource(url=f'{file_url}',
                          code=code,
                          path=f'{code}.xls',
                          long_name=f"{self.area} {self.provider_code} {file_type} Crude Oil Processed at Refineries")

    def transform(self):
        """
        Method defining the transformations to be applied on the source data.
        :return:
        """
        logger.debug("Transforming data ...")
        self.__transform_provider()

        self.data = []
        dfs = []
        df = pd.DataFrame()

        for source in self.sources:
            file_path = FILE_STORE_PATH / source.path

            logger.info(f'Processing file {file_path}')
            xl = pd.ExcelFile(file_path)
            ts = self.get_file_timestamp(source)
            fiscal_year = ts.year if ts.month >= self.month_for_year_shift else ts.year - 1

            if "Current" in source.path:
                # let's clean tab names just in case the file comes with leading or trailing spaces
                tab_names = {tab.strip(): tab for tab in xl.sheet_names}

                df = self.process_sheet(source.code, xl, tab_names[self.current_tabname], fiscal_year)
                dfs.append(df)
            else:
                # process the historical file
                # filter monthly sheets
                monthly_sheets = [sheet for sheet in xl.sheet_names if "Monthwise" in sheet]
                # extract first year of each sheet name as fiscal_year
                fiscal_years = [int(sheet.strip().split(' ')[1].split('-')[0]) for sheet in monthly_sheets]

                # process monthly sheets for its fiscal year
                monthly_dfs = [self.process_sheet(source.code, xl, sheet_name, year)
                               for year, sheet_name in zip(fiscal_years, monthly_sheets)]
                dfs.extend(monthly_dfs)
                del monthly_dfs

                annual_sheets = [sheet for sheet in xl.sheet_names if "PT_crude_H" in sheet]
                annual_dfs = [self.process_sheet(source.code, xl, sheet_name)
                              for sheet_name in annual_sheets]
                dfs.extend(annual_dfs)
                del annual_dfs

        if len(dfs) > 1:
            df = pd.concat(dfs)
        # load results into self.data
        if len(df) > 0:
            logger.info(f"Number of transformed rows: {len(df)}")
            self.data.extend(df.to_dict('records'))

            self.compact_dynamic_dim('entity')
            self.compact_dynamic_dim('detail')
        else:
            logger.debug("No data to load.")

    def compact_dynamic_dim(self, key):
        """"
        Helper function to remove duplicates from dynamic dim entry.
        :param key: str: key to entry in self.dynamic_dim dict.
        """
        logger.info(f"Removing duplicates from dynamic dim entry: {key}")
        df = pd.DataFrame(self.dynamic_dim[key])
        logger.debug(f"Entries in {key} before deduplication: {len(df)}")
        df = df.drop_duplicates(subset='code')
        logger.debug(f"Entries in {key} after deduplication: {len(df)}")
        self.dynamic_dim[key] = df.to_dict('records')
        self.remove_existing_dynamic_dim(key)

    def get_file_timestamp(self, source):
        """
        Extract timestamp from source.url
        :param source: source to extract timestamp.
        :return: a datetime object.
        """
        # source.url.split('/')[-1]: extract last part of the URL
        # .split('.')[0]\: extract filename without extension
        # .split('_')[-1]: extract last part of the filename (the timestamp)

        logger.info(f"Extracting timestamp from {source.url}")
        text_timestamp = None
        try:
            text_timestamp = re.search(self.filename_pattern, source.url, re.IGNORECASE).group(1)
            logger.debug(f'Extracted timestamp: {text_timestamp}')

            # convert text to datetime (%d-%m-%Y)
            timestamp = datetime.strptime(text_timestamp, "%d-%m-%Y")
        except AttributeError as e:
            timestamp = date.today().replace(day=1) + relativedelta(months=-1)
            logger.warning(f'File {source.url} has no timestamp. Assuming previous month: {timestamp}')

        except ValueError as e:
            # sometimes they use '_' as date separator ...
            logger.warning(f"Failed to parse filename date: {text_timestamp}."
                           "Process will try to replace separator from'-' to '_'")
            text_timestamp = text_timestamp.replace('_', '-')
            if text_timestamp[0] in '_-':
                text_timestamp = text_timestamp[1:]

            timestamp = datetime.strptime(text_timestamp, "%d-%m-%Y")

        return timestamp

    def process_sheet(self, source_code, excel_file, sheet_name, fiscal_year=None):
        """
        Helper function to process a monthly data sheet.
        :param fiscal_year: int: fiscal year to process (April YYYY - Mar YYYY + 1).
        :param source_code: str: source code of the file.
        :param excel_file: pandas.Excel_File: an instance to an open Excel file.
        :param sheet_name: str: name of excel sheet to open.
        :return: pandas.DataFrame: data frame with data.
        """

        frequency = f"monthly data for year {fiscal_year}" if fiscal_year else "annual data history"
        logger.info(f"Processing {sheet_name} for {frequency} source_code {source_code}")

        df = excel_file.parse(sheet_name=sheet_name, na_values=self.null_values)

        # clean & transform the data frame
        df = self.__clean_sheet_df(df)

        # load companies into entity dimension
        self.__transform_companies(df)

        # split company / location
        df = self.__split_company_location(df)

        # load locations into detail dimension
        self.__transform_locations(df)

        # transform fiscal year
        df = self.__transform_fact_data(df, source_code, fiscal_year)

        return df

    def __transform_provider(self):
        """
         Loads the provider dimension.
         :return: None
         """
        logger.debug("Transforming provider ...")
        # prepare the provider record as a dictionary
        provider = dict()
        provider["code"] = self.provider_code
        provider["long_name"] = self.provider_name
        provider["url"] = self.main_page

        logger.debug(f"Adding provider to dynamic_dim: {self.provider_code}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    def __clean_sheet_df(self, df):
        """
        Helper method for cleaning raw data frame.
        :param df: raw data frame
        :return: cleaned and transformed data frame
        """
        logger.debug(f"Cleaning fiscal year. Number of rows before cleaning: {len(df)}")

        # find and fix header and skip rows
        # create boolean array testing if first column contains 'OIL COMPANIES'
        header = df.iloc[:, 0].str.contains('OIL COMPANIES')
        # replace nulls by False in the array
        header = header.fillna(False)
        # get index of row where header is True (OIL COMPANIES)
        header_index = df[header].index
        # set column headers to value in the array where value is True
        col_names = df.iloc[header_index].values.tolist()[0]
        df.columns = [col_name.strip() if type(col_name) == str else col_name for col_name in col_names]
        logger.debug(f'columns = {df.columns}')
        # skip rows to the first one after header
        df = df.iloc[header_index.values[0] + 1:, :]

        # drop null rows and columns
        df.dropna(how='all', inplace=True)
        df.dropna(axis='columns', how='all', inplace=True)

        # remove rows after GRAND TOTAL
        df.reset_index(inplace=True, drop=True)
        end_index = df[df.iloc[:, 0] == 'GRAND TOTAL'].index.values[0]
        logger.debug(f"end_index: {end_index}")
        df = df[:end_index]

        # drop TOTAL rows
        df = df[~(df.iloc[:, 0].str.contains('TOTAL'))]

        # remove TOTAL column
        cols = list(df.columns)
        df = df[cols[:-1]]

        # replace non-standard company names
        logger.debug(f"before standardisation: {df.iloc[:, 0]}")
        df.iloc[:, 0].replace(self.companies_to_replace, inplace=True)
        logger.debug(f"before standardisation: {df.iloc[:, 0]}")

        logger.info(f"Number of rows after cleaning: {len(df)}")
        return df

    @staticmethod
    def __split_company_location(df):
        """
        Helper function to split company and location
        :param df: clean data frame
        :return: transformed data frame
        """
        logger.debug("Splitting company and refinery location")
        # locations are the rows where months have numbers
        df_location = df[~(df.iloc[:, 1].isnull())]
        df_location = df_location.rename(columns={'OIL COMPANIES': 'location_code'})
        # company is the text before '-' in OIL COMPANIES
        # but first let's fix RIL,JAMNAGAR,GUJARAT -> RIL-JAMNAGAR,GUJARAT
        df_location['location_code'] = df_location['location_code'].replace(
            to_replace={'RIL,JAMNAGAR,GUJARAT': 'RIL-JAMNAGAR,GUJARAT'})
        # split columns
        logger.debug(f'current df: {df_location.iloc[:, 0].values}')
        df_location['company_code'] = df_location['location_code'].map(lambda x: x.split('-')[0])
        df_location['location_code'] = df_location['location_code'].map(lambda x: x.split('-')[1])
        return df_location

    def __transform_locations(self, df):
        """
        Helper method to load location into detail dimension.
        :param df: source data frame
        :return: None
        """
        logger.info("Transforming locations to load in detail dimension.")
        df_loc = df[['location_code']].drop_duplicates()
        df_loc.loc[:, 'json'] = df_loc.apply(lambda x: {'detail': 'None'}, axis=1)
        df_loc.loc[:, 'category'] = 'REFINERY_LOCATION'
        df_loc.loc[:, 'description'] = self.area + ' - ' + df_loc['location_code']

        df_loc.rename(columns={'location_code': 'code'}, inplace=True)

        logger.info(f"Loading {len(df_loc)} rows to detail dimension.")

        # add dictionary to dynamic dims
        self.__add_df_to_dynamic_dim('detail', df_loc)

    def __add_df_to_dynamic_dim(self, key, df):
        """
        Helper function to add a value to dynamic dim. If key does not exists, default to list
        :param key: str: key of dynamic dim.
        :param df: data frame to add to key entry as an array of dict.
        :return: None
        """
        self.dynamic_dim[key] = self.dynamic_dim.get(key, []) + df.to_dict('records')

    def __transform_fact_data(self, df, source_code, year=None):
        """
        Helper method to transform monthly data into External DB schema.
        :param df: DataFrame cleaned data frame.
        :param source_code: str source file code.
        :param year: int fiscal year for monthly data, None for annual history.
        :return: DataFrame transformed data frame.
        """
        frequency = f'year {year}' if year else 'annual data history'
        logger.info(f'Transforming fact data: {frequency}')
        periods_dict = None
        if year:
            # monthly data:
            # build a dictionary for concatenating the year to (monthly) period column names
            # x[:3] ensures that month name is a 3-letter abbreviation
            periods_dict = {x: f'{x[:3].upper()}{year}' if i < 9 else f'{x[:3].upper()}{str(int(year) + 1)}'
                            for i, x in enumerate(list(df.columns)[1:-1])}
        else:
            # yearly data:
            # extract first year of season in format 'YYYY-YYYY+1'
            periods_dict = {x: x.split('-')[0] for x in list(df.columns)[1:-1]}

        df = df.rename(columns=periods_dict)

        loc_cols = ['company_code', 'location_code'] + list(periods_dict.values())
        logger.debug(f'location columns: {loc_cols} dict: {periods_dict}')
        df = df[loc_cols]

        # let's convert column names to date
        # date_cols = [datetime.strptime(col.title(), '%b%Y') if col not in ('company_code', 'location_code') else col
        #             for col in list(df.columns)]
        # df.columns = date_cols
        df['company_code'] = self.provider_code + '-' + df['company_code']

        # Now time to unpivot the data frame
        df = df.melt(id_vars=['company_code', 'location_code'], var_name='period')
        df = df.rename(columns={'company_code': 'entity',
                                'location_code': 'detail'}) \
            .assign(area=self.area,
                    flow=self.flow,
                    provider=self.provider_code,
                    source=source_code,
                    product=self.product,
                    frequency=self.frequency_monthly if year else self.frequency_annual,
                    unit=self.unit,
                    original=self.original)

        logger.debug(f'Number of rows before removing null values: {len(df)}')
        df.dropna(axis='index', subset=['value'], inplace=True)

        logger.info(f'Number of processed rows in {frequency}: {len(df)}')

        return df

    def __transform_companies(self, df):
        """
        Helper method to extract companies and load them into self.dynamic_dim.
        :param df: input data frame.
        :return: None.
        """
        logger.info("Processing Entity dimension (companies).")
        # Get Oil company names
        # oil company names are rows with Nan in period columns
        df_companies = df.loc[df.iloc[:, 1:].isnull().all(axis='columns')][['OIL COMPANIES']]
        df_companies.columns = ['name']
        df_companies['code'] = df_companies['name'].map(lambda x: x.split('(')[1][:-1])
        df_companies['long_name'] = df_companies['name'].map(lambda x: x.split('(')[0].strip())
        del df_companies['name']

        # adding on-site companies by hand
        df_companies = pd.concat([df_companies, self.one_site_companies])

        # concatenate provider code as prefix to avoid conflicts
        df_companies['code'] = self.provider_code + '-' + df_companies['code']

        df_companies['category'] = 'company'
        df_companies.reset_index(drop=True, inplace=True)
        logger.info(f"Number of companies: {len(df_companies)}")

        # add dictionary to dynamic dims
        self.__add_df_to_dynamic_dim('entity', df_companies)
