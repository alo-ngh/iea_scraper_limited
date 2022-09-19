from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.settings import FILE_STORE_PATH

import itertools
import pandas as pd
from copy import copy
from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
from io import StringIO

import logging

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()
FREQUENCY = 'Monthly'
PRODUCT = 'CRUDEOIL'
AREA = 'CHINA'
FLOW = 'IMPORTS'
ORIGINAL = True

SOURCE_URL = "http://43.248.49.97/indexEn"

# assuming a delay of 1 months
PUBLICATION_DELAY = 1
START_YEAR = 2017
MONTHLY_DATA_YEAR = 2019

FILE_PREFIX = f"{JOB_CODE}_crudeoil_imp"
MAPPING_PATH = Path(__file__).parent
MAPPING_FILE = "cn_customs_mappings.xlsx"
MAPPING_SHEET = 'COUNTRIES'
THOUSANDS = ','

ENCODING = 'iso-8859-1'


class CnCrudeImportsJob(job.ExtDbApiJob):
    """
    Defines the job for loading csv files detailing China customs crude oil imports.

    Files must be (for now) downloaded manually from http://43.248.49.97/indexEn.
    The following fields must be set:

    * Flow: Import
    * Currency: Renminbi Yuan
    * Period: <year>, <start month>, <end month> (if start and end not the same, click 'By month')
    * Select: 'Select commodity', 27090000
    * Select: 'Select partner', comma-separated list of all country_code from cn_customs_mappings.xlsx, COUNTRIES sheet

    History files were named cn_customs_crude_oil_imports_<year>.csv
    From 2019, extractions are made monthly and the file names are: cn_customs_crude_oil_imports_<year><month>.csv
    """
    title: str = "Chinese Customs - Crude Oil Imports"

    def get_sources(self):
        logger.debug("Getting sources...")
        # define current month file
        # reference month: current month - PUBLICATION_DELAY
        ref_month = date.today().replace(day=1) - relativedelta(months=PUBLICATION_DELAY)
        period = ref_month.strftime('%Y%m')
        ref_file = f"{FILE_PREFIX}_{period}.csv"

        self.sources.append(BaseSource(url=SOURCE_URL,
                                       code=ref_file.split(".")[0],
                                       path=ref_file,
                                       long_name=f"{AREA} {PROVIDER} Crude Oil Imports - {period}"))

        if self.full_load:
            # for the same year
            if ref_month.month > 1:
                sources = [BaseSource(url=SOURCE_URL,
                                      code=f"{FILE_PREFIX}_{ref_month.year}{period:02d}",
                                      path=f"{FILE_PREFIX}_{ref_month.year}{period:02d}.csv",
                                      long_name=f"{AREA} {PROVIDER} Crude Oil Imports - {ref_month.year}{period:02d}")
                           for period in reversed(range(1, ref_month.month))]
                self.sources.extend(sources)

            # monthly years
            sources = [BaseSource(url=SOURCE_URL,
                                  code=f"{FILE_PREFIX}_{period[0]}{period[1]:02d}",
                                  path=f"{FILE_PREFIX}_{period[0]}{period[1]:02d}.csv",
                                  long_name=f"{AREA} {PROVIDER} Crude Oil Imports - {period[0]}{period[1]:02d}")
                       for period in itertools.product(reversed(range(MONTHLY_DATA_YEAR, ref_month.year)),
                                                       reversed(range(1, 13)))]
            self.sources.extend(sources)
            # full year files
            sources = [BaseSource(url=SOURCE_URL,
                                  code=f"{FILE_PREFIX}_{year}",
                                  path=f"{FILE_PREFIX}_{year}.csv",
                                  long_name=f"{AREA} {PROVIDER} Crude Oil Imports - {year}")
                       for year in reversed(range(START_YEAR, MONTHLY_DATA_YEAR))]
            self.sources.extend(sources)

            # TODO file for 2006-2016
            sources = [BaseSource(url=SOURCE_URL,
                                 code=f"{FILE_PREFIX}_2006",
                                 path=f"{FILE_PREFIX}_2006.csv",
                                 long_name=f"{AREA} {PROVIDER} Crude Oil Imports - 2006-2016")]
            self.sources.extend(sources)

            # file for 1995-2015
            sources = [BaseSource(url=SOURCE_URL,
                                  code=f"{FILE_PREFIX}_1995",
                                  path=f"{FILE_PREFIX}_1995.csv",
                                  long_name=f"{AREA} {PROVIDER} Crude Oil Imports - 1995-2005")]
            self.sources.extend(sources)

        # add dictionary to dynamic dims
        dicto = [vars(copy(source)) for source in self.sources]
        self.dynamic_dim['source'] += dicto

        self.remove_existing_dynamic_dim('source')

    @staticmethod
    def download_source(source):
        """
        Overriding parent's method to avoid any download.
        For now, file download is MANUAL.
        :param source: BaseSource object describing the file to download.
        Defined as a static method to allow overloading
        """
        logger.warn("Download method not implemented")

    def transform(self):
        """"
        Loads all source files in self.sources, merge with mappings and processes them.
        """
        logger.debug("Transforming data ...")
        self.__transform_provider()

        dfs = parallelize(self.__get_data_from_source, self.sources, job.MAX_WORKER)

        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs, sort=False)
                self.data = []
                self.__transform_data(df)
            except ValueError as e:
                logger.warn(f"Error while concatenating data frames, not transforming data: {e}")
        return None

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Transforming provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = "China - Customs"
        provider["url"] = SOURCE_URL

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    @staticmethod
    def __get_data_from_source(source):
        """
        Transform each downloaded source file.
        :param source: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """
        file_path = FILE_STORE_PATH / source.path
        # preprocessing the file: removing unnecessary \t before passing to pandas
        df = pd.read_csv(StringIO(file_path.read_text(encoding=ENCODING).replace("\t", "")), thousands=THOUSANDS)
        logger.info(f'Source {source.path}: {len(df)} rows read.')

        if 'Date of data' not in list(df):
            period = source.code.split('_')[-1]
            if len(period) == 6:
                logger.debug(f"Monthly data: adding {period} as column 'Date of data' for {source.path}")
                df['Date of data'] = period
        df['source'] = source.code

        df.dropna(axis='columns', how='all', inplace=True)
        return df

    def __transform_data(self, df):
        """
        Transform the data frame and calculate entity dimension.
        :param df: data frame containing data from all files
        :return: None
        """
        logger.debug("Transforming data frame")
        mapping_filename = MAPPING_PATH / MAPPING_FILE
        df_map = pd.read_excel(mapping_filename, engine='openpyxl', sheet_name='COUNTRIES')
        df_map = df_map[~df_map['code'].isnull()]
        logger.debug(f"Loaded {len(df_map)} from mapping file: {mapping_filename}")

        df_m = pd.merge(df, df_map, left_on='Trading partner code', right_on='country_code', how='left',
                        indicator='result_type')

        df_rej = df_m[(df_m['result_type'] == 'left_only') | (df_m['code'].isnull())]

        if len(df_rej) > 0:
            logger.error("Missing country codes detected. Aborting...")
            logger.error(df_rej)
            df_rej = df_rej[['Trading partner code', 'Trading partner']].drop_duplicates()
            df_rej['Rejected Countries'] = df_rej['Trading partner code'].map(str) + " - " + df_rej["Trading partner"]
            missing_countries = df_rej['Rejected Countries'].drop_duplicates().tolist()
            raise ValueError(f'Missing countries in mapping: {missing_countries}')

        # Convert quantity from Kilograms to Kilotons
        df_m['KT'] = df_m['Quantity'] / 10 ** 6

        # Keeping only needed columns
        df_m['period'] = df_m['Date of data'].apply(lambda x: pd.to_datetime(x, format='%Y%m')) \
                                             .dt.strftime("%b%Y").str.upper()

        df_m = df_m.drop(columns=['Date of data',
                                  'Commodity code',
                                  'Commodity',
                                  'Trading partner code',
                                  'Trading partner',
                                  'Quantity'])\
                   .rename(columns={'code': 'to_area',
                           'Renminbi Yuan': 'NC'})
        # unpivot it !
        df_m = df_m.melt(id_vars=['source', 'period', 'to_area'],
                         value_vars=['KT', 'NC'],
                         var_name="unit",
                         value_name="value")

        # if the value is NaN, delete the row
        df_m.dropna(axis='index', subset=['value'], inplace=True)

        # add static columns
        df_m = df_m.drop_duplicates()\
                   .assign(provider=PROVIDER,
                           area=AREA,
                           product=PRODUCT,
                           frequency=FREQUENCY,
                           flow=FLOW,
                           original=ORIGINAL)

        # load results into self.data
        self.data.extend(df_m.to_dict('records'))



