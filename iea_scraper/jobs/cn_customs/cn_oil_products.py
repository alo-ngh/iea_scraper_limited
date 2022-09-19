from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.jobs.utils import convert_m3_to_kbd
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
AREA = 'CHINA'
ORIGINAL = True

IMPORTS = 0
EXPORTS = 1
FLOW = (IMPORTS, EXPORTS)
FLOW_CODE = ('IMPORTS', 'EXPORTS')


SOURCE_URL = "http://43.248.49.97/indexEn"

# assuming a delay of 1 month (data is published by 26th of each month)
PUBLICATION_DELAY = 1
START_YEAR = 2017
MONTHLY_DATA_YEAR = 2019

FILE_PREFIX = f"{JOB_CODE}_oilprod"
MAPPING_PATH = Path(__file__).parent
MAPPING_FILE = "cn_customs_mappings.xlsx"
MAPPING_SHEET = 'PRODUCTS'

ENCODING = 'iso-8859-1'
THOUSANDS = ','
NA_VALUES = ['-', '?']

# LITERS_PER_BARREL = 158.987294928


class CnOilProductsJob(job.ExtDbApiJob):
    """
    Defines the job for loading csv files detailing China customs data on oil products exchange (import and export).

    Files must be (for now) downloaded manually from http://43.248.49.97/indexEn using a Python notebook
    on (codebase project).

    The following fields must be set:

    * Flow: Import / Export
    * Currency: Renminbi Yuan
    * Period: <year>, <start month>, <end month> (if start and end not the same, click 'By month')
    * Select: 'Select commodity', comma-separated list of all Product_code from cn_customs_mappings.xlsx, PRODUCTS sheet

    History files were named cn_customs_oilprod_[imp|exp]_<year>.csv
    From 2019, extractions are made monthly and the file names are: cn_customs_oilprod_[imp|exp]_<year><month>.csv
    """
    title: str = "Chinese Customs - Oil Products Imports/Exports"

    def __init__(self, full_load=None):
        """"
        Constructor.
        @param full_load: True for full-load.
                          False loads latest available month (current month - PUBLICATION_DELAY).
        """
        self.full_load = full_load
        super().__init__()

    def get_sources(self):
        logger.debug("Getting sources...")
        # define current month file
        # reference month: current month - PUBLICATION_DELAY
        ref_month = date.today().replace(day=1) - relativedelta(months=PUBLICATION_DELAY)
        period = ref_month.strftime('%Y%m')

        for flow in FLOW:
            flow_label = FLOW_CODE[flow].lower()
            flow_suffix = flow_label[:3]

            ref_file = f"{FILE_PREFIX}_{flow_suffix}_{period}.csv"

            self.sources.append(BaseSource(url=SOURCE_URL,
                                           code=ref_file.split(".")[0],
                                           path=ref_file,
                                           long_name=f"{AREA} {PROVIDER} Oil products {flow_label} - {period}"))

            if self.full_load:
                # for the same year
                if ref_month.month > 1:
                    sources = [BaseSource(url=SOURCE_URL,
                                          code=f"{FILE_PREFIX}_{flow_suffix}_{ref_month.year}{period:02d}",
                                          path=f"{FILE_PREFIX}_{flow_suffix}_{ref_month.year}{period:02d}.csv",
                                          long_name=f"{AREA} {PROVIDER} Oil products {flow_label}"
                                          f" - {ref_month.year}{period:02d}")
                               for period in reversed(range(1, ref_month.month))]
                    self.sources.extend(sources)

                # monthly years
                sources = [BaseSource(url=SOURCE_URL,
                                      code=f"{FILE_PREFIX}_{flow_suffix}_{period[0]}{period[1]:02d}",
                                      path=f"{FILE_PREFIX}_{flow_suffix}_{period[0]}{period[1]:02d}.csv",
                                      long_name=f"{AREA} {PROVIDER} Oil products {flow_label}"
                                      f" - {period[0]}{period[1]:02d}")
                           for period in itertools.product(reversed(range(MONTHLY_DATA_YEAR, ref_month.year)),
                                                           reversed(range(1, 13)))]
                self.sources.extend(sources)
                # full year files
                sources = [BaseSource(url=SOURCE_URL,
                                      code=f"{FILE_PREFIX}_{flow_suffix}_{year}",
                                      path=f"{FILE_PREFIX}_{flow_suffix}_{year}.csv",
                                      long_name=f"{AREA} {PROVIDER} Oil products {flow_label} - {year}")
                           for year in reversed(range(START_YEAR, MONTHLY_DATA_YEAR))]
                self.sources.extend(sources)

            # add dictionary to dynamic dims
            self.dynamic_dim['source'] = [vars(copy(source)) for source in self.sources]
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
        df = pd.read_csv(StringIO(file_path.read_text(encoding=ENCODING).replace("\t", "")), \
                         thousands = THOUSANDS, \
                         na_values=NA_VALUES)
        logger.info(f'Source {source.path}: {len(df)} rows read.')

        df['source'] = source.code
        df['flow'] = FLOW_CODE[IMPORTS] if source.path[19:22] == 'imp' else FLOW_CODE[EXPORTS]

        df.dropna(axis='columns', how='all', inplace=True)
        return df

    def __transform_detail(self, df):
        """
        Loads the detail dimension into dynamic_dim.
        :param df: data frame containing all the product mappings.
        """

        df['code'] =  df['detail'] = 'cn_customs_' + df['external_db_code'] + '_' + df['Product_code'].astype('str')
        df['category'] = 'CN_CUSTOMS_PRODUCTS'
        df['description'] = df['Product_desc_en']
        df["json"] = df.apply(
            lambda x: {'cn_customs_product_code': x['Product_code']},
            axis=1)

        # keep only needed columns
        df = df[['code', 'json', 'category', 'description']]

        logger.debug(f"Adding details to dynamic_dim ...")
        self.dynamic_dim['detail'] = df.to_dict('records')
        self.remove_existing_dynamic_dim('detail')

    def __transform_data(self, df):
        """
        Transform the data frame and calculate entity dimension.
        :param df: data frame containing data from all files
        :return: None
        """
        logger.debug("Transforming data frame")
        mapping_filename = MAPPING_PATH / MAPPING_FILE
        df_map = pd.read_excel(mapping_filename, engine='openpyxl', sheet_name=MAPPING_SHEET)
        df_map = df_map[~df_map['external_db_code'].isnull()]
        logger.debug(f"Loaded {len(df_map)} from mapping file: {mapping_filename}")

        # process detail dimension
        self.__transform_detail(df_map)

        df_m = pd.merge(df, df_map, left_on='Commodity code', right_on='Product_code', how='left',
                        indicator='result_type')

        # check for non mapped products
        df_rej = df_m[(df_m['result_type'] == 'left_only') | (df_m['external_db_code'].isnull())]

        if len(df_rej) > 0:
            logger.error("Missing product codes detected. Aborting...")
            logger.error(df_rej)
            df_rej = df_rej[['Commodity code', 'Commodity']].drop_duplicates()
            df_rej['Rejected Products'] = df_rej['Commodity code'].map(str) + " - " + df_rej["Commodity"]
            missing_products = df_rej['Rejected Products'].drop_duplicates().tolist()
            raise ValueError(f'Missing products in mapping: {missing_products}')

        # check for unknown supplementary units
        df_rej = df_m[~(df_m['Supplimentary Unit'].isna() | (df_m['Supplimentary Unit'] == 'Litre'))]

        if len(df_rej) > 0:
            raise ValueError(f"Unexpected supplimentary unit: "
                             f"{df_rej['Supplimentary Unit'].drop_duplicates().tolist()}")

        # Calculate period
        df_m['period'] = df_m['Date of data'].apply(lambda x: pd.to_datetime(x, format='%Y%m')) \
                                             .dt.strftime("%b%Y").str.upper()

        # Calculate detail
        # cn_customs_<IEA product code>_<customs product code>
        df_m['detail'] = 'cn_customs_' + df_m['external_db_code'] + '_' + df_m['Product_code'].astype('str')

        # Calculate quantity in Kilotons from Kilograms
        df_m['KT'] = df_m['Quantity'] / 10 ** 6

        # Calculate KBD from supplimentary quantity (from liters)
        df_m['KBD'] = df_m.apply(lambda x:
                                 convert_m3_to_kbd(x['Supplimentary Quantity']/1000,
                                                   x['Date of data'] // 100,
                                                   x['Date of data'] % 100)
                                 , axis=1)

        # Keep only needed columns
        df_m = df_m[['source', 'period', 'flow', 'external_db_code', 'detail',
                     'Supplimentary Unit', 'Supplimentary Quantity',
                     'KT', 'KBD', 'Renminbi Yuan']]\
            .rename(columns={'external_db_code': 'product',
                             'Renminbi Yuan': 'NC'})
        # unpivot it !
        df_m = df_m.melt(id_vars=['source', 'period', 'flow', 'product', 'detail',
                                  'Supplimentary Unit', 'Supplimentary Quantity'],
                         value_vars=['KT', 'KBD', 'NC'],
                         var_name="unit",
                         value_name="value")

        # delete empty KBD rows and then drop 'Supplimentary Quantity' and 'Supplimentary Unit'
        df_m = df_m[~((df_m['unit'] == 'KBD') &
                      (df_m['Supplimentary Quantity'] == 0) &
                      (df_m['Supplimentary Unit'].isna()))]\
            .drop(columns=['Supplimentary Quantity', 'Supplimentary Unit'])

        # add static columns
        df_m = df_m.drop_duplicates()\
                   .assign(provider=PROVIDER,
                           area=AREA,
                           frequency=FREQUENCY,
                           original=ORIGINAL)

        # load results into self.data
        self.data.extend(df_m.to_dict('records'))
