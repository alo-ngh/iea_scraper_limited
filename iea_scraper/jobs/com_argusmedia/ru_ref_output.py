from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import get_driver, wait_file
from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT, ARGUS_USERNAME, ARGUS_PASSWORD

import datetime
import platform
from copy import copy
from pathlib import Path

import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)

if platform.system() == 'Windows':
    from win32com.client import Dispatch

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()
FREQUENCY = 'Monthly'
AREA = 'RUSSIA'
FLOW = 'REFGROUT'
UNIT = 'KT'
ORIGINAL = True

AUTH_URL = "https://myaccount.argusmedia.com/login"
BASE_URL = "https://direct.argusmedia.com/DataAndDownloads/DownloadFile/3666"

ORIGINAL_FILENAME = 'Russian refinery output.xlsx'
FILENAME = "ru_ref_output.xlsx"

HEADERS = {
"User-Agent":
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36"
}

# Entity and Product mapping
MAPPING_PATH = Path(__file__).parent
MAPPING_ENCODING = 'windows-1252'
MAPPING_REJ_FILE = f"{JOB_CODE}_ru_ref_output_rejected.csv"

# worksheet list to ignore
SHEETS_TO_IGNORE = ['Apr 2004',
                    'Nov 2006', 'Dec 2006',
                    'Jan 2007', 'Mar 2009', 'Apr 2009',
                    'Mar 2011', 'Apr 2011', 'Aug 2011',
                    'Aug 2012', 'Oct 2012',
                    'Aug 2013', 'Sep 2013']
# rows to skip on each worksheet in the excel file
ROWS_TO_SKIP = 5

# Pandas excel engine: openpyxl for .xlsx
EXCEL_ENGINE = 'openpyxl'


class RuRefOutputJob(ExtDbApiJob):
    """
    Loads Argus media "Russian refinery output.xlsx" file.
    When installing, don't forget:
    * to set variables ARGUS_USERNAME and ARGUS_PASSWORD on
    scraper/settings.py file.
    * to export the mappings from excel file (2 CSV files)
    """
    title: str = "Argus Media - Russian refinery output"

    def __init__(self, full_load=None, selected_wks=None):
        """"
        Constructor.
        @param full_load: True for full-load (all worksheets).
                          False loads selected_worksheets if provided,
                          latest available month (current month - PUBLICATION_DELAY) otherwise.
        """
        self.full_load = full_load
        self.selected_wks = selected_wks

        # initializes a dictionary with product and entity mappings
        self.mappings = {mapping: self.load_mapping(MAPPING_PATH, mapping) for mapping in ['product', 'entity']}
        super().__init__()

    def get_sources(self):
        """
        Generate an entry for the argus media file.
        :return: None
        """
        source = BaseSource(code=f"{JOB_CODE}_{FILENAME.split('.')[0]}",
                            url=BASE_URL,
                            path=f"{JOB_CODE}_{FILENAME}",
                            long_name=f"{AREA} {PROVIDER} "
                            f"'Russian refinery output.xlsx' from Argus Media Direct"
                            )
        self.sources.append(source)

        # add dictionary to dynamic dims
        for source in self.sources:
            dicto = vars(copy(source))
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    def transform(self):
        """
        Transform data into External-DB format.
        :return: None
        """
        logger.debug("Transforming data...")
        self.__transform_provider()
        self.data = []

        dfs = []
        for source in self.sources:
            # only one source is expected in self.sources
            xlsx_path = FILE_STORE_PATH / source.path

            logger.debug(f"Opening excel file: {xlsx_path}")
            excel = pd.ExcelFile(xlsx_path, engine=EXCEL_ENGINE)

            # select worksheets to load
            sheets_to_process = self.select_monthly_worksheets(excel, SHEETS_TO_IGNORE) if not self.selected_wks \
                else self.selected_wks

            # process excel file
            df = self.process_excel_file(excel, sheets_to_process)
            dfs.append(df)

        if len(dfs) == 0:
            logger.info("No data to transform.")
        else:
            df = pd.concat(dfs)

            self.__transform_entity()
            self.__transform_detail()

            df["detail"] = df["product_detail"].apply(lambda x:
                                                      PROVIDER + "_" + x.upper()
                                                      if x != 'TOTAL'
                                                      else 'None')

            # add standard columns
            df.drop(columns=["entity_url", "product_detail", "detail_desc", "entity_desc"], inplace=True)
            df = df.assign(area=AREA,
                           flow=FLOW,
                           provider=PROVIDER,
                           source=source.code,
                           frequency=FREQUENCY,
                           unit=UNIT,
                           original=ORIGINAL)

            # load results into self.data
            logger.info(f"Number of transformed rows: {str(len(df.index))}")
            self.data.extend(df.to_dict('records'))

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Loading provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = f"{PROVIDER} - {AREA} - Refinery Output"
        provider["url"] = "https://direct.argusmedia.com"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    def __transform_entity(self):
        """
        Deduplicate entity, exclude existing and then load them into self.dynamic_dim.
        :param df: the data frame with the data.
        """
        entity_df = self.mappings['entity'] \
            .drop_duplicates(subset='code') \
            .assign(category="refinery")

        entity_df["long_name"] = PROVIDER + " - " + AREA + " - " + entity_df["long_name"]
        entity_df["meta_data"] = entity_df.apply(lambda x: {'url': x['detail_url']}
        if x['detail_url'] else None, axis=1)
        entity_df.drop(columns=["detail_url"], inplace=True)

        # add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity_df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')

    def __transform_detail(self):
        """
        Deduplicate product details from mapping, exclude existing and then load into self.dynamic_dim.
        :param df: the data frame with the data (not used for now).
        """
        detail_df = self.mappings['product'] \
            .drop_duplicates(subset=['detail_code']) \
            .assign(category="REFINERY_PRODUCTS")

        # filter out TOTAL rows, we don't want them in detail
        detail_df = detail_df.query("detail_code != 'TOTAL'")

        detail_df.loc[:, 'code'] = PROVIDER + "_" + detail_df["detail_code"]
        detail_df.loc[:, "json"] = detail_df.apply(lambda x: {'detail': 'None'}, axis=1)
        detail_df.rename(columns={"detail_desc": "description"}, inplace=True)
        detail_df.drop(columns=['product_code', 'detail_code'], inplace=True)

        # add dictionary to dynamic dims
        self.dynamic_dim['detail'] = detail_df.to_dict('records')
        self.remove_existing_dynamic_dim('detail')

    def download_source(self, source):
        """
        Downloads the file described by source
        :param source: a Source object describing the file to download.
        :return: None
        """

        try:
            path, url = source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        with get_driver() as driver:
            logger.info(f'Authenticating to {AUTH_URL} with {ARGUS_USERNAME}...')
            driver.get(AUTH_URL)

            logger.debug(f'Entering username and password and clicking the button...')
            driver.find_element_by_id('username').send_keys(ARGUS_USERNAME)
            driver.find_element_by_id('password').send_keys(ARGUS_PASSWORD)
            driver.find_element_by_class_name('btn').click()

            file_path = FILE_STORE_PATH / ORIGINAL_FILENAME
            logger.debug(f'Downloading {ORIGINAL_FILENAME}...')
            try:
                file_path.unlink()
            except FileNotFoundError as e:
                pass

            driver.get(BASE_URL)
            wait_file(file_path, 5, 300)
            new_path = file_path.with_name(source.path)
            file_path.replace(new_path)

        setattr(source, 'last_download', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
        logger.debug("Download successful.")

    @staticmethod
    def load_mapping(path, mapping):
        """
        Generic mapping loader.
        For now, loads from csv.
        :param path: path up to the mapping
        :param mapping: mapping name
        :return: a data frame with the mapping
        """
        mapping_path = path / f'{mapping}_mapping.csv'
        logger.debug(f'loading product mapping {mapping_path}')

        df = pd.read_csv(mapping_path, encoding=MAPPING_ENCODING).set_index('argus_name')

        logger.debug(f"Mapping loaded: {df.shape[0]} rows and {df.shape[1]} columns.")
        return df

    def select_monthly_worksheets(self, excel_instance, sheets_to_ignore):
        """
        Selects the worksheets to process, excluding those to ignore.
        If self.full_load is False, returns all selected worksheets, otherwise only the last 3.

        :param excel_instance: excel instance returned by pd.ExcelFile()
        :param sheets_to_ignore: list of worksheet names to ignore
        :return: list of worksheet names to process
        """
        logger.info(f'Ignoring the following sheets: {sheets_to_ignore}')

        # filter out sheet 'Contents' and those to ignore
        selected_wks = [x for x in excel_instance.sheet_names if x != 'Contents' and x not in sheets_to_ignore]

        if not self.full_load:
            selected_wks = selected_wks[-3:]

        logger.debug(f'Number of selected worksheets: {len(selected_wks)}')
        logger.info(f'selected worksheets: {selected_wks}')
        return selected_wks

    def process_excel_file(self, excel_instance, sheets_to_process):
        """
        Process a given excel file.
        :param excel_instance: excel instance returned by pd.ExcelFile()
        :param sheets_to_process:
        """
        # read all worksheets into data frames
        list_of_wks_dfs = [self.process_worksheet(excel_instance, wks) for wks in sheets_to_process]

        # concat results and join with mappings
        df, df_rejs = pd.concat(list_of_wks_dfs) \
            .pipe(self.join_mappings, self.mappings)

        if len(df_rejs) > 0:
            csv_rej_path = FILE_STORE_PATH / MAPPING_REJ_FILE
            df_rejs.to_csv(path_or_buf=csv_rej_path, index=False)
            logger.warning(f'Reject file {csv_rej_path} exported. Number of rejected rows: {df_rejs.shape[0]}')

        return df

    def process_worksheet(self, excel_instance, wks):
        """
        Process each selected worksheet of the excel file.
        :param excel_instance: excel instance returned by pd.ExcelFile()
        :param wks: worksheet to process
        :return: data frame with the results
        """
        logger.info(f'processing {wks}')

        df = (excel_instance.parse(sheet_name=wks, skiprows=ROWS_TO_SKIP)
              .pipe(self.clean_data, wks)
              )

        df = (self.melt_and_concat(self.break_into_tables(df))
              .pipe(self.add_period, wks)
              .pipe(self.standardize_data)
              )

        return df

    @staticmethod
    def clean_data(df, wks):
        """
        Filter empty or non-rows
        :param df: data frame to process
        :param wks: current worksheet
        :return: resulting data frame
        """
        logger.debug("Cleaning data...")
        df = df.copy()

        if wks in ['Feb 2003', 'Oct 2003', 'Oct 2004']:
            logger.debug(f"{wks}: Renaming column 'Other motor gasoline.1' to 'Other motor gasoline YTD'")
            df.rename(columns={'Other motor gasoline.1': 'Other motor gasoline YTD'}, inplace=True)
        elif wks == 'Jan 2013':
            logger.debug(f"{wks}: Replacing 'Company/refinery ’000t' by 'Company/refinery'")
            df["Company/refinery"].replace("Company/refinery ’000t", "Company/refinery", inplace=True)
            df.replace("  10ppm diesel  Jan 13", "GASOIL 0.001%", inplace=True)
        elif wks == 'Jun 2013':
            logger.debug(f"{wks}: Replacing 'Gasoil 0.2%' by 'Gasoil 0.2% YTD'")
            df.replace("Gasoil 0.2%", "Gasoil 0.2% YTD", inplace=True)
            logger.debug(f"{wks}: Replacing 'Gasoil 0.2%, Jun' by 'Gasoil 0.2%'")
            df.replace("Gasoil 0.2%, Jun", "Gasoil 0.2%", inplace=True)
        elif wks in ['Jul 2013', 'Aug 2013', 'Sep 2013']:
            logger.debug(f"{wks}: Replacing 'Total  fuel oil' by 'Total fuel oil YTD'")
            df.replace("Total  fuel oil", "Total fuel oil YTD", inplace=True)
        elif wks in ['Oct 2013', 'Nov 2013', 'Dec 2013', 'Feb 2014', 'Mar 2014', 'Apr 2014']:
            logger.debug(f"{wks}: Replacing 'Total fuel oil' by 'Total fuel oil YTD'")
            df.iloc[:, 3].replace('Total fuel oil', 'Total fuel oil YTD', inplace=True)
        elif wks == 'Jan 2017':
            logger.debug(f"{wks}: Erasing 2nd occurrence of 'Fuel oil 2.5%'")
            # all occurrences of 'Fuel oil 2.5%' on second column (2 expected)
            start_indexes = df.loc[df.iloc[:, 1] == "Fuel oil 2.5%"].index
            if len(start_indexes) == 2:
                # all occurrences of 'Comparisons based on average daily volumes (we want the last one)
                end_indexes = df.loc[df["Company/refinery"] == "Comparisons based on average daily volumes"].index
                # write NaN on the range of the repeated occurrence (2nd column)
                df.iloc[start_indexes[-1] + 1:end_indexes[-1] + 1, 1] = np.nan

        # drop empty rows
        df.dropna(how='all', inplace=True)
        # filter out 'Russian refinery ...' rows
        df = df[~df["Company/refinery"].str.contains("Russian refinery output")]
        df = df[~df["Company/refinery"].str.contains("Comparisons")]
        return df

    @staticmethod
    def melt_and_concat(table_dict):
        """
        Unpivot each data frame of the dictionary and concat them.
        It returns the resulting data frame.
        :param table_dict: dictionary containing data frames
        :return: resulting data frame
        """
        logger.debug('melting and concatenating data ...')
        list_of_melted_dfs = None
        # for each table, melt and concat
        for df in table_dict.values():
            # drop empty columns
            df.dropna(how='all', axis=1, inplace=True)
            # get the list of columns to unpivot
            cols_to_maintain = [x for x in df.columns
                                if (x.upper() != 'CRUDE THROUGHPUT ’000 B/D')
                                and (x.upper() != 'CRUDE THROUGHPUT \'000 B/D')
                                and (x.upper() != '10PPM DIESEL  JAN 12')
                                and ("±%" not in x)
                                and ("YTD" not in x.upper())]

            logger.debug(f'maintaining these  columns: {cols_to_maintain}')
            df = df[cols_to_maintain]

            var_cols = [x for x in df.columns if (x != "Company/refinery")]

            logger.debug(f'before melt: {df}')
            # unpivot it !
            melted_df = df.melt(id_vars=["Company/refinery"],
                                value_vars=var_cols,
                                var_name="Product",
                                value_name="Value")

            # concat data frames
            if not list_of_melted_dfs:
                list_of_melted_dfs = [melted_df]
            else:
                list_of_melted_dfs.append(melted_df)

        # concat all data frames
        return pd.concat(list_of_melted_dfs, ignore_index=True)

    @staticmethod
    def break_into_tables(df):
        """
        It split the worksheets in separate data frames when finding a title line.
        It returns a dictionary of data frames.
        :param df: data frame to break into tables
        :return: a dictionary with data frames for each table
        """
        logger.debug('breaking into tables ...')
        # get the index of table breaks

        break_index = df.loc[df["Company/refinery"] == "Company/refinery"].index

        # loop to split tables
        table_dic = {}
        start_index = 0
        for num, index in enumerate(break_index):
            end_index = (index - 1)
            table_dic[num] = df.loc[start_index:end_index].copy()
            if num != 0:
                table_dic[num].columns = table_dic[num].loc[start_index].values
                table_dic[num] = table_dic[num].iloc[1:]
            start_index = index

        # this creates a data frame for the last table
        table_dic[num + 1] = df.loc[start_index:].copy()
        table_dic[num + 1].columns = table_dic[num + 1].loc[start_index].values
        table_dic[num + 1] = table_dic[num + 1].iloc[1:]

        return table_dic

    @staticmethod
    def add_period(df, period):
        """
        It returns a data frame with a Period column added as the 1st column.
        :param df: data frame to process.
        :param period: period to add.
        :return: resulting data frame.
        """
        logger.debug('adding period ...')
        # add period col
        df = df.copy()
        period = period.replace(" ", "").upper()
        df['Period'] = period

        # get the list of cols
        cols = df.columns.tolist()

        # Put 'Period' at first
        cols = cols[-1:] + cols[:-1]
        return df[cols]

    @staticmethod
    def standardize_data(df):
        """
        Last cleaning actions after transforming data.
        :param df: data frame to process.
        :return: resulting data frame.
        """
        df = df.rename(str.lower, axis='columns') \
            .rename(columns={'company/refinery': 'entity'})

        # standardize first column
        df["entity"] = df["entity"].str.lstrip("\xa0") \
            .str.strip(" ") \
            .str.upper()

        # standardize product column
        df["product"] = df["product"].str.strip(" ") \
            .str.upper()

        # post-cleaning values: keep only rows where Value is numeric and not empty
        df["value"] = pd.to_numeric(df["value"], errors='coerce')
        df.dropna(inplace=True)

        return df

    @staticmethod
    def join_mappings(df, mappings):
        """
        Join the given data frame with product and entity mappings.
        :param df: a data frame from an excel worksheet
        :param mappings: a dictionary containing mappings for product and entity
        :return: df - the resulting data frame, df_rej - a data frame with rejected data
        """
        logger.debug(f"columns before join with product: {list(df)}")
        # join df x product mapping
        df = df.join(mappings['product'], on='product', rsuffix='_product')
        logger.debug(f"columns after join with product: {list(df)}")

        # get rejected rows from df x product mapping
        # keep only distinct 'period' and 'product'
        # add 'type column'
        df_rej_prods = df[df["product_code"].isnull()][['period', 'product']].drop_duplicates() \
            .rename(columns={'product': 'value'}) \
            .assign(type='product')

        if len(df_rej_prods) > 0:
            logger.warning(f"Rejected rows: {df_rej_prods.shape[0]} distinct (period, product) pairs not mapped.")

        # join with entity mapping
        logger.debug(f"columns before join with entity: {list(df)}")
        df = df.join(mappings['entity'], on='entity', rsuffix='_entity')
        logger.debug(f"columns after join with entity: {list(df)}")

        # get rejected rows from df x entity mapping
        # keep only distinct 'period' and 'product'
        # add 'type column'
        df_rej_ents = df[df["code"].isnull()][['period', 'entity']].drop_duplicates() \
            .rename(columns={'entity': 'value'}) \
            .assign(type='entity')

        if len(df_rej_ents) > 0:
            logger.warning(f"Rejected rows: {df_rej_ents.shape[0]} distinct (period, entities) pairs not mapped.")

        # merge rejecteds
        df_rej = df_rej_ents.append(df_rej_prods, sort=False)[['period', 'type', 'value']]

        # filter out rejected rows
        df = df[~df["product_code"].isnull() & ~df["code"].isnull()] \
            .drop(columns=['entity', 'product']) \
            .rename(columns={'product_code': 'product',
                             'detail_code': 'product_detail',
                             'code': 'entity',
                             'long_name': 'entity_desc',
                             'detail_url': 'entity_url'})

        df = df[['period', 'entity', 'entity_desc', 'entity_url',
                 'product', 'product_detail', 'detail_desc', 'value']].drop_duplicates()
        return df, df_rej
