import datetime
import logging
from copy import copy
from datetime import date
from pathlib import Path
from typing import NoReturn
from sqlalchemy import create_engine
import pandas as pd

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.settings import FILE_STORE_PATH, EXT_DB_STR

logger = logging.getLogger(__name__)


class FuturesAndOptionsCombJob(ExtDbApiDedicatedTableJob):
    """
    Scraper for Disaggregated Futures-and-Options Combined Reports released on a weekly basis.

    We are loading individual historical data files from 2017 and grouped historical file for
    2006-2016.

    Source: https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm
    """
    title: str = 'CFTC Disaggregated Commitments of Traders FutOnly and Futures-and-Options Combined Reports'

    max_worker = 3
    file_types = ['com', 'fut']
    source_pattern = '_disagg_txt_'

    base_file_url = 'https://www.cftc.gov/files/dea/history/'
    base_file_ext = '.zip'

    # provider code: the directory just after scraper/jobs (gov_cftc)
    job_code = Path(__file__).parent.parts[-1]
    provider_code = job_code.upper()
    provider_long_name = "CFTC - Commodity Futures Trading Commission"
    provider_url = 'https://www.cftc.gov/'

    default_start_year = 2017
    hist_code = 'h06-16'
    hist_combined_years = 'hist_2006_2016'

    # current year
    current_year = date.today().year

    # key column name
    week_date_column = 'Week_of_Release'
    key_columns = ['provider', 'Week_of_Release', 'CFTC_Contract_Market_Code',
                   'CFTC_Market_Code', 'CFTC_Region_Code', 'CFTC_Commodity_Code', 'FutOnly_or_Combined']

    # column headers for each source file
    csv_columns = {'Market_and_Exchange_Names': str,
                   'As_of_Date_In_Form_YYMMDD': str,
                   'Report_Date_as_YYYY-MM-DD': str,
                   'CFTC_Contract_Market_Code': str,
                   'CFTC_Market_Code': str,
                   'CFTC_Region_Code': int,
                   'CFTC_Commodity_Code': str,
                   'Open_Interest_All': float,
                   'Prod_Merc_Positions_Long_All': int,
                   'Prod_Merc_Positions_Short_All': int,
                   'Swap_Positions_Long_All': int,
                   'Swap_Positions_Short_All': int,
                   'Swap_Positions_Spread_All': int,
                   'M_Money_Positions_Long_All': int,
                   'M_Money_Positions_Short_All': int,
                   'M_Money_Positions_Spread_All': int,
                   'Other_Rept_Positions_Long_All': int,
                   'Other_Rept_Positions_Short_All': int,
                   'Other_Rept_Positions_Spread_All': int,
                   'Tot_Rept_Positions_Long_All': float,
                   'Tot_Rept_Positions_Short_All': float,
                   'NonRept_Positions_Long_All': int,
                   'NonRept_Positions_Short_All': int,
                   'Open_Interest_Old': float,
                   'Prod_Merc_Positions_Long_Old': int,
                   'Prod_Merc_Positions_Short_Old': int,
                   'Swap_Positions_Long_Old': int,
                   'Swap_Positions_Short_Old': int,
                   'Swap_Positions_Spread_Old': int,
                   'M_Money_Positions_Long_Old': int,
                   'M_Money_Positions_Short_Old': int,
                   'M_Money_Positions_Spread_Old': int,
                   'Other_Rept_Positions_Long_Old': int,
                   'Other_Rept_Positions_Short_Old': int,
                   'Other_Rept_Positions_Spread_Old': int,
                   'Tot_Rept_Positions_Long_Old': float,
                   'Tot_Rept_Positions_Short_Old': float,
                   'NonRept_Positions_Long_Old': float,
                   'NonRept_Positions_Short_Old': float,
                   'Open_Interest_Other': float,
                   'Prod_Merc_Positions_Long_Other': float,
                   'Prod_Merc_Positions_Short_Other': float,
                   'Swap_Positions_Long_Other': float,
                   'Swap_Positions_Short_Other': float,
                   'Swap_Positions_Spread_Other': float,
                   'M_Money_Positions_Long_Other': float,
                   'M_Money_Positions_Short_Other': float,
                   'M_Money_Positions_Spread_Other': float,
                   'Other_Rept_Positions_Long_Other': float,
                   'Other_Rept_Positions_Short_Other': float,
                   'Other_Rept_Positions_Spread_Other': float,
                   'Tot_Rept_Positions_Long_Other': float,
                   'Tot_Rept_Positions_Short_Other': float,
                   'NonRept_Positions_Long_Other': float,
                   'NonRept_Positions_Short_Other': float,
                   'Change_in_Open_Interest_All': float,
                   'Change_in_Prod_Merc_Long_All': float,
                   'Change_in_Prod_Merc_Short_All': float,
                   'Change_in_Swap_Long_All': float,
                   'Change_in_Swap_Short_All': float,
                   'Change_in_Swap_Spread_All': float,
                   'Change_in_M_Money_Long_All': float,
                   'Change_in_M_Money_Short_All': float,
                   'Change_in_M_Money_Spread_All': float,
                   'Change_in_Other_Rept_Long_All': float,
                   'Change_in_Other_Rept_Short_All': float,
                   'Change_in_Other_Rept_Spread_All': float,
                   'Change_in_Tot_Rept_Long_All': float,
                   'Change_in_Tot_Rept_Short_All': float,
                   'Change_in_NonRept_Long_All': float,
                   'Change_in_NonRept_Short_All': float,
                   'Pct_of_Open_Interest_All': float,
                   'Pct_of_OI_Prod_Merc_Long_All': float,
                   'Pct_of_OI_Prod_Merc_Short_All': float,
                   'Pct_of_OI_Swap_Long_All': float,
                   'Pct_of_OI_Swap_Short_All': float,
                   'Pct_of_OI_Swap_Spread_All': float,
                   'Pct_of_OI_M_Money_Long_All': float,
                   'Pct_of_OI_M_Money_Short_All': float,
                   'Pct_of_OI_M_Money_Spread_All': float,
                   'Pct_of_OI_Other_Rept_Long_All': float,
                   'Pct_of_OI_Other_Rept_Short_All': float,
                   'Pct_of_OI_Other_Rept_Spread_All': float,
                   'Pct_of_OI_Tot_Rept_Long_All': float,
                   'Pct_of_OI_Tot_Rept_Short_All': float,
                   'Pct_of_OI_NonRept_Long_All': float,
                   'Pct_of_OI_NonRept_Short_All': float,
                   'Pct_of_Open_Interest_Old': float,
                   'Pct_of_OI_Prod_Merc_Long_Old': float,
                   'Pct_of_OI_Prod_Merc_Short_Old': float,
                   'Pct_of_OI_Swap_Long_Old': float,
                   'Pct_of_OI_Swap_Short_Old': float,
                   'Pct_of_OI_Swap_Spread_Old': float,
                   'Pct_of_OI_M_Money_Long_Old': float,
                   'Pct_of_OI_M_Money_Short_Old': float,
                   'Pct_of_OI_M_Money_Spread_Old': float,
                   'Pct_of_OI_Other_Rept_Long_Old': float,
                   'Pct_of_OI_Other_Rept_Short_Old': float,
                   'Pct_of_OI_Other_Rept_Spread_Old': float,
                   'Pct_of_OI_Tot_Rept_Long_Old': float,
                   'Pct_of_OI_Tot_Rept_Short_Old': float,
                   'Pct_of_OI_NonRept_Long_Old': float,
                   'Pct_of_OI_NonRept_Short_Old': float,
                   'Pct_of_Open_Interest_Other': float,
                   'Pct_of_OI_Prod_Merc_Long_Other': float,
                   'Pct_of_OI_Prod_Merc_Short_Other': float,
                   'Pct_of_OI_Swap_Long_Other': float,
                   'Pct_of_OI_Swap_Short_Other': float,
                   'Pct_of_OI_Swap_Spread_Other': float,
                   'Pct_of_OI_M_Money_Long_Other': float,
                   'Pct_of_OI_M_Money_Short_Other': float,
                   'Pct_of_OI_M_Money_Spread_Other': float,
                   'Pct_of_OI_Other_Rept_Long_Other': float,
                   'Pct_of_OI_Other_Rept_Short_Other': float,
                   'Pct_of_OI_Other_Rept_Spread_Other': float,
                   'Pct_of_OI_Tot_Rept_Long_Other': float,
                   'Pct_of_OI_Tot_Rept_Short_Other': float,
                   'Pct_of_OI_NonRept_Long_Other': float,
                   'Pct_of_OI_NonRept_Short_Other': float,
                   'Traders_Tot_All': float,
                   'Traders_Prod_Merc_Long_All': float,
                   'Traders_Prod_Merc_Short_All': float,
                   'Traders_Swap_Long_All': float,
                   'Traders_Swap_Short_All': float,
                   'Traders_Swap_Spread_All': float,
                   'Traders_M_Money_Long_All': float,
                   'Traders_M_Money_Short_All': float,
                   'Traders_M_Money_Spread_All': float,
                   'Traders_Other_Rept_Long_All': float,
                   'Traders_Other_Rept_Short_All': float,
                   'Traders_Other_Rept_Spread_All': float,
                   'Traders_Tot_Rept_Long_All': float,
                   'Traders_Tot_Rept_Short_All': float,
                   'Traders_Tot_Old': float,
                   'Traders_Prod_Merc_Long_Old': float,
                   'Traders_Prod_Merc_Short_Old': float,
                   'Traders_Swap_Long_Old': float,
                   'Traders_Swap_Short_Old': float,
                   'Traders_Swap_Spread_Old': float,
                   'Traders_M_Money_Long_Old': float,
                   'Traders_M_Money_Short_Old': float,
                   'Traders_M_Money_Spread_Old': float,
                   'Traders_Other_Rept_Long_Old': float,
                   'Traders_Other_Rept_Short_Old': float,
                   'Traders_Other_Rept_Spread_Old': float,
                   'Traders_Tot_Rept_Long_Old': float,
                   'Traders_Tot_Rept_Short_Old': float,
                   'Traders_Tot_Other': float,
                   'Traders_Prod_Merc_Long_Other': float,
                   'Traders_Prod_Merc_Short_Other': float,
                   'Traders_Swap_Long_Other': float,
                   'Traders_Swap_Short_Other': float,
                   'Traders_Swap_Spread_Other': float,
                   'Traders_M_Money_Long_Other': float,
                   'Traders_M_Money_Short_Other': float,
                   'Traders_M_Money_Spread_Other': float,
                   'Traders_Other_Rept_Long_Other': float,
                   'Traders_Other_Rept_Short_Other': float,
                   'Traders_Other_Rept_Spread_Other': float,
                   'Traders_Tot_Rept_Long_Other': float,
                   'Traders_Tot_Rept_Short_Other': float,
                   'Conc_Gross_LE_4_TDR_Long_All': float,
                   'Conc_Gross_LE_4_TDR_Short_All': float,
                   'Conc_Gross_LE_8_TDR_Long_All': float,
                   'Conc_Gross_LE_8_TDR_Short_All': float,
                   'Conc_Net_LE_4_TDR_Long_All': float,
                   'Conc_Net_LE_4_TDR_Short_All': float,
                   'Conc_Net_LE_8_TDR_Long_All': float,
                   'Conc_Net_LE_8_TDR_Short_All': float,
                   'Conc_Gross_LE_4_TDR_Long_Old': float,
                   'Conc_Gross_LE_4_TDR_Short_Old': float,
                   'Conc_Gross_LE_8_TDR_Long_Old': float,
                   'Conc_Gross_LE_8_TDR_Short_Old': float,
                   'Conc_Net_LE_4_TDR_Long_Old': float,
                   'Conc_Net_LE_4_TDR_Short_Old': float,
                   'Conc_Net_LE_8_TDR_Long_Old': float,
                   'Conc_Net_LE_8_TDR_Short_Old': float,
                   'Conc_Gross_LE_4_TDR_Long_Other': float,
                   'Conc_Gross_LE_4_TDR_Short_Other': float,
                   'Conc_Gross_LE_8_TDR_Long_Other': float,
                   'Conc_Gross_LE_8_TDR_Short_Other': float,
                   'Conc_Net_LE_4_TDR_Long_Other': float,
                   'Conc_Net_LE_4_TDR_Short_Other': float,
                   'Conc_Net_LE_8_TDR_Long_Other': float,
                   'Conc_Net_LE_8_TDR_Short_Other': float,
                   'Contract_Units': str,
                   'CFTC_Contract_Market_Code_Quotes': str,
                   'CFTC_Market_Code_Quotes': str,
                   'CFTC_Commodity_Code_Quotes': str,
                   'CFTC_SubGroup_Code': str,
                   'FutOnly_or_Combined': str}
    null_values = ['.', ' ']

    # DB schema
    db_schema = 'main'
    db_table_prefix = 'futures_options'

    chunk_size = 10000

    def __init__(self,
                 start_year: int = None,
                 end_year: int = None,
                 **kwargs):
        """
        In addition to existing parent's parameters, this defines a year.
        :param start_year: int: start year to load. 1929 if not specified.
        :param end_year: int: end year to load.
        :param kwargs: parent's parameters
        """
        super().__init__(**kwargs)
        self.start_year = start_year
        self.end_year = end_year

    def get_sources(self) -> NoReturn:
        """
        This method fills in self.source with the list of source files to load.
        We get the list of existing files from the website.
        We load the current-year file.
        :return: NoReturn
        """
        logger.info('Defining list of files to load.')
        # Loading current year data if not in full_load, and previous years data if in full_load
        if not self.full_load:
            start = self.start_year if self.start_year else (self.current_year - 1)
            end = (self.end_year if self.end_year else self.current_year) + 1
        else:
            start = self.default_start_year
            end = self.current_year + 1

        logger.debug(f'Start year: {start}')

        for file_type in self.file_types:
            logger.debug(f'Generating source files for {file_type}')
            source_prefix = f'{file_type}{self.source_pattern}'
            file_desc = 'Futures-and-Options Combined' if file_type == 'com' else 'Futures Only'

            for year in reversed(range(start, end)):
                logger.debug(f"Creating source for {file_type} and {year}")
                file_stem = f'{source_prefix}{year}'
                file_name = f'{file_stem}{self.base_file_ext}'

                source = BaseSource(url=f'{self.base_file_url}{file_name}',
                                    code=f'{self.provider_code.lower()}_{file_stem}',
                                    path=f'{self.provider_code.lower()}_{file_name}',
                                    long_name=f"CFTC Disagg. {file_desc} {year}")

                # append to self.sources
                self.sources.append(source)

            if self.full_load:
                logger.debug(f"Creating source for {file_type} and {self.hist_combined_years}")
                file_stem = f'{source_prefix}{self.hist_combined_years}'
                file_name = f'{file_stem}{self.base_file_ext}'
                # combined history file that cannot be automatically generated by the loop above
                source = BaseSource(url=f'{self.base_file_url}{file_name}',
                                    code=f'{self.provider_code.lower()}_{source_prefix}{self.hist_code}',
                                    path=f'{self.provider_code.lower()}_{file_name}',
                                    long_name=f"CFTC Disagg. {file_desc}"
                                              f"{self.hist_combined_years.replace('_', ' ')}")
                self.sources.append(source)

    @classmethod
    def _get_data_from_source(cls, source: BaseSource):
        """
        Loads one source file into a data frame.
        :param source: a BaseSource object describing the source file.
        :return: a Pandas DataFrame with the file content.
        """
        archive = FILE_STORE_PATH / source.path
        logger.debug(f'Reading {archive}')
        # relying on pandas' ability to automatically read compressed files
        df = pd.read_csv(archive,
                         skip_blank_lines=True,
                         names=cls.csv_columns.keys(),
                         dtype=cls.csv_columns,
                         na_values=cls.null_values,
                         header=0)
        logger.debug(f'{archive}: {len(df)} rows loaded.')

        # adds a column with the source code for allowing us to trace each row to its source
        df['source'] = source.code
        return df

    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data
        :return: NoReturn
        """
        # First we load the files from website and the history files (if needed)
        logger.debug(f"Reading {len(self.sources)} files in parallel...")
        dfs = parallelize(self._get_data_from_source, self.sources, self.max_worker)
        df = None
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                # add provider code
                df['provider'] = self.provider_code
                # add week date as datetime type
                df[self.week_date_column] = pd.to_datetime(df['As_of_Date_In_Form_YYMMDD'], format='%y%m%d')
                logger.debug(f"Loading {len(df)} rows to self.data")
                self.data = df

            except ValueError as e:
                logger.exception(f"Error while concatenating data frames, not transforming data")
        else:
            logger.warning(f'No data read from file list: {self.sources}.')

    def upsert(self) -> NoReturn:
        """
        Overrides parent method to bypass API and write results directly to specific table.
        if self.full_load = True, it removes all rows from this provider and loads the content of self.data into it,
        It loads self.data into a temporary table and runs a merge with final table otherwise.
        :return: NoReturn
        """
        logger.info("Writing to database.")
        if self.data is None or len(self.data) == 0:
            logger.info("No data to load into the database.")
            return

        logger.debug(f'Database: {EXT_DB_STR}')
        engine = create_engine(EXT_DB_STR, fast_executemany=True)
        # engine.begin() starts a transaction
        with engine.begin() as con:
            if self.full_load:
                logger.debug(f'Cleaning table {self.db_schema}.{self.db_final_table} for provider {self.provider_code}')
                # logger.debug('Sending truncate table statement')
                # con.execute(f"IF OBJECT_ID('{self.db_schema}.{self.db_final_table}','U') is not null "
                #            f"TRUNCATE TABLE {self.db_schema}.{self.db_final_table}")
                cleanup_statement = f"IF OBJECT_ID('{self.db_schema}.{self.db_final_table}','U') is not null " \
                                    f"DELETE FROM {self.db_schema}.{self.db_final_table} " \
                                    f"WHERE [provider] = '{self.provider_code}'"
                con.execute(cleanup_statement)

                logger.info(f'Loading {len(self.data)} rows to database.')
                self.data['date_created'] = datetime.datetime.now()
                self.data.to_sql(self.db_final_table,
                                 con=con,
                                 schema=self.db_schema,
                                 index=False,
                                 if_exists='append',
                                 chunksize=self.chunk_size
                                 )
            else:
                logger.debug(f'Loading {len(self.data)} rows to temporary table {self.db_temp_table}')
                self.data.to_sql(self.db_temp_table,
                                 con=con,
                                 schema=self.db_schema,
                                 index=False,
                                 if_exists='append',
                                 chunksize=self.chunk_size
                                 )

                logger.debug(f'Merging data from {self.db_temp_table} into {self.db_final_table}')
                merge_query = self.build_merge_query()
                con.execute(merge_query)
                logger.info(f'Merge from {self.db_temp_table} into {self.db_final_table} finished successfully.')

        # new transaction for index reorganisation
        with engine.begin() as con:
            cci_index: str = f'cci_{self.db_final_table}'
            logger.info(f'Reorganising clustered columnstore index: {cci_index}')
            reorg_cci_query = f'ALTER INDEX {cci_index} ON {self.db_schema}.{self.db_final_table} REORGANIZE;'
            con.execute(reorg_cci_query)
            logger.info(f'Clustered columnstore index {cci_index} reorganised successfully.')
