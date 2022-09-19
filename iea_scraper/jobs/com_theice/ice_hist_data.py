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


class IceHistDataJob(ExtDbApiDedicatedTableJob):
    """
    Scraper for Commitment of Traders ICE Futures Europe released on a weekly basis.

    Source: https://www.theice.com/marketdata/reports/122

    """
    title: str = 'Commitment of Traders ICE Futures Europe'

    online_sources_url = 'https://www.theice.com/marketdata/reports/122'
    year_pattern = '@YEAR@'
    base_file_url = f'https://www.theice.com/publicdocs/futures/COTHist{year_pattern}.csv'

    job_code = Path(__file__).parent.parts[-1]
    provider_code: str = job_code.upper()
    provider_long_name: str = 'ICE - Intercontinental Exchange, Inc.'
    provider_url: str = 'https://www.theice.com'

    source_prefix = 'COTHist'

    # start_year
    default_start_year = 2011

    # current year
    current_year = date.today().year

    # maximum number of workers in parallel processing
    max_worker = 3

    # consider characters below as null values
    null_values = [' ',  # this appears in 2018 file
                   '#VALUE!'  # this one appears in 2017 file
                   ]

    # column headers for each source file
    csv_columns = {'Market_and_Exchange_Names': str,
                   'As_of_Date_In_Form_YYMMDD': str,
                   'As_of_Date_Form_MM/DD/YYYY': str,
                   'CFTC_Contract_Market_Code': str,
                   'CFTC_Market_Code': str,
                   'CFTC_Region_Code': 'Int64',
                   'CFTC_Commodity_Code': str,
                   'Open_Interest_All': float,
                   'Prod_Merc_Positions_Long_All': 'Int64',
                   'Prod_Merc_Positions_Short_All': 'Int64',
                   'Swap_Positions_Long_All': 'Int64',
                   'Swap_Positions_Short_All': 'Int64',
                   'Swap_Positions_Spread_All': 'Int64',
                   'M_Money_Positions_Long_All': 'Int64',
                   'M_Money_Positions_Short_All': 'Int64',
                   'M_Money_Positions_Spread_All': 'Int64',
                   'Other_Rept_Positions_Long_All': 'Int64',
                   'Other_Rept_Positions_Short_All': 'Int64',
                   'Other_Rept_Positions_Spread_All': 'Int64',
                   'Tot_Rept_Positions_Long_All': float,
                   'Tot_Rept_Positions_Short_All': float,
                   'NonRept_Positions_Long_All': 'Int64',
                   'NonRept_Positions_Short_All': 'Int64',
                   'Open_Interest_Old': float,
                   'Prod_Merc_Positions_Long_Old': 'Int64',
                   'Prod_Merc_Positions_Short_Old': 'Int64',
                   'Swap_Positions_Long_Old': 'Int64',
                   'Swap_Positions_Short_Old': 'Int64',
                   'Swap_Positions_Spread_Old': 'Int64',
                   'M_Money_Positions_Long_Old': 'Int64',
                   'M_Money_Positions_Short_Old': 'Int64',
                   'M_Money_Positions_Spread_Old': 'Int64',
                   'Other_Rept_Positions_Long_Old': 'Int64',
                   'Other_Rept_Positions_Short_Old': 'Int64',
                   'Other_Rept_Positions_Spread_Old': 'Int64',
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
                   'Traders_Tot_All': 'Int64',
                   'Traders_Prod_Merc_Long_All': 'Int64',
                   'Traders_Prod_Merc_Short_All': 'Int64',
                   'Traders_Swap_Long_All': 'Int64',
                   'Traders_Swap_Short_All': 'Int64',
                   'Traders_Swap_Spread_All': 'Int64',
                   'Traders_M_Money_Long_All': 'Int64',
                   'Traders_M_Money_Short_All': 'Int64',
                   'Traders_M_Money_Spread_All': 'Int64',
                   'Traders_Other_Rept_Long_All': 'Int64',
                   'Traders_Other_Rept_Short_All': 'Int64',
                   'Traders_Other_Rept_Spread_All': 'Int64',
                   'Traders_Tot_Rept_Long_All': float,
                   'Traders_Tot_Rept_Short_All': float,
                   'Traders_Tot_Old': 'Int64',
                   'Traders_Prod_Merc_Long_Old': 'Int64',
                   'Traders_Prod_Merc_Short_Old': 'Int64',
                   'Traders_Swap_Long_Old': 'Int64',
                   'Traders_Swap_Short_Old': 'Int64',
                   'Traders_Swap_Spread_Old': 'Int64',
                   'Traders_M_Money_Long_Old': 'Int64',
                   'Traders_M_Money_Short_Old': 'Int64',
                   'Traders_M_Money_Spread_Old': 'Int64',
                   'Traders_Other_Rept_Long_Old': 'Int64',
                   'Traders_Other_Rept_Short_Old': 'Int64',
                   'Traders_Other_Rept_Spread_Old': 'Int64',
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

    # key columns
    week_date_column = 'Week_of_Release'
    key_columns = ['provider', 'Week_of_Release', 'CFTC_Contract_Market_Code',
                   'CFTC_Market_Code', 'CFTC_Region_Code', 'CFTC_Commodity_Code', 'FutOnly_or_Combined']

    # DB schema
    db_schema = 'main'
    db_table_prefix = 'futures_options'

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

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        """
        logger.info('Defining list of files to load.')
        if not self.full_load:
            start = self.start_year if self.start_year else (self.current_year - 1)
            end = (self.end_year if self.end_year else self.current_year) + 1
        else:
            start = self.default_start_year
            end = self.current_year + 1

        data_list = [BaseSource(url=self.base_file_url.replace(self.year_pattern, str(year)),
                                code=f'{self.provider_code.lower()}_{self.source_prefix}{year}',
                                path=f'{self.provider_code.lower()}_{self.source_prefix}{year}.csv',
                                long_name=f"ICE HISTORICAL DATA FILE {year}")
                     for year in range(start, end)]

        self.sources.extend(data_list)

    @classmethod
    def _get_data_from_source(cls, source: BaseSource):
        """
        Loads one source file into a data frame.
        :param source: a BaseSource object describing the source file.
        :return: a Pandas DataFrame with the file content.
        """
        file = FILE_STORE_PATH / source.path
        try:
            df = cls._read_and_clean(file)
            # adds a column with the source code for allowing us to trace each row to its source
            df['source'] = source.code
            return df
        except Exception as e:
            logger.exception(f'Error while reading {source.path}')
            raise e

    @classmethod
    def _get_file_reader(cls, file: Path):
        """
        Verify if there is a special reader for this file.
        Returns a default reader in case there is no special reader.
        :return: a function that reads this file.
        """
        file_reader_mapping = {
            '2018': cls.read_2018
        }

        year = file.stem[-4:]
        logger.debug(f'Year in file: {year}')

        return file_reader_mapping.get(year, cls.default_reader)

    @classmethod
    def _read_and_clean(cls, file):
        """
        Auxiliary function to cope with changes in format and content of files
        throughout the years.

        Currently, for 2018, it manages the difference in column names and the additional column.

        :param file: str: file name.
        :return: pd.DataFrame: data frame with the file content
        """
        logger.debug(f'Reading {file}')

        file_reader = cls._get_file_reader(file)
        df = file_reader(file)

        df.dropna(how='all', inplace=True)
        logger.info(f'{len(df)} rows after removing null rows.')

        logger.debug(f'{file}: {len(df)} rows loaded.')
        return df

    @classmethod
    def default_reader(cls, file):
        """
        Default reader for CSV files.
        :param file: filename
        :return: pd.DataFrame: data frame with file data.
        """
        df = pd.read_csv(file,
                         skip_blank_lines=True,
                         names=cls.csv_columns.keys(),
                         dtype=cls.csv_columns,
                         na_values=cls.null_values,
                         header=0)
        logger.info(f'{len(df)} rows loaded from {file}.')
        return df

    @classmethod
    def read_2018(cls, file):
        """
        Read and clean 2018 file: deal with additional column.
        :param file: filename
        :return: pd.DataFrame: data frame with file data.
        """
        logger.debug(f'Reading and cleaning 2018 file: {file}')

        # add additional column to the end
        csv18_columns = copy(cls.csv_columns)
        csv18_columns['Unnamed: 191'] = str

        df = pd.read_csv(file,
                         skip_blank_lines=True,
                         na_values=cls.null_values,
                         names=csv18_columns.keys(),
                         dtype=csv18_columns,
                         index_col=False,
                         header=0
                         )

        logger.debug('Year 2018: removing additional last column')
        df = df[df.columns[:-1]]

        return df

    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data
        :return: NoReturn
        """
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

                # replace nulls by a text as these columns cannot be null
                df['CFTC_Contract_Market_Code'].fillna('n/a', inplace=True)

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
