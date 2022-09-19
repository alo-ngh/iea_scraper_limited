import pandas as pd
import datetime
from copy import copy
from pathlib import Path

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH
import logging

logger = logging.getLogger(__name__)


class IndianOilDeliveriesJob(ExtDbApiJob):
    """
    Indian Oil Deliveries - Monthly Data

    Provider: Petroleum Planning and Analysis Cell (www.ppac.gov.in)
    """

    title: str = "Indian Oil Deliveries - Monthly Data"
        
    skip_rows = 7

    query = 'SELECT * FROM main.indian_oil_deliveries'

    table_name = 'indian_oil_deliveries'

    area = 'INDIA'
    frequency = 'Monthly'
    flow = 'DEMAND'
    unit = 'KT'
    original = True

    provider = {'code': Path(__file__).parent.parts[-1].upper(),
                'long_name': f'{area} - Petroleum Planning & Analysis Cell',
                'url': 'https://www.ppac.gov.in'}

    file_prefix = f"{provider['code']}_cons"

    base_url = 'https://www.ppac.gov.in/WriteReadData/userfiles/file'
    source_files = {'Current': 'PT_consumption.xls',
                    'Historical': 'PT_Consumption_H.xls'}

    # file containing product mappings (it's in a file to make it easier to change if needed)
    mapping_path = Path(__file__).parent
    mapping_file = mapping_path / 'IN_GOV_PACC_mappings.xlsx'

    hist_sheets_to_ignore = ['Historical (year-wise)', '2010-11Rev']

    def get_sources(self):
        """
        This method defines all information about the source files to be processed.
        In this scraper, for simplicity, we ignore full_load flag, as in the website,
        there is only one file, not a history.

        :return: NoReturn
        """
        logger.debug('Defining sources')
        # BaseSource object contains all the information needed to process a source file.
        #
        # It's also used to fill in dimension.source table in the star schema.
        if self.full_load:
            self.sources.append(self.get_base_source('Historical'))
        self.sources.append(self.get_base_source('Current'))

        # self.sources is a list of all files to process in the scraper
        # The parent class will use this information to download these files to the filestore
        # and process them only if they have changed from last download.

        logger.info(f'{len(self.sources)} source files to load.')
        # This add the list of sources as values to dimension 'source'.
        # The parent class will process self.dynamic_dim and insert all entries to the respective dimension.
        # TODO: the following code should be done in parent class as we do it every time...
        self.dynamic_dim['source'] += [vars(copy(source)) for source in self.sources + self.source_complements]
        # remove sources already existing in the dimension (ugly, but the API does not do upsert for dimensions)
        self.remove_existing_dynamic_dim('source')

    def get_base_source(self, file_type):
        """
        Helper function to generate a BaseSource object.
        :param file_type: a string describing the file type (Current, Historical)
        :return: a BaseSource object representing this source.
        """
        code = f"{self.provider['code']}_cons_{file_type}"

        return BaseSource(url=f'{self.base_url}/{self.source_files[file_type]}',
                          code=f'{code}',
                          path=f'{code}.xls',
                          long_name=f"{self.area} {self.provider['code']} {file_type} {self.title}")

    def transform(self):
        """
        Method to transform the data frame.
        The result must be written to self.data list as one dict per row.

        :return: NoReturn
        """
        logger.debug('transforming data')

        # load provider
        self.__transform_provider()

        try:
            df = pd.concat([self.read_dataframe(source) for source in self.sources]) \
                   .pipe(self.transform_dataframe) \
                   .pipe(self.prepare_extdb)

            # load results into self.data
            self.data = df.to_dict('records')

        except ValueError as e:
            logger.info('No data to process.')
    
    def read_dataframe(self, source: BaseSource) -> pd.DataFrame:
        """
        Reads the data from one given source into a file.
        It reads the data from the filestore.
        :param source: BaseSource: object describing the source file.
        :return: df: pd.DataFrame: dataframe with the data.
        """
        file_path = FILE_STORE_PATH / source.path
        if 'Current' in source.code:
            logger.debug('Processing Current file')
            df = pd.read_excel(file_path, engine='xlrd').pipe(self.pre_process_df)
        else:
            logger.debug('Processing Historical file')
            # old excel files use xlrd engine
            xl = pd.ExcelFile(file_path, engine='xlrd')
            sheets_to_process = [sheet for sheet in xl.sheet_names if sheet not in self.hist_sheets_to_ignore]
            logger.debug(f"Sheets to process: {', '.join(sheets_to_process)}")
            df = pd.concat([pd.read_excel(xl, sheet_name=sheet)
                              .pipe(self.pre_process_df) for sheet in sheets_to_process])
        # adding an external db column earlier (only place we have access to it)
        df['source'] = source.code
        logger.info(f'{len(df)} rows read from {source.path}')
        return df

    def pre_process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pre-process the dataframe: calculate correct periods, melt, remove N/As.
        :param df: pd.DataFrame: the raw data frame
        :return: pd.DataFrame: resulting dataframe
        """
        logger.info('Preprocessing dataframe')
        periods = self.get_period_from_df(df)
        df = self.slice_data(df)
        df = self.rename_columns(df, periods)

        logger.debug('Melting and removing nulls')
        df = df.melt(id_vars=['PRODUCTS'], var_name='period').dropna(subset=['value'])
        logger.info(f'{len(df)} rows after preprocessing')

        return df

    def get_period_from_df(self, df: pd.DataFrame) -> pd.DatetimeIndex:
        """
        Calculating the correct periods as datetimes from line 5: 'Period: Apr <start_year>-Mar <end_year>'
        :param df: pd.DataFrame: the raw data frame
        :return: pd.DatetimeIndex: an index containing the period range
        """
        logger.info('Calculating the period')
        # find the period:
        row_with_period = df[df.iloc[:, 0].notna() & df.iloc[:, 0].str.startswith('Period')]
        # period text is in the first column
        period_text = row_with_period.iloc[:, 0].values[0]
        logger.info(f'Period: {period_text}')
        # take 'Period:' out and split by - to get date range
        # for 2014-15, we remove ')' in the end
        str_start, str_end = period_text.replace(')', '').split(':')[1].split('-')
        return pd.date_range(start=str_start, end=str_end, freq='MS')

    def slice_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Slice the data frame to return only data of interest.
        :param df: raw data frame.
        :return: pd.DataFrame: sliced data frame.
        """
        logger.debug('Slicing dataframe')
        idx_start = df.loc[df.iloc[:, 0] == 'PRODUCTS'].index.values[0]
        # we do upper() to avoid mismatch for some years
        idx_end = df[(df.iloc[:, 0] == 'TOTAL')|(df.iloc[:, 0].str.upper() == 'ALL PRODUCTS TOTAL')].index.values[0]
        logger.debug(f'Keeping rows between {idx_start} and {idx_end}')
        sliced_df = df.iloc[idx_start + 1: idx_end]

        logger.debug('Removing total column')
        sliced_df = sliced_df.iloc[:, :-1]
        return sliced_df

    def rename_columns(self, df: pd.DataFrame, periods: pd.DatetimeIndex) -> pd.DataFrame:
        """
        Renaming columns to have dates as correct timestamps.
        :param df: pd.DataFrame: source dataframe.
        :param periods: pd.DatetimeIndex: index with the list of periods
        :return: pd.DataFrame: dataframe with renamed columns
        """
        column_names = ['PRODUCTS']
        column_names.extend(periods.values)
        logger.debug(f'Setting column names with correct periods {column_names}')
        df.columns = column_names
        return df

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the data from the source and prepare to load.
        :param df: pd.DataFrame: data to process
        :return: df: pd.DataFrame
        """
        # Nothing to be done here for now
        return df

    def map_products_to_extdb(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Auxiliary function to map products from source to external DB ones.
        Mappings are load from file self.mapping_file, column 'products'.
        :param df: pd.DataFrame: data frame to adapt.
        :return: pd.DataFrame: resulting data frame.
        """
        logger.info('Mapping products to External DB values.')
        logger.debug(f'Loading mappings from {self.mapping_file}, sheet=products')
        df_map = pd.read_excel(self.mapping_file, engine='openpyxl', sheet_name='products')
        logger.debug(f'{len(df_map)} rows read from {self.mapping_file}')
        df = df.merge(df_map, how='left', left_on='PRODUCTS', right_on='ppac_label', indicator=True)

        check_df = df[df['_merge'] == 'left_only']

        if len(check_df) > 0:
            products = df['PRODUCTS'].drop_duplicates().tolist()
            raise ValueError(f"Product values not mapped: {', '.join(products)}")

        df.drop(columns=['PRODUCTS', 'ppac_label', 'Source name', 'Comments', '_merge'], inplace=True)
        df.rename(columns={'extdb_code': 'product'}, inplace=True)
        return df

    def prepare_extdb(self, df) -> pd.DataFrame:
        """
        This adds all columns needed for external DB schema.

        :param df: pd.DataFrame: dataframe to fit in external db model.
        :return: pd.DataFrame
        """
        logger.debug('Adapting dataframe to External DB schema')
        df['period'] = df['period'].dt.strftime("%b%Y").str.upper()
        df = self.map_products_to_extdb(df)
        return df.assign(provider=self.provider['code'],
                         area=self.area,
                         frequency=self.frequency,
                         flow=self.flow,
                         unit=self.unit,
                         original=self.original)

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: NoReturn
        """
        logger.debug("Transforming provider ...")
        logger.debug(f"Adding provider to dynamic_dim: {self.provider['code']}")
        self.dynamic_dim['provider'] = [self.provider]
        self.remove_existing_dynamic_dim('provider')