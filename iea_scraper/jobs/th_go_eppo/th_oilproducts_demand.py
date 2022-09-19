import logging
from copy import copy
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)

PROVIDER = 'TH_GO_EPPO'
BASE_URL = "http://www.eppo.go.th/index.php/en/en-energystatistics/petroleum-statistic"
FREQUENCY = "Monthly"
JOB_CODE = Path(__file__).parent.parts[-1]
AREA = "THAILAND"
FLOW = "DEMAND"
PRODUCT_MAPPING = {"MOGAS": "MOTORGAS",
                   "DIESEL OIL": "DIESEL",
                   "OTHERS": "OTHERPRODS"}
UNIT = 'KBD'
FILE = 'th_go_eppo_T02_04_01.xls'

URL = 'http://www.eppo.go.th/epposite/images/Energy-Statistics/' \
      'energyinformation/Energy_Statistics/Petroleum/T02_04_01.xls'


class ThOilproductsDemandJob(ExtDbApiJob):
    """
    This scraper will extract monthly oil products demand data
    from the thailand Ministry of Energy webpage
    """
    title: str = "Thailand - Demand of Oil Products"

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        :return:None
        """

        source = BaseSource(url=URL,
                            code=FILE.split(".")[0].upper(),
                            path=FILE,
                            long_name=f"{AREA}"
                                      f" Monthly Oil Products Demand")

        self.sources.append(source)

        # add dictionary to dynamic dims
        for source in self.sources:
            dicto = vars(copy(source))
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Transforming provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = "THAILAND Energy Policy and Planning Office"
        provider["url"] = "http://www.eppo.go.th/index.php/en/en-energystatistics/petroleum-statistic"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    @staticmethod
    def __get_data_from_source(file: Path) -> pd.DataFrame:
        """
        Transform each downloaded source file.
        :param file: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """

        logger.info(f"Opening {file}")
        # read the excel sheet and convert it to data frame
        df = pd.read_excel(file, header=[0, 1, 2], skiprows=3)
        logger.debug('Renaming the MONTH column into period column.')
        df = df.rename(columns={"MONTH": "period"})

        return df

    @staticmethod
    def __split_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        a function to split and parse data frame.
        It returns a data frame containing monthly oil products demand.
        :param df: data frame containing the whole data from excel sheet.
        :return: df without NA values on the period column.
        """
        logger.debug('Collecting indices corresponding to NAN values on first column')
        break_index = df.loc[df.iloc[:, 0].isna()].index
        table_list = []
        start_index = 0
        end_index = 0
        for index in break_index:
            logger.debug(f'beginning of the loop: {index}, {start_index}, {end_index}')
            if start_index == index:
                start_index = index + 1
                logger.debug('continue!')
                continue
            logger.debug('define end_index...')
            end_index = (index - 1)
            table_list.append(df.loc[start_index:end_index].copy())
            logger.debug(start_index, end_index)
            start_index = index + 1
        logger.debug(f'Dataframe split in {len(table_list)}.')
        logger.debug('Splitting the dataframe on spaces on first column and keeping the '
                     'part corresponding to the second index ')
        df = table_list[0]
        return df

    @staticmethod
    def __filter_and_map_monthly_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        This method filters monthly data from 2020
        :param df: the dataframe obtained from the list of dataframes table_list.
        :return: a more refined dataframe.
        """

        logger.debug('Splitting dataframe by getting rid of the YTD row')
        df = df[~df.iloc[:, 0].str.contains('YTD')]

        year = df.columns[0][0]
        if not year.isnumeric():
            raise ValueError(f'Year read from excel file is not numeric: {year}')
        logger.debug(f'Processing {year}')

        # To turn off SettingWithCopyWarning for a single dataframe
        df = df.copy()

        # dropping the first header
        df.columns = df.columns.droplevel(0)

        # Concatenating the file year to the period column.
        df.loc[:, ('Unnamed: 0_level_1', 'period')] = df.loc[:, ('Unnamed: 0_level_1',
                                                                 'period')].str.strip().str.upper() + year

        return df

    @staticmethod
    def __merge_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        This function slices the dataframe into two and then merges the two slices back together
        :param df: the dataframe derived from the above method
        :return: a dataframe
        """
        df = pd.merge(df.loc[:, df.columns[0]],
                      df.loc[:, df.columns[9:12]],
                      left_index=True, right_index=True, how="outer")

        return df

    def transform(self):
        """
         This function reads data from each data source in self.sources and transforms it into a dataframe
        :return:  a Pandas DataFrame.
        """
        logger.info("Transforming data...")
        self.__transform_provider()
        self.data = []

        for source in self.sources:
            logger.debug(f'processing {source.path}')
            file_path = FILE_STORE_PATH / source.path

            df = self.__get_data_from_source(file_path)
            df = self.__merge_df(self.__filter_and_map_monthly_data(self.__split_df(df)))

            # Drop the first header
            df.columns = df.columns.droplevel(0)

            # melt fields
            df = pd.melt(df, id_vars=['period'], var_name='product')

            # remove null values
            df.dropna(subset=['value'], inplace=True)
            logger.debug(f'Number of rows after removing nulls: {len(df)}')

            # Converting into KBD
            df['value'] = df['value'] / 1000

            # Mapping our three products to their equivalent in the External Db in the product dimension section
            df['product'] = df['product'].map(PRODUCT_MAPPING)

            df = (df.assign(provider=PROVIDER,
                            source=source.code,
                            area=AREA,
                            frequency=FREQUENCY,
                            flow=FLOW,
                            unit=UNIT,
                            original=True))
            # Loading the results into self.data
            self.data.extend(df.to_dict('records'))
            logger.debug("data transformation complete.")
