import logging
from copy import copy
from pathlib import Path
from typing import List

import pandas as pd

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)

PROVIDER = 'TH_GO_EPPO'
BASE_URL = "http://www.eppo.go.th/index.php/en/" \
           "en-energystatistics/petroleum-statistic"
FREQUENCY = "Monthly"
JOB_CODE = Path(__file__).parent.parts[-1]
AREA = "THAILAND"
FLOW_MAPPING = {"CONSUMPTION": "DEMAND"}
PRODUCT = "LPG"
UNIT = 'KTOE'
FILE = 'th_go_eppo_T02_04_02-1.xls'

URL = 'http://www.eppo.go.th/epposite/images/Energy-Statistics/' \
      'energyinformation/Energy_Statistics/Petroleum/T02_04_02-1.xls'


class ThLpgDemandJob(ExtDbApiJob):
    """
    This scraper will extract monthly LPG demand data
    from the thailand Ministry of Energy webpage.
    """
    title: str = "Thailand - LPG Demand data "

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
                                      f" Monthly LPG Demand Data")

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
        df = pd.read_excel(file, header=[1], skiprows=4)
        logger.debug(f"{len(df)} rows read from file {file}")
        df.iloc[:, 0] = df.iloc[:, 0].astype('str')
        logger.debug('Converting every row of the first column into string characters.')
        # remove column TOTAL
        del df['TOTAL']

        return df

    @staticmethod
    def __slice_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Collects two indices of pandas dataframe, then slices our dataframe on
        those indices.
        :param df: dataframe containing the entire dataset loaded from excel file.
        :return: a new dataframe with only the part we wish to work with.
        """
        logger.debug('Setting indices corresponding to the part of the dataframe we want to work on')
        first_index = df.loc[df.iloc[:, 0].str.contains('2019')].index
        last_index = df.loc[df.iloc[:, 0].str.contains('Source')].index

        # using indexing to collect the first and last index values, since they are of dtype 'int64'
        df = df.iloc[first_index[0]: last_index[0]].copy()

        # remove own index with default index
        df.reset_index(inplace=True, drop=True)

        return df

    @staticmethod
    def __split_df(df: pd.DataFrame) -> List[pd.DataFrame]:
        """
        a function to split and parse data frame.
        :param df: the dataframe sliced by the previous method.
        :return: a list containing two dataframes.
        """
        logger.debug('Collecting indices corresponding to the BALANCE row values on first column')
        break_index = df.loc[df.iloc[:, 0].str.contains('BALANCE')].index
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

        return table_list

    def __filter_and_concat_monthly_data(self, table_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        This method filters monthly data, formats the period and concatenate it into one dataframe.
        :param table_list: the list of dataframes returned by the above method __split_df.
        :return: a concatenated dataframe.
        """
        dfs = [self.__melt_map_period_and_filter(df) for df in table_list]
        return pd.concat(dfs)

    @staticmethod
    def __melt_map_period_and_filter(df: pd.DataFrame) -> pd.DataFrame:
        """
        This function melts the data frame, then maps the period column.
        The year is the first row of the first column. Then Concatenates it to period.
        Raise ValueError if year not numeric.
        :param df: the dataframe we want to melt
        :return: a melted dataframe
        """
        year = df.iloc[0, 0]
        df = df.iloc[1:, :]
        if not year.isnumeric():
            raise ValueError(f'Year read from excel file is not numeric: {year}')
        logger.debug(f'Processing {year}')

        logger.debug('melting the dataframe ...')
        df = pd.melt(df,
                     id_vars=["DESCRIPTION"],
                     var_name="period",
                     value_name="value")
        df['period'] = (df['period'] + f'{year}').str.upper()

        # remove null values
        df.dropna(subset=['value'], inplace=True)

        logger.debug('Splitting dataframe by getting rid of the any row containing the word TOTAL ')
        df = df[~df.iloc[:, 0].str.contains('TOTAL')]

        # To turn off SettingWithCopyWarning for a single dataframe
        df = df.copy()

        # Getting rid of any undesired characters on the first column
        df.iloc[:, 0] = df.iloc[:, 0].str.strip(' -')

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
            df = self.__slice_df(df)
            df = self.__filter_and_concat_monthly_data(self.__split_df(df))

            # filtering out the dataframe by keeping only the CONSUMPTION rows
            df = df[df['DESCRIPTION'].str.contains('CONSUMPTION')]

            logger.debug('Renaming the DESCRIPTION column into flow.')
            df = df.rename(columns={"DESCRIPTION": "flow"})

            # Mapping the flow column to its equivalent in the External Db in the flow dimension section
            df['flow'] = df['flow'].map(FLOW_MAPPING)

            df = (df.assign(provider=PROVIDER,
                            source=source.code,
                            area=AREA,
                            frequency=FREQUENCY,
                            product=PRODUCT,
                            unit=UNIT,
                            original=True))
            # Loading the results into self.data
            self.data.extend(df.to_dict('records'))
            logger.debug("data transformation complete.")
