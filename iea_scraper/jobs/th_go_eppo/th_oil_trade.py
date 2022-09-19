import logging
from copy import copy
from datetime import date
from pathlib import Path
from typing import List
from dateutil.relativedelta import relativedelta

import pandas as pd

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)

PROVIDER = 'TH_GO_EPPO'
BASE_URL = 'http://www.eppo.go.th/epposite/images/Energy-Statistics/' \
           'energyinformation/Energy_Statistics/Petroleum'
FREQUENCY = "Monthly"
JOB_CODE = Path(__file__).parent.parts[-1]
AREA = "THAILAND"
PRODUCT = "CRUDEOIL"
UNIT = 'KBD'

FILES = {
    'T02_01_04.xls': 'Imports',
    'T02_01_05.xls': 'Exports'
}

# Publication delay in months (currently 2: in Jan 2021, they publish data for Nov 2020)
PUBLICATION_DELAY = 2


class ThOilTradeJob(ExtDbApiJob):
    """
    This scraper will extract monthly oil trade data from the thailand Ministry of Energy webpage
    """
    title: str = "Thailand - Quantity and Value of Petroleum Trade"

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        :return:None
        """

        sources = [BaseSource(url=f'{BASE_URL}/{file}',
                              code=f'{PROVIDER}_{file.split(".")[0].upper()}',
                              path=f'{PROVIDER.lower()}_{file}',
                              long_name=f"{AREA} Monthly Crude Oil {FILES[file]}") for file in FILES.keys()]

        self.sources.extend(sources)

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
        df = pd.read_excel(file, header=[0, 1], skiprows=3)
        logger.debug(f"{len(df)} rows read from file {file}")
        return df

    @staticmethod
    def __split_df(df: pd.DataFrame) -> List[pd.DataFrame]:
        """
        Helper function to split and parse data frame.
        It returns a data frame containing only CRUDEOIL trade.
        :param df: data frame containing the whole data from excel sheet.
        :return: df with only CRUEDOIL imports info.
        """
        logger.debug('Splitting data frame on spaces on first column')
        break_index = df.loc[df.iloc[:, 0].isna()].index
        table_list = []
        start_index = 1
        end_index = 1
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

        return table_list

    @staticmethod
    def __filter_crudeoil_trade(table_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        From table_list, which is a list of dataframes, select only
        the first dataframe corresponding to the index 0 (Crude oil) and then filter
        only the row corresponding to volume.
        :param table_list: a list of data frames.
        :return: pd.DataFrame with the results.
        """
        logger.debug('Filtering crudeoil export volumes data.')
        df = table_list[0]
        # filtering out the new dataframe by keeping only the BBL/D row
        df = df[df['TYPE', 'Unnamed: 0_level_1'].str.contains('BBL/D')]
        return df

    @staticmethod
    def __melt(df, year):
        """
        This function melts the data frame putting months from columns to rows.
        :param df: the dataframe we want to melt
        :param year: the year you would like to display on your dataframe
        :return: a melted dataframe
        """
        logger.debug('melting the dataframe ...')
        df_melted = df.melt(value_vars=df.columns,
                            var_name="month",
                            value_name="value")
        df_melted['period'] = (df_melted['month'] + f'{str(year)}').str.upper()

        del df_melted['month']
        logger.debug(f'Number of rows in df for {year}: {len(df)}')
        return df_melted

    @classmethod
    def __filter_and_melt_monthly_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        This method filters only monthly data from current and last years and melt it to have month as rows.
        :param df: input data frame with multi-index columns having year in level 0 and month in level 1.
        :return: a data frame with columns period and value.
        """
        # multi-index file: first-level contains year
        # we seek current and last year monthly data
        current_year = (date.today().replace(day=1) - relativedelta(months=PUBLICATION_DELAY)).year
        current_year_data = df[str(current_year)]

        last_year = current_year - 1
        last_year_data = df[str(last_year)]

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        common_cols = list(set(months).intersection(last_year_data.columns.tolist()))

        if not common_cols:
            logger.debug(f"{common_cols} is an empty list")
            last_year_data = pd.DataFrame()
        else:
            last_year_data = last_year_data[common_cols]
            last_year_data = cls.__melt(last_year_data, last_year)

        current_year_data = cls.__melt(current_year_data, current_year)

        return pd.concat([last_year_data, current_year_data])

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
            df = self.__filter_crudeoil_trade(self.__split_df(df))
            df = self.__filter_and_melt_monthly_data(df)

            # convert to KBD
            df['value'] = (df.loc[:, 'value'] / 1000)

            # we derive flow from source.long_name
            df = (df.assign(provider=PROVIDER,
                            source=source.code,
                            area=AREA,
                            frequency=FREQUENCY,
                            flow=source.long_name.split(' ')[-1].upper(),
                            product=PRODUCT,
                            unit=UNIT,
                            original=True))
            # Loading the results into self.data
            self.data.extend(df.to_dict('records'))
            logger.debug("data transformation complete.")
