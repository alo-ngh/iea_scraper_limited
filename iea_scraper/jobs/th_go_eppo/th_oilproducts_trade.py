import logging
from copy import copy
from datetime import date
from pathlib import Path
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

PRODUCT_MAPPING = {""
                   "GASOLINE": "MOTORGAS",
                   "KEROSENE": "JETKERO",
                   "J.P.": "OTHKERO",
                   "FUEL OIL": "RESFUEL",
                   "LPG": "LPG",
                   }

UNIT = 'KBD'

FILES = {
    'T02_03_02.xls': 'Supply',
    'T02_03_07.xls': 'Imports',
    'T02_03_09.xls': 'Exports'
}

# Publication delay in months (currently 2: in Jan 2021, they publish data for Nov 2020)
PUBLICATION_DELAY = 2


class ThOilproductsTradeJob(ExtDbApiJob):
    """
    This scraper will extract monthly petroleum products flows data from the thailand Ministry of Energy webpage
    """
    title: str = "Thailand - Production, Import and Export of Petroleum Products"

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
                              long_name=f"{AREA} Monthly {FILES[file]} of Petroleum Products")
                   for file in FILES.keys()]

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
    def __refine_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        This function drops the NA values on the first column, and then
        filters the dataframe by only keeping the required products
        :param df:  the dataframe returned by the above method
        :return: a refined pandas dataframe
        """
        df.dropna(subset=[('ENERGY', 'TYPE')], inplace=True)
        logger.debug(f"There is now only {len(df)} rows remaining from the previous dataframe")
        df = df[df.iloc[:, 0].str.contains('GASOLINE|KEROSENE|J.P|FUEL OIL|LPG')]
        logger.debug('Filtering out by only keeping the required rows')

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
        df_melted = df.melt(id_vars=['TYPE'],
                            var_name="month",
                            value_name="value")
        df_melted['period'] = (df_melted['month'] + f'{str(year)}').str.upper()

        del df_melted['month']

        return df_melted

    @classmethod
    def __filter_monthly_data(cls, df: pd.DataFrame) -> pd.DataFrame:
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
        last_year_data = last_year_data[common_cols]

        # dropping the first header
        df.columns = df.columns.droplevel(0)

        # Merging current_year_data and last_year_data with the The first column of our DataFrame
        current_year_data = pd.merge(df[['TYPE']],
                                     current_year_data,
                                     left_index=True,
                                     right_index=True,
                                     how="outer")

        last_year_data = pd.merge(df[['TYPE']],
                                  last_year_data,
                                  left_index=True,
                                  right_index=True,
                                  how="outer")

        current_year_data = cls.__melt(current_year_data, current_year)
        last_year_data = cls.__melt(last_year_data, last_year)

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
            df = self.__refine_df(df)
            df = self.__filter_monthly_data(df)

            # remove null values
            # df.dropna(subset=['value'], inplace=True)
            # logger.debug(f'Number of rows after removing nulls: {len(df)}')

            # convert to KBD
            df['value'] = (df.loc[:, 'value'] / 1000)

            logger.debug('Renaming the TYPE column into flow.')
            df = df.rename(columns={"TYPE": "product"})

            # Striping the product column of all the spaces
            df['product'] = df['product'].str.strip()

            # Mapping the product column to its equivalent in the External Db in the product dimension section
            df['product'] = df['product'].map(PRODUCT_MAPPING)

            # we derive flow from source.long_name
            df = (df.assign(provider=PROVIDER,
                            source=source.code,
                            area=AREA,
                            frequency=FREQUENCY,
                            flow=source.long_name.split(' ')[-4].upper(),
                            unit=UNIT,
                            original=True))
            # Loading the results into self.data
            self.data.extend(df.to_dict('records'))
            logger.debug("data transformation complete.")
