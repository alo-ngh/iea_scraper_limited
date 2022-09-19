import datetime
import logging

import pandas as pd

from iea_scraper.core.job import ExtDbApiJobV2
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class CrudeOilExportsByRailJob(ExtDbApiJobV2):
    """
    Canadian Crude Oil Exports by Rail - Monthly Data

    Provider: Canada Energy Regulator (www.cer-rec.gc.ca)
    """

    title: str = "Canadian Crude Oil Exports by Rail - Monthly Data"

    skip_rows = 7

    start_year = 2012

    # code column in dimension.source is limited to 30 characters
    file_code = 'ca_crude_rail_exports_monthly'

    provider_code = 'ca_gc_cer_rec'.upper()
    provider_long_name = 'Canada Energy Regulator'
    provider_url = 'http://www.cer-rec.gc.ca'

    link = f'{provider_url}' \
           '/en/data-analysis/energy-commodities/crude-oil-petroleum-products/statistics' \
           '/canadian-crude-oil-exports-rail-monthly-data.xlsx'

    area = 'CANADA'
    frequency = 'Monthly'
    flow = 'EXPORTS'
    product = 'CRUDEOIL'
    unit = 'KBD'
    original = True

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
        source: BaseSource = BaseSource(code=self.file_code.upper(),
                                        url=self.link,
                                        path=f'{self.file_code}.xlsx',
                                        long_name=f"{self.provider_long_name} - "
                                                  f"{self.title}")

        # self.sources is a list of all files to process in the scraper
        # The parent class will use this information to download these files to the filestore
        # and process them only if they have changed from last download.
        self.sources.append(source)
        logger.info(f'{len(self.sources)} source files to load.')

    def transform(self):
        """
        Method to transform the data frame.
        The result must be written to self.data list as one dict per row.

        :return: NoReturn
        """
        logger.debug('transforming data')
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
        df = pd.read_excel(file_path, skiprows=self.skip_rows, engine='openpyxl')
        logger.info(f'{len(df)} rows read from {source.path}')
        return df

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the data from the source and prepare to load.
        :param df: pd.DataFrame: data to process
        :return: df: pd.DataFrame
        """
        logger.debug("Transforming dataframe...")
        logger.debug("Keeping only data rows...")
        df.dropna(subset=['Month'], axis=0, inplace=True)
        df.dropna(how='all', axis=1, inplace=True)
        # TODO: replace date calculation by fillna(method='ffil') on year
        logger.debug("Expanding years to cover empty cells...")
        start = datetime.date(self.start_year, 1, 1)
        dates = pd.date_range(start=start, periods=len(df), freq='M')[::-1]
        df.index = dates
        df.index.rename('date', inplace=True)
        df.sort_index(ascending=True, inplace=True)
        df.drop(['Year', 'Month'], axis=1)
        logger.debug('Keeping only last column (Volume bbl per day)')
        df = df.iloc[:, -1:]
        df.columns = ['value']
        logger.debug('converting bbl to kbd')
        df = df / 1000
        df.reset_index(inplace=True)
        return df

    def prepare_extdb(self, df) -> pd.DataFrame:
        """
        This adds all columns needed for external DB schema.

        :param df: pd.DataFrame: dataframe to fit in external db model.
        :return: pd.DataFrame
        """
        logger.debug('Adapting dataframe to External DB schema')
        df['period'] = df['date'].dt.strftime("%b%Y").str.upper()
        df.drop(columns='date', inplace=True)
        return df.assign(provider=self.provider_code,
                         source=self.file_code.upper(),
                         area=self.area,
                         frequency=self.frequency,
                         flow=self.flow,
                         product=self.product,
                         unit=self.unit,
                         original=self.original)
