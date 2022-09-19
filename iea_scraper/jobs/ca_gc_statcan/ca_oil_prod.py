import calendar
import logging
import zipfile
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiJobV2
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class CaOilProdJob(ExtDbApiJobV2):
    """
    Scraper for downloading Canadian statistics from statcan.
    Currently, 2 files are downloaded:

    - 100063-eng.zip: CRUDEOIL_MONTHLY
    - 25100036-eng.zip: NGL__MONTHLY

    Details about the time series collected can be found on self.details.
    """
    title: str = 'STATCAN - Canadian Oil Statistics'

    provider_code: str = 'STATCAN'
    provider_long_name: str = "Canada - Statistics Canada"
    provider_url: str = "https://www.statcan.gc.ca/"

    job_code = Path(__file__).parent.parts[-1]
    base_url = "https://www150.statcan.gc.ca/n1/tbl/csv/"
    files = [
        {'file': '25100063-eng.zip', 'name': 'CRUDEOIL_MONTHLY'},
        {'file': '25100036-eng.zip', 'name': 'NGL_MONTHLY'}
    ]
    area = 'CANADA'
    flow = 'SUPPLY'
    frequency = 'Monthly'

    original = False
    details = [
        {"code": "SC_CRUDEOIL_PRODUCTION", "description": "Crude oil production", "category": "STATCAN"},
        {"code": "SC_NETFIELD_PRODUCTION", "description": "Net Field production of crude oil", "category": "STATCAN"},
        {"code": "SC_HEAVY_CRUDEOIL", "description": "Heavy crude oil", "category": "STATCAN"},
        {"code": "SC_LIGHT_MEDIUM_CRUDEOIL", "description": "Light and medium crude oil", "category": "STATCAN"},
        {"code": "SC_NON_UP_BITUMEN", "description": "Non-upgraded production of crude bitumen", "category": "STATCAN"},
        {"code": "SC_IN_SITU_BITUMEN", "description": "In-Situ crude bitumen production", "category": "STATCAN"},
        {"code": "SC_MINED_BITUMEN", "description": "Mined crude bitumen production", "category": "STATCAN"},
        {"code": "SC_BITUMEN_FOR_PROCESSING", "description": "Crude bitumen sent for further processing",
         "category": "STATCAN"},
        {"code": "SC_SYNCRUD", "description": "Synthetic crude oil production", "category": "STATCAN"},
        {"code": "SC_EQUIVALENT_PRODUCT", "description": "Equivalent products production", "category": "STATCAN"},
        {"code": "SC_CONDENSATE", "description": "Condensate", "category": "STATCAN"},
        {"code": "SC_PENTANES_PLUS", "description": "Pentanes plus", "category": "STATCAN"}
    ]

    cubic_meter_to_barrel = 6.2898

    def get_sources(self):
        """
        List the sources to process.
        :return: NoReturn
        """
        sources = [BaseSource(code=f"{self.job_code}_{file['name']}",
                              url=f"{self.base_url}{file['file']}",
                              path=f"{self.job_code}_{file['file']}",
                              long_name=f"Statcan {file['name'].title()}",
                              meta_data={'csv': f"{file['file'].split('-')[0]}.csv"}) for file in self.files]

        logger.info(f"{len(sources)} sources to process.")
        self.sources.extend(sources)

    def transform(self):
        """
        Transforms the source content into the star schema of external db.
        :return: NoReturn
        """
        logger.info('Transforming sources: reading all sources data...')
        dfs = [self._get_df(source) for source in self.sources]
        if len(dfs) == 0:
            logger.info("No source files to process. Exiting transform().")
            return
        df = pd.concat(dfs)
        logger.info(f'{len(df)} rows read from the source files.')
        self.__transform_entity(df)
        self.__transform_details()

        # prep entity (GEO) dimension
        df['GEO'] = df['GEO'].str.upper().str.replace(' ', '_')
        df.replace({'GEO': {'CANADA': 'None'}}, inplace=True)
        df.rename(columns={'REF_DATE': 'period',
                           'GEO': 'entity',
                           'UOM': 'unit',
                           'VALUE': 'value'}, inplace=True)

        df['detail'] = 'None'
        df.dropna(subset=['value'], inplace=True)
        ngl = df[df['product'].isin(['Propane', 'Butane', 'Ethane'])].copy()
        oil = df[~df['product'].isin(['Propane', 'Butane', 'Ethane'])].copy()
        del df
        logger.debug(f'Data split in oil ({len(oil)} rows) and ngl ({len(ngl)} rows).')

        # Let's process oil data
        oil = oil[oil['unit'] == 'Barrels']
        logger.debug(f'oil: {len(oil)} rows after keeping only unit=Barrels')

        details_df = self._prepare_detail_df()
        oil = oil.merge(details_df, left_on='product', right_on='description', how='inner')
        logger.debug(f'oil: {len(oil)} rows after merging with details')

        oil.drop(columns=['product', 'detail', 'description',
                          'category'], inplace=True)

        oil.rename(columns={'code': 'detail',
                            'prods': 'product'}, inplace=True)

        # Let's process NGL data
        ngl['product'] = ngl['product'].str.upper()
        ngl['unit'] = 'Barrels'
        # NGL data has SCALAR_FACTOR = 'thousands' in the source file:
        ngl['value'] = ngl['value'] * self.cubic_meter_to_barrel * 1000

        # Let's put oil and ngl together again
        df = pd.concat([oil, ngl], ignore_index=True, sort=True)
        df['period'] = pd.to_datetime(df['period'], format="%Y-%m")
        # Let's convert barrels to kbd
        df['day_in_month'] = df['period'].map(lambda x: calendar.monthrange(x.year, x.month)[1])
        df['value'] = round(df['value'] / (df['day_in_month'] * 1000), 3)
        del df['day_in_month']
        df['unit'] = 'KBD'

        # put dataframe in star schema format
        df['period'] = df['period'].dt.strftime("%b%Y").str.upper()
        df = (df.assign(provider=self.provider_code).
              assign(area=self.area).
              assign(flow=self.flow).
              assign(frequency=self.frequency).
              assign(original=self.original)
              )
        logger.info(f'{len(df)} rows to be sent to External DB.')
        self.data = df.to_dict('records')

    def __transform_entity(self, df):
        """
        Calculates entities to be processed and insert into self.dynamic_dim['entity'].
        :param df: pd.DataFrame: dataframe with all data.
        :return: NoReturn
        """
        entities = df[['GEO']].drop_duplicates()
        entities = entities[entities['GEO'] != 'Canada']
        logger.info(f"Processing entities: {len(entities)} distinct values after removing 'Canada'")
        entities.columns = ['long_name']
        entities['code'] = entities['long_name'].str.upper().str.replace(' ', '_')
        entities['category'] = 'province'
        self.dynamic_dim['entity'] = entities.to_dict('records')
        self.remove_existing_dynamic_dim('entity')

    def __transform_details(self):
        """
        Calculates details to be processed and insert into self.dynamic_dim['detail'].
        :return: NoReturn
        """
        logger.info(f'{len(self.details)} rows for dimension detail.')
        detail = [{**k, 'json': {}} for k in self.details]
        self.dynamic_dim['detail'] = detail
        self.remove_existing_dynamic_dim('detail')

    @staticmethod
    def _get_df(source: BaseSource) -> pd.DataFrame:
        """
        Get the data from zip file and keep kb.
        :param source: BaseSource: object representing the source to be processed.
        :return: pd.DataFrame containing the processed data.
        """
        path = FILE_STORE_PATH / source.path
        logger.debug(f'Opening file {path.name}')
        z = zipfile.ZipFile(Path(FILE_STORE_PATH, source.path))
        csv_file = source.meta_data['csv']
        with z.open(csv_file) as file:
            df = pd.read_csv(file)
        logger.debug(f'{len(df)} rows read from {csv_file}')

        df.columns = df.columns.str.replace('Supply.*', 'product')
        df = df[["REF_DATE", "GEO", "product", "UOM", "VALUE"]]
        df['source'] = source.code
        return df

    def _prepare_detail_df(self):
        """
        Create a data frame with details.
        :return: pd.DataFrame: dataframe with details.
        """
        df = pd.DataFrame(self.details)
        df['prods'] = 'CRUDEOIL'
        df.loc[df['code'].str.contains('BITUMEN'), 'prods'] = 'BITUMEN'
        df.loc[df['code'] == 'SC_SYNCRUD', 'prods'] = 'NONCONV'
        df.loc[df['code'].isin(['SC_EQUIVALENT_PRODUCT', 'SC_CONDENSATE',
                                'SC_PENTANES_PLUS']), 'prods'] = 'COND'
        return df
