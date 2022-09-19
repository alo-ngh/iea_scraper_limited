from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
import logging
from pathlib import Path
from copy import copy
from iea_scraper.settings import FILE_STORE_PATH
import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL = "https://ourairports.com/data"
JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()

FT_TO_M = 0.3048

AIRPORT_METADATA_COLS = [
                 'latitude_deg',
                 'longitude_deg',
                 'elevation_m',
                 'continent',
                 'iso_country',
                 'iso_region',
                 'municipality',
                 'iata_code',
                 'wikipedia_link']

REGIONS_METADATA_COLS = ["local_code", "continent", "iso_country", "wikipedia_link", "keywords"]


class AirportDataJob(job.ExtDbApiJob):
    """
    Class for downloading files from ourairports.com:
    * airports.csv - to be loaded in dimension.LU_entity
    * countries.csv
    * regions.csv
    """
    title: str = "OurAirports - Airport data"

    def get_sources(self):
        """
        Fill in self.sources with one BaseSource element for each file.
        :return: None
        """
        logger.info("Getting sources...")
        files = ['airports', 'countries', 'regions']

        self.sources = [BaseSource(url=f"{BASE_URL}/{file}.csv",
                                   code=f"{JOB_CODE}_{file}",
                                   path=f"{JOB_CODE}_{file}.csv",
                                   long_name=f"{PROVIDER} {file.title()}") for file in files]

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
        logger.info("Loading provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = f"OurAirports.com"
        provider["url"] = "https://ourairports.com"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    def __transform_airports(self):
        """
        Loads the entity dimension with airport data.
        Loading only airports.
        :return: None
        """
        logger.info("Loading airports...")
        sources = [source for source in self.sources if source.code.split('_')[-1] == 'airports']
        if len(sources) == 0:
            logger.debug('No airport data sources to process.')
            return
        source = sources[0]
        path = FILE_STORE_PATH / source.path
        logger.debug(f"Reading file {path}")
        df = pd.read_csv(path)
        logger.debug(f"Reading file {path}: {len(df)} rows read.")

        df = df[((df['type'] == 'large_airport') | (df['type'] == 'medium_airport')) & ~df['iata_code'].isna()]
        logger.debug(f"Loading only large and medium airports having IATA code: {len(df)} rows left.")

        # filtering duplicates
        df = df[~((df['iata_code'] == 'JNB') & ((df['iso_region'] == 'ZA-U-A')|(df['wikipedia_link'].isnull())))]
        df = df[~((df['iata_code'] == 'KCZ') & df['wikipedia_link'].isnull())]
        df = df[~((df['iata_code'] == 'IZA') & df['name'].str.contains('Zona'))]
        df = df[~((df['iata_code'] == 'YTY') & df['municipality'].isnull())]
        # mapping to entity
        df['category'] = 'airport'
        df['code'] = df['iata_code']
        df['long_name'] = df['iso_region'] + ' - ' + df['name']
        df['elevation_m'] = df['elevation_ft'] * FT_TO_M

        # json.dump() used to convert dictionary to str to allow drop_duplicates()
        df['meta_data'] = df.apply(lambda x: {column: x[column]
                                              for column in AIRPORT_METADATA_COLS if type(x[column]) == str}
                                   , axis=1)
        df = df[['code', 'long_name', 'category', 'meta_data']]
        # add dictionary to dynamic dims
        logger.debug(f'Number of airports after deduplication: {len(df)}')
        self.dynamic_dim['entity'].extend(df.to_dict('records'))
        self.remove_existing_dynamic_dim('entity')

    def __transform_regions(self):
        """
        Loads the entity dimension with regions data.
        :return: None
        """
        logger.info("Loading regions...")
        sources = [source for source in self.sources if source.code.split('_')[-1] == 'regions']
        if len(sources) == 0:
            logger.debug('No regions data sources to process.')
            return
        source = sources[0]
        path = FILE_STORE_PATH / source.path
        logger.debug(f"Reading file {path}")
        df = pd.read_csv(path)
        logger.debug(f"Reading file {path}: {len(df)} rows read.")

        df = df[~(df['code'] == 'ES-PM')]
        df = df[~((df['local_code'] == '139') & (df['iso_country'] == 'SI'))]
        df = df[~((df['local_code'] == 'TO') & (df['iso_country'] == 'UZ'))]
        logger.debug(f"{path}: {len(df)} rows after filtering.")

        # mapping to entity
        df['category'] = 'region'
        df['long_name'] = df['iso_country'] + " - " + df['name']
        logger.debug("Filling null long names")
        df.loc[df['code'] == 'NA-U-A', 'long_name'] = df['code'] + ' (unassigned)'
        df.loc[df['long_name'].isnull(), 'long_name'] = df['code'] + ' (unassigned)'

        logger.debug(f" remaining nulls: {df[df['long_name'].isnull()]}")
        df['meta_data'] = df.apply(lambda x: {column: x[column]
                                              for column in REGIONS_METADATA_COLS if type(x[column]) == str}
                                   , axis=1)
        df = df[['code', 'long_name', 'category', 'meta_data']]
        # add dictionary to dynamic dims
        logger.debug(f'Number of regions after deduplication: {len(df)}')
        self.dynamic_dim['entity'].extend(df.to_dict('records'))
        self.remove_existing_dynamic_dim('entity')

    def transform(self):
        """
        Loads provider dimension.
        Loads file airports into dimension.LU_area.
        :return:
        """
        self.__transform_provider()
        self.__transform_airports()
        self.__transform_regions()
