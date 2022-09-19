from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
import logging
from pathlib import Path
from copy import copy
from iea_scraper.settings import FILE_STORE_PATH
import pandas as pd

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

BASE_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data"
JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()

COLUMN_NAMES = ['id', 'name', 'alias', 'iata_code', 'icao_code', 'callsign', 'country', 'active']
METADATA_COLS = ['iata_code', 'icao_code', 'callsign', 'country', 'active']


class AirlinesJob(job.ExtDbApiJob):
    """
     Class for downloading files from openflights.org:
     * airlines.dat - CSV file listing airlines
    """
    title: str = "OpenFlights - Airlines data"

    def get_sources(self):
        """
        Fill in self.sources with one BaseSource element for each file.
        :return: None
        """
        logger.info("Getting sources...")
        files = ['airlines']

        self.sources = [BaseSource(url=f"{BASE_URL}/{file}.dat",
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
        provider["long_name"] = f"openflights.org"
        provider["url"] = "https://openflights.org/data.html"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    def __transform_airline(self):
        """
        Loads the entity dimension with airlines data.
        :return: None
        """
        logger.info("Loading airlines...")
        sources = [source for source in self.sources if source.code.split('_')[-1] == 'airlines']
        if len(sources) == 0:
            logger.debug('No airlines data sources to process.')
            return
        source = sources[0]
        path = FILE_STORE_PATH / source.path
        logger.debug(f"Reading file {path}")
        df = pd.read_csv(path, names=COLUMN_NAMES, na_values=r"\N")
        logger.debug(f"Reading file {path}: {len(df)} rows read.")

        # filter Unknown
        df = df[~df['id'].isin([-1, 169, 1620, 2869, 3637, 4560, 4937])]
        # load only active companies
        df = df[df['active'] == 'Y']

        # map to entity
        df['category'] = 'airline'
        # code for private flights
        df.loc[df['id'] == 1, 'code'] = 'PRIV'
        # otherwise, iata or icao
        df.loc[df['icao_code'].notnull(), 'code'] = df['icao_code']
        df.loc[df['icao_code'].isnull(), 'code'] = df['iata_code']
        df['long_name'] = df['code'] + " - " + df['name']
        df['meta_data'] = df.apply(lambda x: {column: x[column]
                                              for column in METADATA_COLS if type(x[column]) == str}, axis=1)
        df = df[['code', 'long_name', 'category', 'meta_data']]
        df = df[~df.isna()]
        df = df[~df.long_name.isna()]
        # add dictionary to dynamic dims
        logger.debug(f'Number of airports after deduplication: {len(df)}')
        self.dynamic_dim['entity'].extend(df.to_dict('records'))
        self.remove_existing_dynamic_dim('entity')

    def transform(self):
        """
        Loads provider dimension.
        Loads file airports into dimension.LU_area.
        :return:
        """
        self.__transform_provider()
        self.__transform_airline()
