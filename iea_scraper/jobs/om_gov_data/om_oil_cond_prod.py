import pandas as pd
from pathlib import Path
from copy import copy
import json

from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import convert_bbl_to_kbd
from iea_scraper.settings import FILE_STORE_PATH

import logging

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()
PROV_LONG_NAME = "Sultanate of Oman - National Centre for Statistics & Information"
PROV_URL = "https://data.gov.om"
AREA = "OMAN"
ENTITY = "None"
UNIT = "KBD"
FLOW = "SUPPLY"
SECTOR = "None"
FREQUENCY = "Monthly"
ORIGINAL = True

BASE_URL = "https://data.gov.om/api/1.0/data/OMOLGS2016" \
           "?region=1000000" \
           "&indicator=1000020" \
           "&oil-refineries-in-oman=1000000" \
           "&type-of-petroleum-products=1000020,1000030" \
           "&oil-producing-companies=1000000" \
           "&minerals-type=1000000" \
           "&gas-uses-type=1000000" \
           "&country=1000000" \
           "&frequencies=M"

# time variable will be used if not full load
#           "&time=2009M1-2019M12&" \

FILE_NAME = f"{JOB_CODE}_oil_cond.json"

COLS_TO_KEEP = {'source': 'source',
                'Time': 'period',
                'Value': 'value',
                'type-of-petroleum-products': 'product'}


class SmartDict(dict):
    """
    A dictionary that returns the key as value when key is not in dict.
    Useful for mapping values in data frame.
    """
    def __missing__(self, key):
        """
        This defines the value to return when a key is missing.
        In this case, we return the key as default value.
        :param key: searched key
        :return: the key as value
        """
        return key


PRODUCT_MAPPER = SmartDict({'Crude Oil': 'CRUDEOIL',
                            'Condensate MTBE': 'COND'})


class OmOilCondProdJob(job.ExtDbApiJob):
    """
    This class gets data for Oil & Condensate Production for Oman.
    Data is taken from the REST API provided by the Data Portal of the National Centre for Statistics & Information.
    It relies on the data series 'OMOLGS2016', indicator 'Production of Condensate MTBE & Crude Oil', type of petroleum
    products 'Crude Oil' and 'Condensate MTBE'.
    Data is provided in barrels (BBL) and converted to thousands barrels per day (KBD) before loading into
    IEA External DB.

    Currently we are loading the whole history as it is small.
    But it can be improved further to have a full_load or only current month data by adding 'time' parameter to query.
    """
    title: str = "Oman - Oil & Condensate Monthly Production"

    def get_sources(self):
        """
         Add sources into self.sources and insert into database.
         """
        source = BaseSource(code=f"{FILE_NAME.split('.')[0]}",
                            url=BASE_URL,
                            path=f"{FILE_NAME}",
                            long_name=f"{AREA} {PROVIDER} "
                                      f"Monthly Oil & Condensate Production"
                            )
        self.sources.append(source)

        # add dictionary to dynamic dims
        for source in self.sources:
            dicto = vars(copy(source))
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    def transform(self):
        """
        Converts data from BBL to KBD and load it into External DB.
        """
        self.__transform_provider()
        dfs = []
        for source in self.sources:
            source_df = self.__get_data_from_source(source)
            dfs.append(source_df)

        if len(dfs) == 0:
            logger.warning(f"No data to process")
            return None

        # Now we can transform data
        df = pd.concat(dfs)
        # Keep only the columns to keep
        # [*COLS_TO_KEEP] unpacks the dict COLS_TO_KEEP (which returns its keys) as a list
        df = df[[*COLS_TO_KEEP]]\
            .rename(columns=COLS_TO_KEEP)

        # map values in product, convert value to kbd, convert time to period
        df['product'] = df['product'].map(PRODUCT_MAPPER)
        # first convert to timestamp to help conversion to kbd
        df['period'] = pd.to_datetime(df['period'])
        # convert from bbl to kbd
        df['value'] = df.apply(lambda x: convert_bbl_to_kbd(x['value'], x['period'].year, x['period'].month),
                               axis=1)
        # convert timestamp into MMMYYYY (eg.: 2001-01-01 -> 'JAN2001'
        df['period'] = df['period'].map(lambda x: x.strftime('%b%Y').upper())

        # add missing columns (provider, area, entity, frequency, unit, original)
        df = df.assign(provider=PROVIDER)\
               .assign(area=AREA) \
               .assign(entity=ENTITY) \
               .assign(frequency=FREQUENCY) \
               .assign(unit=UNIT) \
               .assign(flow=FLOW) \
               .assign(sector=SECTOR) \
               .assign(original=ORIGINAL)

        logger.debug(f"Number of records in output data frame: {df.count()}")
        self.data = df.to_dict('records')

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Transforming provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = PROV_LONG_NAME
        provider["url"] = PROV_URL

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    @staticmethod
    def __get_data_from_source(source):
        logger.debug(f'Getting data from {source.path}')
        full_path = FILE_STORE_PATH / source.path

        content = None
        with open(full_path, encoding="utf8") as file:
            content = file.read()

        if not content:
            logger.warn(f"file {source.path} is empty.")
            return None

        # json content to data frame
        data = json.loads(content)['data']

        if len(data) == 0:
            logger.warn(f"JSON file {source.path} contains no data.")
            return None

        return pd.DataFrame(data)\
                 .assign(source=source.code)
