import datetime
import logging
from copy import copy
from datetime import date
from pathlib import Path
from typing import NoReturn
from sqlalchemy import create_engine
import pandas as pd
import requests
import json

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.settings import FILE_STORE_PATH, EXT_DB_STR, PROXY_DICT

logger = logging.getLogger(__name__)


class IceFuturesJob(ExtDbApiDedicatedTableJob):
    """
    Scraper for settlement prices for ICE Futures products released on a daily basis.

    Brent Crude Source : https://www.theice.com/products/219/Brent-Crude-Futures/data?marketId=5166946
    Low Sulphur Gasoil Futures : https://www.theice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data?marketId=5444732&span=1

    """

    title: str = 'ICE Futures Prices'

    # URL :
    # Brent Crude Source : https://www.theice.com/products/219/Brent-Crude-Futures/data?marketId=5166946
    # Low Sulphur Gasoil Futures : https://www.theice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data?marketId=5444732&span=1https://www.theice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data?marketId=5444732&span=1
    # Dictionary Data :
    # Brent Crude Source : https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=254&hubId=403
    # Low Sulphur Gasoil Futures : https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=5817&hubId=9373

    target_urls = {
        'BrentCrudeFutures' : {
            'online_sources_url' : f'https://www.theice.com/products/219/Brent-Crude-Futures/data?marketId=5166946',
            'dictionary_file_url' : f'https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=254&hubId=403',
            'base_file_url' : f'https://www.theice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&historicalSpan=1&marketId=',
            'source_prefix' : f'brent'
        },
        'LowSulphurGasoilFutures': {
            'online_sources_url': f'https://www.theice.com/products/34361119/Low-Sulphur-Gasoil-Futures/data?marketId=5444732&span=1',
            'dictionary_file_url': f'https://www.theice.com/marketdata/DelayedMarkets.shtml?getContractsAsJson=&productId=5817&hubId=9373',
            'base_file_url': f'https://www.theice.com/marketdata/DelayedMarkets.shtml?getHistoricalChartDataAsJson=&historicalSpan=1&marketId=',
            'source_prefix' : f'lsgasioil'
        }
    }

    job_code = Path(__file__).parent.parts[-1]
    provider_code: str = job_code.upper()
    provider_long_name: str = 'ICE - Intercontinental Exchange, Inc.'
    provider_url: str = 'https://www.theice.com'

    # DB schema
    db_schema = 'main'
    db_table_prefix = 'futures_ice'
    key_columns = ['date', 'target_year', 'target_month']

    # maximum number of workers in parallel processing
    max_worker = 3

    # dedicated subdirectory in the filestore
    filestore_dir = Path(__file__).parent.stem

    def __init__(self,
                 start_year: int = None,
                 end_year: int = None,
                 **kwargs):
        """
        In addition to existing parent's parameters, this defines a year.
        :param start_year: int: start year to load. 1929 if not specified.
        :param end_year: int: end year to load.
        :param kwargs: parent's parameters
        """
        super().__init__(**kwargs)
        filestore_path = FILE_STORE_PATH / self.filestore_dir
        filestore_path.mkdir(exist_ok=True)

        # self.target_urls = target_urls

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        """

        logger.info('Defining list of files to load.')

        data_list = [BaseSource(url=source_url,
                                # code=f'{self.provider_code.lower()}_{source_prefix}_{target_month}',
                                code=f'{date.today().strftime("%y%m%d")}_{source_prefix}_{target_month}',
                                path=f'{self.filestore_dir}/{date.today().strftime("%y%m%d")}_{source_prefix}_{target_month}.csv',
                                target_settle_month = f'{target_month}',
                                long_name=f'ICE FUTURES PRICE DATA FILE {product_name} {target_month} {date.today().strftime("%y%m%d")}')
                     for source_url, source_prefix, product_name, target_month in self._get_data_sources_from_dict_url()]

        self.sources.extend(data_list)

    @classmethod
    def _get_data_sources_from_dict_url(cls) -> list:
        """
        Loads one source file into list.
        :param source: a BaseSource object describing the source file.
        :return: a Pandas DataFrame with the file content.
        """

        try:

            list_data = []

            for k, v in cls.target_urls.items():

                # download dictionary file which includes all the source information targets
                res_list = requests.get(v['dictionary_file_url'], proxies=PROXY_DICT)
                dict_ice_data_list = json.loads(res_list.content)

                # Try to prepare target urls, source_prefix name and product key name
                for i in dict_ice_data_list:
                    target_url = v['base_file_url'] + str(i["marketId"])
                    list_data.append([target_url, v['source_prefix'], k, i['marketStrip']])

            return list_data

        except Exception as e:
            logger.exception(f'Error while reading data list from {v["dictionary_file_url"]}')
            raise e

    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data in appropriate format
        :return: NoReturn
        """
        logger.debug(f"Reading {len(self.sources)} files in parallel...")
        dfs = parallelize(self._get_data_from_source, self.sources, self.max_worker)

        df = None
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)

                # add provider code
                df['provider'] = self.provider_code

                # separate one target column such as 'Jul22' into target year '2022' and target month '7'.
                df['target_year'] = pd.to_datetime(df.target_settle_month, format='%b%y').dt.year
                df['target_month'] = pd.to_datetime(df.target_settle_month, format='%b%y').dt.month
                df.drop(['target_settle_month'], axis=1, inplace=True)

                logger.debug(f"Loading {len(df)} rows to self.data")

                self.data = df

            except ValueError as e:
                logger.exception(f"Error while concatenating data frames, not transforming data")
        else:
            logger.warning(f'No data read from file list: {self.sources}.')


    @classmethod
    def _get_data_from_source(cls, source: BaseSource):
        """
        Loads one source file into a data frame.
        :param source: a BaseSource object describing the source file.
        :return: a Pandas DataFrame with the file content.
        """
        file = FILE_STORE_PATH / source.path
        try:

            logger.debug(f'DEBUG {file}')

            with open(file) as f:
                dict_ice_data = json.load(f)

            # adds a column with the source code for allowing us to trace each row to its source
            df = pd.DataFrame([[date.today(), source.target_settle_month, dict_ice_data['bars'][-1][1], source.code]], columns=['date', 'target_settle_month' ,'value', 'source'])

            return df

        except Exception as e:
            logger.exception(f'Error while reading {source.path}')
            raise e



