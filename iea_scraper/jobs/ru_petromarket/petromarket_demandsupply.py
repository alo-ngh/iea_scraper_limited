import logging
from pathlib import Path
from typing import NoReturn
import pandas as pd

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, EXT_DB_STR, PROXY_DICT

logger = logging.getLogger(__name__)

class PetromarketDemandsupplyJob(ExtDbApiDedicatedTableJob):
    """
    Data Uploader for the Petro Market monthly demand/supply etc data.

    """

    title: str = 'Petro Market Monthly'

    job_code = Path(__file__).parent.parts[-1]
    provider_code: str = job_code # .upper()
    provider_long_name: str = 'Petro Market'
    provider_url: str = 'http://www.petromarket.ru/'

    # DB schema
    db_schema = 'main'
    db_table_prefix = 'petromarket'
    key_columns = ['product', 'demand_supply', 'category', 'subcategory', 'area', 'date']

    # dedicated subdirectory in the filestore
    filestore_dir = Path(__file__).parent.stem
    file_name = f'Russia\'s Balances for IEA.xlsx'

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


    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        """

        logger.info('Defining list of files to load.')

        data = BaseSource(url=self.provider_url,
                          code=f'{self.provider_code}',
                          path=f'{self.filestore_dir}/{self.file_name}',
                          long_name=f"{self.provider_long_name}")

        self.sources.append(data)


    @staticmethod
    def download_source(source):
        """
            Overriding parent's method to avoid any download.
            For now, file download is MANUAL.
            :param source: BaseSource object describing the file to download.
            Defined as a static method to allow overloading
        """
        logger.warn("Download method not implemented")



    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data in appropriate format
        :return: NoReturn
        """

        logger.info(f'Start to transform')

        if len(self.sources) == 0:
            return None

        else:
            file = FILE_STORE_PATH / self.sources[0].path
            df_all = pd.read_excel(file ,
                                   sheet_name=['OIL', 'Condensate', 'LPG', 'GASOLINE', 'NAPHTHA', 'JET FUEL',
                                   'GASOIL', 'VGO', 'FUEL OIL', 'COKE'], index_col=0)

        df_return = None
        if len(df_all) > 0:
            try:
                logger.debug("Concatenating results ...")

                for key, df in df_all.items():
                    logger.info(f'start transform {key}.')

                    if key == 'OIL':
                        df = self.transpose_oil(key, df)
                    elif key == 'Condensate':
                        df = self.transpose_condensate(key, df)
                    elif key == 'GASOIL':
                        df = self.transpose_gasoil(key, df)
                    else:
                        df = self.transpose_other(key, df)

                    df_return = pd.concat([df_return, df])

                logger.info(f'Return counts: {len(df_return)}.')

                # add provider code / file name
                df_return['provider'] = self.provider_code
                df_return['source'] = self.file_name

                logger.debug(f"Loading {len(df_return)} rows to self.data")
                self.data = df_return

            except ValueError as e:
                logger.exception(f"Error while concatenating data frames, not transforming data")
        else:
            logger.warning(f'No data read from file list: {self.sources}.')



    def transpose_oil(self, product : str, df_tmp : pd.DataFrame):
        """
        Transpose df to tabular form for oil products.

        :param df_tmp:
        :return:
        """

        df_oil = df_tmp.iloc[[1,2,5,6,10,11,12,13],:].reset_index().copy()
        df_oil.rename(columns={'RUSSIA\'s CRUDE OIL(*) BALANCE (thou. tonnes)':'category'}, inplace=True)

        df_oil['demand_supply'] = ['Supply', 'Supply', 'Demand', 'Demand', 'Demand', 'Demand', 'Demand', 'Demand']
        df_oil['category'] = ['Production', 'Imports', 'InsideDemand', 'InsideDemand', 'Exports', 'Exports', 'Exports', 'Exports']
        df_oil['subcategory'] = ['', '', 'Throughput', 'Use/Loss', 'CrudeOil', 'CrudeOil', 'CrudeOil', 'Condensate']
        df_oil['area'] = ['', '', '', '', 'CISWest', 'CISEast', 'CISInside', '']

        df_oil.set_index(['demand_supply', 'category', 'subcategory', 'area'], inplace=True)
        df_oil_return = df_oil.stack().reset_index()
        df_oil_return.rename(columns={'level_4': 'date', 0: 'value'}, inplace=True)

        df_oil_return['product'] = product
        df_oil_return = df_oil_return.reindex(
            columns=['product', 'demand_supply', 'category', 'subcategory', 'area', 'date', 'value'])

        logger.info(f'Return {product} counts : {len(df_oil_return)} rows.')

        return df_oil_return

    def transpose_condensate(self, product : str, df_tmp : pd.DataFrame):
        """
        Transpose df to tabular form for condensate products.

        :param df_tmp:
        :return:
        """

        df_condensate = df_tmp.iloc[[2, 3, 5, 6, 7], :].reset_index().copy()
        df_condensate.rename(columns={'RUSSIAN GAS CONDENSATE SUPPLY AND DEMAND BALANCE (thou. tonnes)': 'category'},
                             inplace=True)

        df_condensate['demand_supply'] = ['Supply', 'Supply', 'Demand', 'Demand', 'Demand']
        df_condensate['category'] = ['Production', 'Imports', 'InsideDemand', 'InsideDemand', 'Exports']
        df_condensate['subcategory'] = ['', '', 'Throughput', 'Use/Loss', '']
        df_condensate['area'] = ['', '', '', '', '']

        df_condensate.set_index(['demand_supply', 'category', 'subcategory', 'area'], inplace=True)
        df_condensate_return = df_condensate.stack().reset_index()
        df_condensate_return.rename(columns={'level_4': 'date', 0: 'value'}, inplace=True)

        df_condensate_return['product'] = product
        df_condensate_return = df_condensate_return.reindex(
            columns=['product', 'demand_supply', 'category', 'subcategory', 'area', 'date', 'value'])

        logger.info(f'Return {product} counts : {len(df_condensate_return)} rows.')

        return df_condensate_return

    def transpose_gasoil(self, product : str, df_tmp : pd.DataFrame):
        """
        Transpose df to tabular form for gasoil products.

        :param df_tmp:
        :return:
        """

        # prepare for the deasel
        df_diesel = df_tmp.iloc[[1, 2, 4, 5], :].reset_index().copy()
        df_diesel.rename(columns={'RUSSIA\'s DIESEL BALANCE (thou. tonnes)': 'category'}, inplace=True)

        df_diesel['demand_supply'] = ['Supply', 'Supply', 'Demand', 'Demand']
        df_diesel['category'] = ['Production', 'Imports', 'Consumption', 'Exports']
        df_diesel['subcategory'] = ['', '', '', '']
        df_diesel['area'] = ['', '', '', '']

        df_diesel.set_index(['demand_supply', 'category', 'subcategory', 'area'], inplace=True)
        df_diesel_return = df_diesel.stack().reset_index()
        df_diesel_return.rename(columns={'level_4': 'date', 0: 'value'}, inplace=True)

        df_diesel_return['product'] = 'DIESEL'
        df_diesel_return = df_diesel_return.reindex(
            columns=['product', 'demand_supply', 'category', 'subcategory', 'area', 'date', 'value'])

        # prepare for the heat oil
        df_heating_oil = df_tmp.iloc[[15, 16, 18, 19], :].reset_index().copy()
        df_heating_oil.rename(columns={'RUSSIA\'s DIESEL BALANCE (thou. tonnes)': 'category'}, inplace=True)

        df_heating_oil['demand_supply'] = ['Supply', 'Supply', 'Demand', 'Demand']
        df_heating_oil['category'] = ['Production', 'Imports', 'Consumption', 'Exports']
        df_heating_oil['subcategory'] = ['', '', '', '']
        df_heating_oil['area'] = ['', '', '', '']

        df_heating_oil.set_index(['demand_supply', 'category', 'subcategory', 'area'], inplace=True)
        df_heating_oil_return = df_heating_oil.stack().reset_index()
        df_heating_oil_return.rename(columns={'level_4': 'date', 0: 'value'}, inplace=True)

        df_heating_oil_return['product'] = 'HEATING OIL'
        df_heating_oil_return = df_heating_oil_return.reindex(
            columns=['product', 'demand_supply', 'category', 'subcategory', 'area', 'date', 'value'])

        df_gasoil_return = pd.concat([df_diesel_return, df_heating_oil_return])

        logger.info(f'Return {product} counts : {len(df_gasoil_return)} rows.')

        return df_gasoil_return

    def transpose_other(self, product : str, df_tmp : pd.DataFrame):
        """
        Transpose df to tabular form for other products.

        :param df_tmp:
        :return:
        """

        df_other = df_tmp.iloc[[1, 2, 4, 5], :].reset_index().copy()

        if product == 'GASOLINE':
            title = 'MOTOR GASOLINE'
        elif product == 'COKE':
            title = 'COKES'
        else:
            title = product
        df_other.rename(columns={f'RUSSIA\'s {title} BALANCE (thou. tonnes)': 'category'}, inplace=True)

        df_other['demand_supply'] = ['Supply', 'Supply', 'Demand', 'Demand']
        df_other['category'] = ['Production', 'Imports', 'Consumption', 'Exports']
        df_other['subcategory'] = ['', '', '', '']
        df_other['area'] = ['', '', '', '']

        df_other.set_index(['demand_supply', 'category', 'subcategory', 'area'], inplace=True)
        df_other_return = df_other.stack().reset_index()
        df_other_return.rename(columns={'level_4': 'date', 0: 'value'}, inplace=True)

        df_other_return['product'] = product
        df_other_return = df_other_return.reindex(
            columns=['product', 'demand_supply', 'category', 'subcategory', 'area', 'date', 'value'])

        logger.info(f'Return {product} counts : {len(df_other_return)} rows.')

        return df_other_return
