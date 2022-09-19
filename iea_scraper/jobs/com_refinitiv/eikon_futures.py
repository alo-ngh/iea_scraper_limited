import logging
from datetime import timedelta, date
from pathlib import Path
from typing import NoReturn
import pandas as pd
import eikon as ek
import time

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, REFINITIVE_APP_KEY

logger = logging.getLogger(__name__)

class EikonFuturesJob(ExtDbApiDedicatedTableJob):
    """
    Scraper for settlement prices for Futures products released on a daily basis from Refinitive Eikon.

    """

    title: str = 'Eikon Futures Prices'

    # Code for the Eikon
    list_of_contracts = ['HOc1',
                         'HOc2', 'HOc3',
                         'HOc4', 'HOc5',
                         'HOc6', 'HOc7',
                         'HOc8', 'HOc9',
                         'HOc10', 'HOc11',
                         'HOc12', 'NGc1',
                         'NGc2', 'NGc3',
                         'NGc4', 'NGc5',
                         'NGc6', 'NGc7',
                         'NGc8', 'NGc9',
                         'NGc10', 'NGc11',
                         'NGc12', 'LGOc1',
                         'LGOc2', 'LGOc3',
                         'LGOc4', 'LGOc5',
                         'LGOc6', 'LGOc7',
                         'LGOc8', 'LGOc9',
                         'LGOc10', 'LGOc11',
                         'LGOc12', 'RBc1',
                         'RBc2', 'RBc3',
                         'RBc4', 'RBc5',
                         'RBc6', 'RBc7',
                         'RBc8', 'RBc9',
                         'RBc10', 'RBc11',
                         'RBc12', 'RBc1',
                         'RBc2', 'RBc3',
                         'RBc4', 'RBc5',
                         'RBc6', 'RBc7',
                         'RBc8', 'RBc9',
                         'RBc10', 'RBc11',
                         'RBc12', 'WTCLc1',
                         'WTCLc2', 'WTCLc3',
                         'CLc1', 'CLc2',
                         'CLc3', 'CLc4',
                         'CLc5', 'CLc6',
                         'CLc7', 'CLc8',
                         'CLc9', 'CLc10',
                         'CLc11', 'CLc12',
                         'CLc13', 'CLc14',
                         'CLc15', 'CLc16',
                         'CLc17', 'CLc18',
                         'CLc19', 'CLc20',
                         'CLc21', 'CLc22',
                         'CLc23', 'CLc24',
                         'CLc25', 'CLc26',
                         'CLc27', 'CLc28',
                         'CLc29', 'CLc30',
                         'CLc31', 'CLc32',
                         'CLc33', 'CLc34',
                         'CLc35', 'CLc36',
                         'CLc37', 'CLc38',
                         'CLc39', 'CLc40',
                         'CLc41', 'CLc42',
                         'CLc43', 'CLc44',
                         'CLc45', 'CLc46',
                         'CLc47', 'CLc48',
                         'CLc49', 'CLc50',
                         'CLc51', 'CLc52',
                         'CLc53', 'CLc54',
                         'CLc55', 'CLc56',
                         'CLc57', 'CLc58',
                         'CLc59', 'CLc60',
                         'CLc61', 'CLc62',
                         'CLc63', 'CLc64',
                         'CLc65', 'CLc66',
                         'CLc67', 'CLc68',
                         'CLc69', 'CLc70',
                         'CLc71', 'CLc72',
                         'CLc73', 'CLc74',
                         'CLc75', 'CLc76',
                         'CLc77', 'CLc78',
                         'LCOc1', 'LCOc2',
                         'LCOc3', 'LCOc4',
                         'LCOc5', 'LCOc6',
                         'LCOc7', 'LCOc8',
                         'LCOc9', 'LCOc10',
                         'LCOc11', 'LCOc12',
                         'LCOc13', 'LCOc14',
                         'LCOc15', 'LCOc16',
                         'LCOc17', 'LCOc18',
                         'LCOc19', 'LCOc20',
                         'LCOc21', 'LCOc22',
                         'LCOc23', 'LCOc24',
                         'LCOc25', 'LCOc26',
                         'LCOc27', 'LCOc28',
                         'LCOc29', 'LCOc30',
                         'LCOc31', 'LCOc32',
                         'LCOc33', 'LCOc34',
                         'LCOc35', 'LCOc36']

    # Column Name
    list_of_contract_names = ['NymexULSD_M1',
                              'NymexULSD_M2', 'NymexULSD_M3',
                              'NymexULSD_M4', 'NymexULSD_M5',
                              'NymexULSD_M6', 'NymexULSD_M7',
                              'NymexULSD_M8', 'NymexULSD_M9',
                              'NymexULSD_M10', 'NymexULSD_M11',
                              'NymexULSD_M12', 'NymexHenryHubNatGas_M1',
                              'NymexHenryHubNatGas_M2', 'NymexHenryHubNatGas_M3',
                              'NymexHenryHubNatGas_M4', 'NymexHenryHubNatGas_M5',
                              'NymexHenryHubNatGas_M6', 'NymexHenryHubNatGas_M7',
                              'NymexHenryHubNatGas_M8', 'NymexHenryHubNatGas_M9',
                              'NymexHenryHubNatGas_M10', 'NymexHenryHubNatGas_M11',
                              'NymexHenryHubNatGas_M12', 'ICELSGasoil_M1',
                              'ICELSGasoil_M2', 'ICELSGasoil_M3',
                              'ICELSGasoil_M4', 'ICELSGasoil_M5',
                              'ICELSGasoil_M6', 'ICELSGasoil_M7',
                              'ICELSGasoil_M8', 'ICELSGasoil_M9',
                              'ICELSGasoil_M10', 'ICELSGasoil_M11',
                              'ICELSGasoil_M12', 'NymexRBOB_M1',
                              'NymexRBOB_M2', 'NymexRBOB_M3',
                              'NymexRBOB_M4', 'NymexRBOB_M5',
                              'NymexRBOB_M6', 'NymexRBOB_M7',
                              'NymexRBOB_M8', 'NymexRBOB_M9',
                              'NymexRBOB_M10', 'NymexRBOB_M11',
                              'NymexRBOB_M12', 'NymexRBOBPit_M1',
                              'NymexRBOBPit_M2', 'NymexRBOBPit_M3',
                              'NymexRBOBPit_M4', 'NymexRBOBPit_M5',
                              'NymexRBOBPit_M6', 'NymexRBOBPit_M7',
                              'NymexRBOBPit_M8', 'NymexRBOBPit_M9',
                              'NymexRBOBPit_M10', 'NymexRBOBPit_M11',
                              'NymexRBOBPit_M12', 'ICEWTI_M1',
                              'ICEWTI_M2', 'ICEWTI_M3',
                              'NymexWTI_M1', 'NymexWTI_M2',
                              'NymexWTI_M3', 'NymexWTI_M4',
                              'NymexWTI_M5', 'NymexWTI_M6',
                              'NymexWTI_M7', 'NymexWTI_M8',
                              'NymexWTI_M9', 'NymexWTI_M10',
                              'NymexWTI_M11', 'NymexWTI_M12',
                              'NymexWTI_M13', 'NymexWTI_M14',
                              'NymexWTI_M15', 'NymexWTI_M16',
                              'NymexWTI_M17', 'NymexWTI_M18',
                              'NymexWTI_M19', 'NymexWTI_M20',
                              'NymexWTI_M21', 'NymexWTI_M22',
                              'NymexWTI_M23', 'NymexWTI_M24',
                              'NymexWTI_M25', 'NymexWTI_M26',
                              'NymexWTI_M27', 'NymexWTI_M28',
                              'NymexWTI_M29', 'NymexWTI_M30',
                              'NymexWTI_M31', 'NymexWTI_M32',
                              'NymexWTI_M33', 'NymexWTI_M34',
                              'NymexWTI_M35', 'NymexWTI_M36',
                              'NymexWTI_M37', 'NymexWTI_M38',
                              'NymexWTI_M39', 'NymexWTI_M40',
                              'NymexWTI_M41', 'NymexWTI_M42',
                              'NymexWTI_M43', 'NymexWTI_M44',
                              'NymexWTI_M45', 'NymexWTI_M46',
                              'NymexWTI_M47', 'NymexWTI_M48',
                              'NymexWTI_M49', 'NymexWTI_M50',
                              'NymexWTI_M51', 'NymexWTI_M52',
                              'NymexWTI_M53', 'NymexWTI_M54',
                              'NymexWTI_M55', 'NymexWTI_M56',
                              'NymexWTI_M57', 'NymexWTI_M58',
                              'NymexWTI_M59', 'NymexWTI_M60',
                              'NymexWTI_M61', 'NymexWTI_M62',
                              'NymexWTI_M63', 'NymexWTI_M64',
                              'NymexWTI_M65', 'NymexWTI_M66',
                              'NymexWTI_M67', 'NymexWTI_M68',
                              'NymexWTI_M69', 'NymexWTI_M70',
                              'NymexWTI_M71', 'NymexWTI_M72',
                              'NymexWTI_M73', 'NymexWTI_M74',
                              'NymexWTI_M75', 'NymexWTI_M76',
                              'NymexWTI_M77', 'NymexWTI_M78',
                              'ICEBrent_M1', 'ICEBrent_M2',
                              'ICEBrent_M3', 'ICEBrent_M4',
                              'ICEBrent_M5', 'ICEBrent_M6',
                              'ICEBrent_M7', 'ICEBrent_M8',
                              'ICEBrent_M9', 'ICEBrent_M10',
                              'ICEBrent_M11', 'ICEBrent_M12',
                              'ICEBrent_M13', 'ICEBrent_M14',
                              'ICEBrent_M15', 'ICEBrent_M16',
                              'ICEBrent_M17', 'ICEBrent_M18',
                              'ICEBrent_M19', 'ICEBrent_M20',
                              'ICEBrent_M21', 'ICEBrent_M22',
                              'ICEBrent_M23', 'ICEBrent_M24',
                              'ICEBrent_M25', 'ICEBrent_M26',
                              'ICEBrent_M27', 'ICEBrent_M28',
                              'ICEBrent_M29', 'ICEBrent_M30',
                              'ICEBrent_M31', 'ICEBrent_M32',
                              'ICEBrent_M33', 'ICEBrent_M34',
                              'ICEBrent_M35', 'ICEBrent_M36']


    job_code = Path(__file__).parent.parts[-1]
    provider_code: str = job_code.upper()
    provider_long_name: str = 'Refinitive Eikon'
    provider_url: str = 'https://www.refinitiv.com/en'

    # DB schema
    db_schema = 'main'
    db_table_prefix = 'futures_eikon'
    key_columns = ['date', 'type', 'product' 'month']

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

        self.path = f'{self.filestore_dir}/{date.today().strftime("%y%m%d")}_eikon.csv'
        self.code = f'{date.today().strftime("%y%m%d")}_eikon'

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        """

        logger.info('Definition to load.')

        data_list = [BaseSource(url=self.provider_url,
                                code= self.code,
                                path= self.path,
                                long_name=f'Refinitive Eikon FUTURES {date.today().strftime("%y%m%d")}')]

        self.sources.extend(data_list)

    def download_source(self, source, http_headers=None):
        """
        Download one given file.
        :param source: BaseSource object describing the file to download.
        :param http_headers: optional headers to pass in the request. Default is None.
        Defined as a static method to allow overloading
        """

        logger.debug(f'url : {source.url}')

        try:

            ek.set_app_key(self.REFINITIVE_APP_KEY)

            # Even I take 5 days from today, there is no meaning the dates itself. I need dates in length
            main_df = ek.get_timeseries([self.list_of_contracts[0]], interval='daily', start_date=(date.today() - timedelta(5)).strftime(('%Y-%m-%d %H:%M:%S')), end_date=None)

            main_df = main_df[['CLOSE']]
            main_df.rename(columns={"CLOSE": self.list_of_contract_names[0]}, inplace=True)

            for id, contract in zip(self.list_of_contracts[1:], self.list_of_contract_names[1:]):
                df = ek.get_timeseries([id], interval='daily', start_date= (date.today() - timedelta(5)).strftime(('%Y-%m-%d %H:%M:%S')), end_date=None)
                df = df[['CLOSE']]
                df.rename(columns={"CLOSE": contract}, inplace=True)

                main_df = pd.concat([main_df, df], axis=1)
                # To download completely, put some sleep seconds.
                time.sleep(0.15)

            file_path = FILE_STORE_PATH / source.path
            main_df.to_csv(f"{file_path}", index=True)

        except Exception as e:
            logger.exception('error during download', e)

    def transform(self) -> NoReturn:
        """
        For now, this method simply reads all files to data frames, concatenates them, and put the result
        in self.data in appropriate format
        :return: NoReturn
        """

        logger.debug(f"Reading today's files")
        df = pd.read_csv(FILE_STORE_PATH / self.path)

        if len(df) > 0:
            try:
                logger.debug("Transform dataset ...")

                # Latest date might not be 'settlement' price. Therefore, I put columns for that.
                list_type = ['settle'] * (len(df) - 1)
                list_type.append('live')
                df['type'] = list_type

                # Change table format to tabular one.
                df.set_index(['Date', 'type'], inplace=True)
                df = df.stack()
                df = df.reset_index()

                df.rename(columns={'level_2': 'product_month', 0: 'value'}, inplace=True)
                df = pd.concat([df, df["product_month"].str.split('_M', expand=True)], axis=1).drop('product_month', axis=1)
                df.rename(columns={0: 'product', 1: 'month'}, inplace=True)

                df['provider'] = self.provider_code
                df['source'] = self.code

                logger.debug(f"Loading {len(df)} rows to self.data")

                self.data = df

            except ValueError as e:
                logger.exception(f"Error while concatenating data frames, not transforming data")
        else:
            logger.warning(f'No data read from file list: {self.sources}.')




