import logging
from time import sleep
from random import randint
from datetime import datetime
from pathlib import Path
from incapsula import IncapSession

import pandas as pd

from iea_scraper.core.job import ExtDbApiJobV2
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT, SSL_CERTIFICATE_PATH

JOB_CODE = Path(__file__).parent.parts[-1]

SOURCE = f"{JOB_CODE}_Quarterly_Oil"
UNIT = 'KBBL'
AREA = 'NZ'
FLOW = 'SUPPLY'
ORIGINAL = True

QUARTERLY_SHEET = "Quarterly_mmbbls"
ANNUAL_SHEET = "Annual_mmbbls"

logger = logging.getLogger(__name__)


class NzOilStatsJob(ExtDbApiJobV2):
    """
    Scraper for New Zealand Quarterly Oil Statistics.
    """
    title: str = "New Zealand - MBIE Oil Stats"

    base_url = "https://www.mbie.govt.nz/building-and-energy/energy-and-natural-resources/" \
               "energy-statistics-and-modelling/energy-statistics/oil-statistics/"
    url = "https://www.mbie.govt.nz/assets/Data-Files/Energy/nz-energy-quarterly-and-energy-in-nz/oil.xlsx"

    # wait between 5 and 10 seconds (we will get a random number in between)
    wait_between_pages = (5, 10)

    provider_code = 'NZ_MBIE'
    provider_long_name = "NZ Ministry of Business, Innovation and Employment"
    provider_url = url.split('/asset')[0]

    user_agent = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36')

    def get_sources(self):
        """
        Defines a BaseSource object with information about the file to download.
        :return: NoReturn
        """
        logger.info("Getting source.")
        source = {'code': SOURCE,
                  'url': self.url,
                  'path': f"{JOB_CODE}_Quarterly_Oil.xlsx"}
        self.sources.append(BaseSource(**source))

    def transform(self):
        """
        Transform the content of the source file.
        :return: NoReturn
        """
        logger.info("Transforming source.")

        if len(self.sources) == 0:
            logger.info('No data to load.')
            return None

        file = FILE_STORE_PATH / self.sources[0].path
        logger.debug(f'Opening excel file {file.name}')
        xlsx: pd.ExcelFile = pd.ExcelFile(file, engine='openpyxl')
        transforming_functions = [self.__transform_annual_mmbls, self.__quarterly_mmbls]
        # execute each transforming function and concatenate the resulting dataframes
        df = pd.concat([f(xlsx) for f in transforming_functions],
                       sort=True,
                       ignore_index=True)

        self.__prepare_entities(df)

        df['value'] = round(df['value'] * 1000, 1)
        df = (df.assign(provider=self.provider_code).
              assign(unit=UNIT).
              assign(source=SOURCE).
              assign(area=AREA).
              assign(flow=FLOW).
              assign(original=ORIGINAL))
        self.data = df.to_dict('records')
        return None

    @staticmethod
    def __quarterly_mmbls(excel_file: pd.ExcelFile):
        """
        Transforms quarterly data.
        :param excel_file: pd.ExcelFile: ExcelFile object aready opened with pd.ExcelFile().
        :return: pd.DataFrame: dataframe containing the quarterly data.
        """
        logger.debug(f'Reading sheet {QUARTERLY_SHEET}')
        df = pd.read_excel(excel_file, sheet_name=QUARTERLY_SHEET, skiprows=8)
        logger.debug(f'{len(df)} rows read from the sheet {QUARTERLY_SHEET}.')
        columns = df.columns.tolist()
        columns = ["product"] + columns[1:]
        df.columns = columns
        df.drop(columns=['Notes'], inplace=True)
        last_index = df[df['product'] == "Imports"].index[0]
        df = df.iloc[3:last_index - 1]
        df = df.replace({'product': {"Crude Oil, Condensate and Naphtha": 'CRUDEOIL',
                                     "From Other Sources": 'NONCONV'}})
        df['entity'] = 'None'
        df.dropna(axis='columns', inplace=True)
        df = df.melt(id_vars=['entity', 'product'], var_name='period')
        df['period'] = df['period'].map(lambda x: f"{x.quarter}Q{x.year}")
        df['frequency'] = 'Quarterly'
        logger.debug(f'{len(df)} rows after transforming data from sheet {QUARTERLY_SHEET}')

        return df

    @staticmethod
    def __transform_annual_mmbls(excel_file: pd.ExcelFile):
        """
        Transforms annual data.
        :param excel_file: pd.ExcelFile: ExcelFile object aready opened with pd.ExcelFile().
        :return: pd.DataFrame: a dataframe containing the annual data.
        """
        logger.debug(f'Reading sheet {ANNUAL_SHEET}')
        df = pd.read_excel(excel_file, sheet_name=ANNUAL_SHEET, skiprows=8)
        logger.debug(f'{len(df)} rows read from the sheet {ANNUAL_SHEET}.')
        columns = df.columns.tolist()
        columns = ["entity"] + columns[1:]
        df.columns = columns
        df.drop(columns=['Notes'], inplace=True)

        last_index = df[df['entity'] == "Imports"].index[0]
        df = df.iloc[3:last_index]
        df['product'] = 'CRUDEOIL'

        lpg_index = df[df['entity'] == "LPG"].index[0]
        df.loc[lpg_index:, 'product'] = "LPG"

        nonconv_index = df[df['entity'] == "From Other Sources"].index[0]
        df.loc[nonconv_index:, 'product'] = "NONCONV"

        df = df.replace({'entity': {"Crude, Condensate, Naphtha and Natural Gas Liquids": 'None',
                                    "LPG": 'None',
                                    "From Other Sources": 'None'}})

        df = df.melt(id_vars=['entity', 'product'], var_name='period')
        df['entity'] = 'NZ_' + df['entity'].str.upper().str.replace(' ', '_')
        df.loc[df['entity'] == 'NZ_NONE', 'entity'] = 'None'
        df['frequency'] = 'Annual'
        df['period'] = df['period'].astype(str)

        logger.debug(f'{len(df)} rows after transforming data from sheet {ANNUAL_SHEET}')
        return df

    def __prepare_entities(self, df):
        """
        Prepare entities.
        :param df: pd.DataFrame: data to process.
        :return: NoReturn
        """
        entity = df[['entity']].drop_duplicates()
        logger.debug(f'{len(entity)} unique entities to process.')
        entity = entity[entity['entity'] != 'None']
        logger.debug(f'{len(entity)} entities left after removing None.')
        entity["category"] = "field"
        entity.rename(columns={'entity': 'code'}, inplace=True)
        entity['long_name'] = entity['code'].map(
            lambda x: 'Nz ' + x[3:].title()
        )
        entity = entity.to_dict('records')
        self.dynamic_dim['entity'] = entity
        self.remove_existing_dynamic_dim('entity')
        return None

    def download_source(self, source, http_headers=None):
        """
        Download one given file. Uses incapsula-cracker-py3 module to bypass security protection.
        :param source: BaseSource object describing the file to download.
        :param http_headers: optional headers to pass in the request. Default is None.
        Defined as a static method to allow overloading
        """
        try:
            path, url = source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        logger.debug('Trying to bypass Incapsula security with incapsula-cracker-py3 module...')
        session = IncapSession(user_agent=self.user_agent)
        logger.debug(f'Accessing base URL: {self.base_url}')
        r = session.get(self.base_url,
                        proxies=PROXY_DICT,
                        verify=SSL_CERTIFICATE_PATH)
        r.raise_for_status()

        random_wait = randint(*self.wait_between_pages)
        logger.debug(f'Waiting {random_wait}s before trying to download file...')
        sleep(random_wait)

        logger.debug(f'Getting file from URL: {self.url}')
        r = session.get(self.url,
                        proxies=PROXY_DICT,
                        verify=SSL_CERTIFICATE_PATH)
        r.raise_for_status()

        file_path = FILE_STORE_PATH / path
        file_path.write_bytes(r.content)
        logger.debug(f'{len(r.content)} bytes written to {file_path.name}.')

        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
