from pathlib import Path
import zipfile
import pandas as pd
import openpyxl

from io import StringIO

import requests

from iea_scraper.core.job import ExtDbApiJobV2, API_END_POINT
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

import logging

logger = logging.getLogger(__name__)


class WorldCsvJob(ExtDbApiJobV2):
    """
    Scraper for loading Jodi World data in CSV format (primary and secondary files).
    """
    title: str = "JODI - World Data (Primary and Secondary)"

    provider_code = 'JODI'
    provider_long_name = 'Joint Organisations Data Initiative (JODI)'
    provider_url = 'https://www.jodidata.org/'

    base_url = "https://www.jodidata.org/_resources/files/downloads/oil-data"
    files = ("primary", "secondary")
    job_code = Path(__file__).parent.parts[-1]

    mapping_file = Path(__file__).parent / "jodi_iea_mappings.xlsx"

    frequency = "Monthly"

    # Units retained to load into IEA-External-DB
    retained_units = ['KBBL', 'KBD']

    def get_sources(self):
        """
        Function that defines the file sources to be scraped
        with the source being a dictionary with 3 keys('code', 'url', and the "path")
        :return:None
        """
        logger.info("Getting the file sources...")
        sources = [BaseSource(code=f"{self.job_code}_world_{file}",
                              long_name=f"JODI WORLD OIL {file.title()}",
                              url=f"{self.base_url}/world_{file}_csv.zip?iid=34",
                              path=f"{self.job_code}_world_{file}_csv.zip"
                              ) for file in self.files]
        self.sources.extend(sources)

    def transform(self):
        """
         This function reads data from each data source in self.sources and transforms it into a dataframe
        :return:  a Pandas DataFrame.
        """
        logger.info("Transforming data...")
        self.data = []
        for source in self.sources:
            logger.debug(f'processing {source.path}')
            df = self._get_clean_df(source.path)
            logger.debug(f"keeping only data for the following units: {', '.join(self.retained_units)}")
            df = df[df.UNIT_MEASURE.isin(self.retained_units)]

            df['TIME_PERIOD'] = (pd.to_datetime(df.TIME_PERIOD).
                                 dt.strftime("%b%Y").str.upper())
            logger.debug("time transformation")
            df = (df.pipe(self._merge_flow_product_country).
                  rename(columns={'TIME_PERIOD': 'period',
                                  'UNIT_MEASURE': 'unit',
                                  'OBS_VALUE': 'value'
                                  }).
                  assign(provider=self.provider_code,
                         source=source.code,
                         frequency=self.frequency,
                         original=True))
            self.data.extend(df.to_dict('records'))
            logger.debug("data transformation complete.")

    @staticmethod
    def _get_clean_df(path):
        """
        This function gets data from zip file and keeps kb
        :param path: path that leads to the zip file
        :return: a Pandas DataFrame
        """
        logger.debug(f"Opening {str(path)}")
        path = Path(FILE_STORE_PATH, path)
        logger.debug(f'Path: {path}')

        p = zipfile.Path(path)
        list_df = []
        # process all files in zip
        for f in p.iterdir():
            logger.info(f"Processing {path}: reading {f}")
            df = pd.read_csv(StringIO(f.read_text()))
            logger.info(f"Processing {path}: {len(df)} rows read from {f}")
            list_df.append(df)
        logger.info(f'Concatenating contents of files in zip.')
        df = pd.concat(list_df)

        del df['ASSESSMENT_CODE']

        df = df[df['OBS_VALUE'] != 'x']
        logger.debug(f"After removing OBS_VALUE = 'x': {len(df)} rows")

        df.dropna(inplace=True)
        return df

    def _merge_flow_product_country(self, df):
        """
        Maps JODI flow and product definitions to IEA-External DB ones.
        The mapping tables are in one excel file, one sheet for flow and another for product.

        It also maps countries to IEA External DB definitions (from dimension area).

        :param df: the input DataFrame with raw JODI data
        :return: a pandas DataFrame df merged with mapping tables for flow and product
        """
        logger.info("Mapping flow and product to IEA definitions")
        logger.debug(f'Opening mapping file: {str(self.mapping_file)}')
        jodi_iea_mappings = pd.ExcelFile(self.mapping_file, engine='openpyxl')

        logger.debug('Reading sheet jodi_product')
        map_prod = jodi_iea_mappings.parse(sheet_name='jodi_product')
        logger.debug(f'jodi_product: {len(map_prod)} definitions loaded.')
        df = (pd.merge(df, map_prod[['jodi_name', 'short_name']],
                       how='left', left_on='ENERGY_PRODUCT', right_on='jodi_name').
              rename(columns={'short_name': 'product'}).
              drop(columns=['ENERGY_PRODUCT', 'jodi_name']))
        logger.debug("The dataframe has been mapped to the IEA product")

        logger.debug('Reading sheet jodi_flow')
        map_flow = jodi_iea_mappings.parse(sheet_name='jodi_flow')
        logger.debug(f'jodi_flow: {len(map_flow)} definitions loaded.')

        df = (pd.merge(df, map_flow[['jodi_name', 'short_name']],
                       how='left', left_on='FLOW_BREAKDOWN', right_on='jodi_name').
              rename(columns={'short_name': 'flow'}).
              drop(columns=['FLOW_BREAKDOWN', 'jodi_name']))
        df.dropna(inplace=True)
        logger.debug("The dataframe has been mapped to the IEA flow")

        logger.debug('Reading dimension area from External DB API for mapping countries')
        r = requests.get(f"{API_END_POINT}/dimension/area")
        area = pd.DataFrame(r.json())[['iso_alpha_2', 'code']]
        logger.debug(f'Dimension area: {len(area)} definitions loaded.')
        df = (pd.merge(df, area,
                       how='left', left_on='REF_AREA', right_on='iso_alpha_2').
              rename(columns={'code': 'area'}).
              drop(columns=['REF_AREA', 'iso_alpha_2']))
        logger.debug("The dataframe has been mapped to the IEA country definitions.")
        return df
