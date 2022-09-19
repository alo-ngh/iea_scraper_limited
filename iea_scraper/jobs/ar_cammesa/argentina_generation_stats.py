import pandas as pd
from datetime import datetime, timedelta
import json
import logging
import sys

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class ArgentinaGenerationStatsJob(EdcJob):
    title: str = 'CAMMESA - Argentina hourly Power Statistics - generation'

    def __init__(self):
        EdcJob.__init__(self)
        self.url = 'https://api.cammesa.com/demanda-svc/generacion/ObtieneGeneracioEnergiaPorRegion?id_region=1002'
        self.input_df = pd.DataFrame()
        self.generation_df = pd.DataFrame()
        self.product_dict = {'hidraulico':'Hydro',
                             'termico':'Thermal',
                             'nuclear':'Nuclear',
                             'renovable':'Renewables'}

    def get_generation(self):
        """
        extracts generation data for yesterday from API url and updates input_df
        :return: DataFrame
        """
        r = requests.get(self.url, verify=SSL_CERTIFICATE_PATH)
        json_data = json.loads(r.text.encode('utf8'))
        self.input_df = pd.json_normalize(json_data)

    def format_generation(self):
        """
        This methods formats input_df to comply with DW
        """
        df = self.input_df
        df = df.rename(columns={'fecha':'local_datetime'})
        df = df.drop(columns=['sumTotal'])
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df = pd.melt(df, id_vars=['local_datetime', 'local_date'], var_name='Product', value_name='Value')
        df = df.nlargest(96, 'local_datetime') #for last 2 hours
        df['Product'] = df['Product'].map(self.product_dict)
        df['Metric'] = 'Generation'
        df['Country'] = 'Argentina'
        df['Source'] ='Cammesa'
        df['utc_datetime'] = df['local_datetime'].dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].apply(lambda x: x.date())
        df['Export Date'] = self.export_datetime
        self.generation_df =df

    @property
    def df_dw(self):
        df_dw = self.generation_df
        return df_dw

    def pre_run(self):
        self.get_generation()
        self.format_generation()
        logger.info('Generation scraped')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    arg_scraper = ArgentinaGenerationStatsJob()
    arg_scraper.test_run(folder)

