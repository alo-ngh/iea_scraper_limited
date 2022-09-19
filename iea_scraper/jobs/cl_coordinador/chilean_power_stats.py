# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""
from time import sleep

import pandas as pd
from datetime import datetime, timedelta
import json
import logging
import sys
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH
from iea_scraper.core.exceptions import EdcJobError

REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'}


class ChileanPowerStatsJob(EdcBulkJob):
    title: str = 'Coordinador.Chile - Chile Daily Power Statistics'

    def __init__(self):
        super().__init__(self)
        self.urls = {'prices': 'https://sipub.api.coordinador.cl/sipub/api/v1/recursos/costo_marginal_programado?',
                     'demand': 'https://sipub.api.coordinador.cl//sipub/api/v1/recursos/demandasistemareal?',
                     'generation': 'https://sipub.api.coordinador.cl/sipub/api/v1/recursos/generacion_centrales_tecnologia_horario?'}
        self.user_key = '7710aad283684e5d04fb88960b667afc'
        self.headers = {'Host': 'sipub.api.coordinador.cl',
                        'Origin': 'https://www.coordinador.cl',
                        'Referer': "https://www.coordinador.cl"}

        self.input_data = {'prices': pd.DataFrame(),
                           'demand': pd.DataFrame(),
                           'generation': pd.DataFrame()}
        self.output_data = {'prices': pd.DataFrame(),
                            'demand': pd.DataFrame(),
                            'generation': pd.DataFrame()}
        self.mnemotecnico_barra__in = "BA01T002SE031T002%2CBA02T002SE032T002%2CBA03T002SE027T002%2CBA22T027SE062G216%2CBA83T002SE188T002%2C"
        self.price_node_mapping = {'BA01T002SE031T002': 'Puerto Montt',
                                   'BA02T002SE032T002': 'Quillota',
                                   'BA03T002SE027T002': 'Pan de Azucar',
                                   'BA22T027SE062G216': 'Crucero',
                                   'BA83T002SE188T002': 'Tarapaca'}
        self.fuel_mapping = {'eolica': 'Wind Onshore',
                             'hidraulica': 'Hydro',
                             'solar': 'Solar',
                             'termica': 'Thermal',
                             'geotermica': 'Geothermal'}
        self.number_of_queries = 0

    @property
    def offset_now(self):
        return 2

    def get_json_data(self, tdate, metric):
        """
        extracts data for metric for yesterday from API url and updates input_df[metric]
        :return: DataFrame
        """
        query = "user_key=" + self.user_key \
                + "&fecha__gte=" + tdate.strftime("%Y-%m-%d") \
                + "&fecha__lte=" + tdate.strftime("%Y-%m-%d")
        if metric == 'prices':
            query += "&mnemotecnico_barra__in=" + self.mnemotecnico_barra__in

        url = self.urls[metric] + query
        if self.number_of_queries / ((self.time_since_start.seconds+0.1) / 3600) > 60:
            try:
                r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=self.headers)
                self.number_of_queries += 1
            except:
                logger.info("Number of queries to the API exceeded in an hour")
                sleep(3601 - self.time_since_start.seconds % 3600)
                self.number_of_queries = 0
        else:
            r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=self.headers)
            self.number_of_queries += 1
        if r.status_code == 504:
            data = []
            logger.warning(f"Chile: request gateway timeout for {metric}, data not available")
        else:
            json_data = json.loads(r.text)
            data = json_data['aggs'] if metric != "demand" else json_data['data']
        self.input_data[metric] = pd.DataFrame(data)

    def format_demand(self, df):
        '''
        formats demand data
        '''
        df = df.rename(columns={'demanda': 'Value'})
        df['Metric'] = 'Demand'
        df['Product'] = 'ELE'
        return df

    def format_prices(self, df):
        '''
        formats prices data (the nodes chosen are main nodes according to map https://sic.coordinador.cl/en/sobre-sic/sic/)
        '''

        df = df.rename(columns={'costo': 'Value', 'mnemotecnico_barra': 'Region'})
        df['Region'] = df['Region'].map(self.price_node_mapping)
        df['Metric'] = 'Prices'
        df['Flow 2'] = 'USD'
        df['Product'] = 'ELE'
        return df

    def format_generation(self, df):
        '''
        formats generation data
        '''
        df = df.rename(columns={'generacion_sum': 'Value', 'tipo_central': 'Product'})
        df['Product'] = df['Product'].map(self.fuel_mapping)
        df['Metric'] = 'Generation'
        df = df[['local_date', 'local_datetime', 'Product', 'Value', 'Metric']]
        return df

    def format_dfs(self, metric):
        """
        This method formats input_df to comply with DW format
        """
        df = self.input_data[metric]
        if not df.empty:
            df = df.rename(columns={'fecha': 'local_date'})
            df = df.loc[df['hora'].isin(range(1,25))]
            df['local_datetime'] = df.apply(
                lambda x: datetime.strptime(x['local_date'], "%Y-%m-%d").replace(hour=x['hora'] - 1), axis=1)
            df['local_date'] = pd.to_datetime(df['local_date']).dt.date
            df = df.drop(columns=['hora'])
            if metric == 'demand':
                df = self.format_demand(df)
            elif metric == 'prices':
                df = self.format_prices(df)
            elif metric == 'generation':
                df = self.format_generation(df)
            else:
                raise KeyError("metric has to  be in ['Demand', 'Prices', 'Generation']")
            df['utc_datetime'] = df['local_datetime'].dt.tz_localize('America/Santiago',
                                                                     nonexistent='shift_forward',
                                                                     ambiguous='NaT').dt.tz_convert('UTC')
            df['utc_date'] = df['utc_datetime'].dt.date
            df['Export Date'] = self.export_datetime
            df['Country'] = 'Chile'
            df['Source'] = 'Coordinador Electrico Nacional'
            self.output_data[metric] = pd.concat([df, self.output_data[metric]])

    @property
    def df_dw(self):
        df_dw = pd.DataFrame()
        for metric in self.output_data:
            if not self.output_data[metric].empty:
                df_dw = pd.concat([df_dw, self.output_data[metric]])
        return df_dw

    def run_date(self, tdate):
        for metric in self.input_data:
            self.get_json_data(tdate, metric)
            self.format_dfs(metric)
            logger.info(f'Chile - {metric} scraped for {tdate.strftime("%Y-%m-%d")}')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    start_time = datetime.now()
    cl_scraper = ChileanPowerStatsJob()
    tdate = datetime(2019,4,6)
    metric = 'demand'
    # cl_scraper.test_run(folder, historical= True)
    cl_scraper.run_date(tdate)
    end_time = datetime.now()
    delta = end_time - start_time

