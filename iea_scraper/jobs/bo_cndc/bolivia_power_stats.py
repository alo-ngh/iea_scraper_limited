# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""

import pandas as pd
import logging
import sys
from datetime import datetime, timedelta
import numpy as np
import requests


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob


class BoliviaPowerStatsJob(EdcBulkJob):
    title: str = 'BOLIVIA.CNDC - Bolivia Power Statistics'

    def __init__(self):
        super().__init__(self)
        self.urls = {'Generation': 'https://www.cndc.bo/media/archivos/boletindiario/inyecsti_',
                     'Demand': 'https://www.cndc.bo/media/archivos/boletindiario/deener_',
                     'Prices': 'https://www.cndc.bo/media/archivos/boletindiario/precmp_'}
        self.input_dfs = {'Generation': pd.DataFrame(),
                          'Demand': pd.DataFrame(),
                          'Prices': pd.DataFrame()}
        self.fuel_mapping = {'TOTAL HIDRO': 'Hydro',
                             'TOTAL EOLICO': 'Wind Onshore',
                             'TOTAL SOLAR': 'Solar',
                             'TOTAL TERMICO': 'Thermal',
                             'TOTAL': 'ELE',
                             'COSTO US$/MWh': 'ELE'}
        self.output_dfs = {'Generation': pd.DataFrame(),
                           'Demand': pd.DataFrame(),
                           'Prices': pd.DataFrame()}

    def get_data(self, tdate, metric):
        '''
        Collects data from html source and converts to DataFrame
        :param tdate: datetime
        :param metric: string
        :return: DataFrame
        '''
        url = self.urls[metric] + str(tdate.strftime('%d%m%y')) + '.htm'
        html = pd.read_html(url, skiprows=8)
        test_point = html[0].iloc[2,2]   
        if '.' not in test_point:
           html = pd.read_html(url, skiprows=8, decimal=',', thousands='.') 
        df = pd.concat(html)
        df = df.rename(columns={df.columns[0]: 'Product'})
        df = df[df['Product'].notna()]
        df = df.drop(df.columns[25], axis=1)
        
        return df

    def get_demand_data(self, tdate):
        '''
        Collects demand data and stores it in input_df
        :param tdate: datetime
        :return: DataFrame
        '''
        metric = 'Demand'
        df = self.get_data(tdate, metric)
        df = df.loc[df['Product'] == 'TOTAL']
        self.input_dfs[metric] = df

    def get_generation_data(self, tdate):
        '''
        Collects generation data and stores it in input_df
        :param tdate: datetime
        :return: DataFrame
        '''
        metric = 'Generation'
        df = self.get_data(tdate, metric)
        df = df[df['Product'].apply(lambda x: x in self.fuel_mapping.keys())]
        df = df.replace('-', 0)
        self.input_dfs[metric] = df

    def get_prices_data(self, tdate):
        '''
        Collects prices data and stores it in input_df
        :param tdate: datetime
        :return: DataFrame
        '''
        metric = 'Prices'
        df = self.get_data(tdate, metric)
        df = df.loc[df['Product'] == 'COSTO US$/MWh']
        df = df.dropna(axis=1)
        self.input_dfs[metric] = df

    def format_df(self, metric, tdate):
        '''
        Formats input_df to output_df = DW format
        :param tdate: datetime
        :return: DataFrame
        '''
        df = self.input_dfs[metric]
        df = pd.melt(df, id_vars='Product', var_name='Hour', value_name='Value')
        df['local_date'] = tdate.strftime('%Y-%m-%d')
        df['local_datetime'] = df['Hour'].apply(
            lambda x: (tdate + timedelta(hours=float(x) - 1)).strftime('%Y-%m-%d %H:00:00'))
        df = df.drop(columns='Hour')
        df['Product'] = df['Product'].apply(lambda x: self.fuel_mapping[x])
        df['Metric'] = metric
        df['Country'] = 'Bolivia'
        df['Source'] = 'CNDC.bo'
        df['Export Date'] = self.export_datetime
        df['utc_datetime'] = pd.to_datetime(df['local_datetime']).dt.tz_localize('America/La_Paz').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Flow 2'] = np.nan
        df.loc[df['Metric'] == 'Prices', 'Flow 2'] = 'USD'
        self.output_dfs[metric] = pd.concat([df, self.output_dfs[metric]])

    @property
    def offset_now(self):
        return 5

    @property
    def df_demand(self):
        return self.output_dfs['Demand']

    @property
    def df_generation(self):
        return self.output_dfs['Generation']

    @property
    def df_prices(self):
        return self.output_dfs['Prices']

    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_demand, self.df_generation, self.df_prices])
        return df_dw

    def run_date(self, tdate):
        self.get_demand_data(tdate)
        logger.info(f'Demand data scraped for {tdate}')
        self.get_generation_data(tdate)
        logger.info(f'Generation data scraped for {tdate}')
        self.get_prices_data(tdate)
        logger.info(f'Prices data scraped for {tdate}')
        for metric in self.urls.keys():
            self.format_df(metric, tdate)
            logger.info(f'{metric} formatted')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    bo_scraper = BoliviaPowerStatsJob()
    # bo_scraper.test_run(folder)
    tdate = datetime(2019, 8, 4)
    bo_scraper.run_date(tdate)
