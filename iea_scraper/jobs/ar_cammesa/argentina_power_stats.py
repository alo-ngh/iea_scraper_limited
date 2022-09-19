# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""

import pandas as pd
from datetime import datetime, timedelta
import json
import logging
import sys
import requests

import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
pd.options.mode.chained_assignment = None


class ArgentinaPowerStatsJob(EdcBulkJob):
    title: str = 'CAMMESA - Argentina Daily Power Statistics - demand and prices'

    def __init__(self):
        super().__init__(self)
        self.url_prices_demand = 'https://api.cammesa.com/pub-svc/public/especial/costoMarginal'
        self.input_data = {'prices': pd.DataFrame(),
                           'demand': pd.DataFrame()}
        self.output_data = {'prices': pd.DataFrame(),
                            'demand': pd.DataFrame()}

    @property
    def offset_now(self):
        return 2

    def get_json_data(self, date):
        """
        extracts generation data for yesterday from API url and updates input_df[metric]
        :return: DataFrame
        """
        query = "?fechadesde=" + f'{date.strftime("%Y-%m-%d")}' + "T00%3A00%3A00.000-03%3A00&" + \
                "&fechahasta=" + f'{date.strftime("%Y-%m-%d")}' + "T00%3A00%3A00.000-03%3A00&"
        url = self.url_prices_demand + query
        r = requests.get(url, verify=False)
        json_data = json.loads(r.text.encode('utf8'))
        data = json_data[0]
        self.input_data['prices'] = pd.json_normalize(data['detalle'])
        self.input_data['demand'] = pd.json_normalize(data['detalleDemanda'])
        for metric in self.input_data:
            self.input_data[metric]['local_date'] = data['fecha']

    def format_demand(self, df):
        df['local_datetime'] = df.apply(
            lambda x: datetime.strptime(x['local_date'], "%Y-%m-%d").replace(hour=x['hora'] - 1, minute=x['minuto']),
            axis=1)
        df['demandaReal'] = df.apply(lambda x: x['demandaPrevista'] if x['demandaReal'] == 0 else x['demandaReal'], axis=1)
        df = df.rename(columns={'demandaReal': 'Value'})
        df['Metric'] = 'Demand'
        df['Flow 2'] = np.nan
        return df

    def format_prices(self, df):
        df = df.loc[df['area'] == 'MERCADO']
        df['local_datetime'] = df.apply(
            lambda x: datetime.strptime(x['local_date'], "%Y-%m-%d").replace(hour=x['hora'] - 1),
            axis=1)
        df = df.rename(columns={'costo': 'Value'})
        df['Metric'] = 'Prices'
        df['Flow 2'] = 'ARS'
        return df

    def format_dfs(self, metric):
        """
        This method formats input_df to comply with DW format
        """
        df = self.input_data[metric]
        if metric == 'demand':
            df = self.format_demand(df)
        elif metric == 'prices':
            df = self.format_prices(df)
        else:
            raise KeyError("metric has to  be in ['Demand', 'Prices']")

        df = df[['local_date', 'local_datetime', 'Value', 'Metric', 'Flow 2']]
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize('America/Argentina/Buenos_Aires').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Product'] = 'ELE'
        df['Export Date'] = self.export_datetime
        df['Country'] = 'Argentina'
        df['Source'] = 'Cammesa'
        self.output_data[metric] = pd.concat([df, self.output_data[metric]])

    @property
    def df_dw(self):
        df_dw = pd.concat([self.output_data['demand'], self.output_data['prices']])
        return df_dw

    def run_date(self, tdate):
        self.get_json_data(tdate)
        for metric in self.input_data:
            self.format_dfs(metric)
            logger.info(f'Argentina- {metric} scraped for {tdate.strftime("%Y-%m-%d")}')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    arg_scraper = ArgentinaDailyStatsJob()
    arg_scraper.test_run(folder)
