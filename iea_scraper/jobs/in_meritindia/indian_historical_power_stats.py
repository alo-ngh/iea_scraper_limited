# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import requests
import pandas as pd
import xml.etree.ElementTree as et
from datetime import datetime, timedelta
import logging
import sys
import os
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
from iea_scraper.core.job import EdcJob, EdcBulkJob
from iea_scraper.core import factory
        
class IndianHistoricalPowerStatsJob(EdcBulkJob):
    
    title: str = 'Carbontracker.in - Historica Indian Power Statistics'
    #to run only for the history, the live scraper does the rest
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.url = 'https://32u36xakx6.execute-api.us-east-2.amazonaws.com/v4/get-merit-data?start_time={start_date}%2000:00:00&end_time={end_date}%2000:00:00&corrected_values=true'
        self.gen_mapping = {'thermal_generation': 'Coal',
                            'hydro_generation': 'Hydro',
                            'gas_generation': 'Natural Gas',
                            'nuclear_generation': 'Nuclear',
                            'renewable_generation': 'Renewables'}
        self.df_all = None
        self.df_gen = pd.DataFrame()
        self.df_demand = pd.DataFrame()
        
    def get_indian_data(self, tdate):
        '''
        Gets both generation and demand at the same time
        tdate : datetime
        '''
        start_date = tdate.strftime('%Y-%m-%d')
        end_date = (tdate + timedelta(days=1)).strftime('%Y-%m-%d')
        r = requests.get(self.url.replace('{start_date}', start_date).replace('{end_date}', end_date))
        data_string = r.json()
        data_dict = json.loads(data_string)['timeseries_values']
        data = [{key: data_dict[key][i] for key in data_dict} for i in range(len(data_dict['timestamps']))]
        df_all = pd.DataFrame(data)
        df_all['local_datetime'] = pd.to_datetime(df_all['timestamps'])
        self.df_all = df_all
        
    def format_gen(self):
        df_gen = self.df_all.melt(id_vars='local_datetime', value_vars=list(self.gen_mapping.keys()),
                                  value_name='Value', var_name='Product')
        if not set(df_gen['Product']).issubset(set(self.gen_mapping.keys())):
            raise ValueError(f"{', '.join(set(self.df_all['Product']))} contains ununaccepted product")
        df_gen['Product'] = df_gen['Product'].apply(lambda x: self.gen_mapping[x])
        df_gen['Metric'] = 'Generation'
        self.df_gen = pd.concat([self.df_gen, df_gen])        
    
    def format_demand(self):
        df_demand = self.df_all[['local_datetime', 'demand_met']].copy()
        df_demand = df_demand.rename(columns={'demand_met': 'Value'})
        df_demand['Product'] = 'ELE'
        df_demand['Metric'] = 'Demand'
        self.df_demand = pd.concat([self.df_demand, df_demand])     
        
    @property
    def offset_now(self):
        return 10
        
    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_gen, self.df_demand])
        df_dw = df_dw.loc[df_dw['local_datetime'].apply(lambda x: x.minute==0)]
        df_dw['Country'] = 'IND'
        df_dw['Source'] = 'Merit India'
        return df_dw
    
    def run_date(self, tdate):
        self.get_indian_data(tdate)
        self.format_gen()
        self.format_demand()
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    in_scraper = IndianHistoricalPowerStatsJob()
    in_scraper.test_run(folder)
    # moon_scraper = factory.get_scraper_job('com_source', 'moon_power_stats')
    
    