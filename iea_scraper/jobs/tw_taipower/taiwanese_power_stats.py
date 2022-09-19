# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""

import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core import factory

        
class TaiwanesePowerStatsJob(EdcBulkJob):
    
    title: str = 'Taipower.tw - Taiwanese Power Statistics'
    
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.url = 'https://www.taipower.com.tw/d006/loadGraph/loadGraph/load_supdem_his_E.html'
        self.url_csv = 'https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/sys_dem_sup.csv'
        self.plant_mapping = {'Nuclear': 'Nuclear',
                              'Coal': 'Coal',
                              'Hoping#2': 'Coal',
                              'Mailiao#1': 'Coal',
                              'Mailiao#2': 'Coal',
                              'Mailiao#3': 'Coal',
                              'Co-Gen': 'Cogeneration',
                              'LNG': 'Natural Gas',
                              'IPP-LNG': 'Natural Gas',
                              'Oil': 'Oil',
                              'Diesel': 'Diesel',
                              'Hydro': 'Hydro',
                              'Pumping Gen': 'Hydro Pumped Storage',
                              'Wind': 'Wind',
                              'Solar': 'Solar'}
        self.raw_df = None
        self.df_gen = None
        self.df_demand = None
        self.df_gen_filtered = pd.DataFrame()
        self.df_demand_filtered = pd.DataFrame()

    def get_column_names(self):
        '''Get column names from html to use in the csv import'''
        df_lst = pd.read_html(self.url)
        first_lst = [df_lst[0].iloc[i,0] for i in range(len(df_lst[0]))]
        df2 = df_lst[1]
        df2 = df2.loc[~((df2['Power generation (MW)']=='Nuclear') & (df2['Power generation (MW).1'].isnull()))]
        second_lst = [df2.iloc[i,0] for i in range(len(df2))]
        if "Hoping#1" not in second_lst:
            hoping_2_place = second_lst.index("Hoping#2")
            second_lst = second_lst[:hoping_2_place] + ["Hoping#1"] + second_lst[hoping_2_place:]
        return ['local_date']+ first_lst + [''] + second_lst    

    def retrieve_csv(self):
        df = pd.read_csv(self.url_csv, header=None)
        # df = df.iloc[:, :-1] #remove last column
        df.columns = self.get_column_names()
        df['local_date'] = pd.to_datetime(df['local_date'], format='%Y%m%d')
        self.raw_df = df
    
    def format_demand(self):
        df_demand = self.raw_df[['local_date', 'Power consumption(GWh)']].copy()
        df_demand['Value'] = df_demand['Power consumption(GWh)'].sum(axis=1) /24 *1000 #convert to MW
        df_demand['Metric'] = 'Demand'
        df_demand['Product'] = 'ELE'
        df_demand = df_demand.drop(columns='Power consumption(GWh)')
        self.df_demand = df_demand
    
    def format_generation(self):
        df_gen_pivot = self.raw_df.rename(columns=self.plant_mapping).copy()
        df_gen_pivot = df_gen_pivot[['local_date'] + list(set(self.plant_mapping.values()))]
        df_gen = df_gen_pivot.melt(id_vars=['local_date'],
                                   value_name='Value',
                                   var_name='Product')
        df_gen = df_gen.groupby(['local_date', 'Product']).sum()['Value'].reset_index()
        df_gen['Value'] *= 10 #convert 100kW to MW
        df_gen['Metric'] = 'Generation'
        self.df_gen = df_gen
                                   
        
    @property
    def offset_now(self):
        #last month
        return (self.export_datetime - self.export_datetime.replace(day=1)).days + 1
    
    @property
    def day_lags(self):
        '''
        type : list
        Enables to bypass the 3 weeks logic to scrape the
        last 3 months
        '''
        return list(range(90))
    
    def pre_run(self, historical=True):
        EdcBulkJob.pre_run(self, historical=historical, max_errors=60)
    
    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_gen_filtered, self.df_demand_filtered])
        df_dw['Country'] = 'TWN'
        df_dw['Source'] = 'Taipower'
        return df_dw
        
    def run_date(self, tdate):
        if self.raw_df is None:
            self.retrieve_csv()
            self.format_generation()
            self.format_demand()
        self.df_gen_filtered = pd.concat([self.df_gen.loc[self.df_gen['local_date']==tdate], self.df_gen_filtered])
        self.df_demand_filtered =pd.concat([self.df_demand.loc[self.df_demand['local_date']==tdate], self.df_demand_filtered])
            
            
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    tw_scraper = TaiwanesePowerStatsJob()
    tw_scraper.test_run(folder, historical=True)
    # moon_scraper = factory.get_scraper_job('com_source', 'moon_power_stats')
    # tw_scraper.run_date(datetime(2022,1,1))
    