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
from lxml import html
import logging
import sys
import os
from urllib.request import Request, urlopen
import io
from zipfile import ZipFile

from iea_scraper.jobs.utils import get_driver, FILE_STORE_PATH
from iea_scraper.settings import BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob
from iea_scraper.core import factory
from iea_scraper.jobs.ph_iemop.power_plants_ph import POWER_PLANTS_MAPPING_EDC

        
class PhilippinesPowerStatsJob(EdcBulkJob):
    
    title: str = 'IEMOP.PH - Philippines Power Statistics'
    
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.url = 'https://www.iemop.ph/market-data/dipc-energy-results-raw'
        self.df_all_data = pd.DataFrame()
        self.dict_datetime_to_key = self.get_datetime_to_key_dict()
        self.earliest_available_date = datetime(2021, 6, 27)

    @property
    def offset_now(self):
        return 4
    
    def get_datetime_to_key_dict(self):
        '''
        Gets a dictionary that converts a date into its code and filename

        '''
        form_data = {'action': 'display_filtered_market_data_files',
             'datefilter%5Bstart%5D': '2021-09-13+00%3A00',
             'datefilter%5Bend%5D': '2021-09-13+12%3A20',
             'sort': '',
             'page': '1',
             'post_id': '5754'}
        r = requests.post('https://www.iemop.ph/wp-admin/admin-ajax.php', 
                          data=form_data, verify=False)
        key_to_files_dict = eval(r.text)['data']
        datetime_to_key = {datetime.strptime(value['date'], '%d %B %Y %H:%M'): 
                           {'key': key, 'filename': value['filename']}
                           for key, value in key_to_files_dict.items()}
        return datetime_to_key
        
    def get_daily_df(self, tdate):
        '''
        tdate : datetime
        Returns a dataframe
        '''
        lst_hour_df = []
        for hour in range(1, 25):
            tdatetime = tdate + timedelta(hours=hour)
            if tdatetime in self.dict_datetime_to_key:
                url = self.url +  '/?md_file=' + self.dict_datetime_to_key[tdatetime]['key']
                df = self.url_to_df(url)
                lst_hour_df += [df]
                logger.info(f'Philippines data scraped for {tdatetime}')
            else:
                logger.warning(f'Philippines data not available for {tdatetime}')
        df_day = pd.concat(lst_hour_df)
        return df_day
    
    def url_to_df(self, url):
        '''
        url : string
        Converts url into zip into csv and eventually returns dataframe
        '''   
        r = requests.get(url, verify=False)
        zip_file = ZipFile(io.BytesIO(r.content))
        filename = zip_file.namelist()[0]
        df = pd.read_csv(zip_file.open(filename))
        return df
    
    def format_df(self, df):
        '''
        Takes a dataframe returns a formatted one
        '''
        df = df.loc[df['TIME_INTERVAL']!='EOF'].copy()
        df['local_datetime'] = pd.to_datetime(df['TIME_INTERVAL'], format='%m/%d/%Y %H:%M:00 %p', errors='ignore')
        df['local_datetime'] = pd.to_datetime(df['local_datetime'], format='%m/%d/%Y', errors='ignore')
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['minute'] = df['local_datetime'].apply(lambda x: x.minute)
        df = df[df['minute'].isin([0,30])].copy()
        df['local_date'] = df['local_datetime'].apply(lambda x: x.date())
        #price and demand
        df_price_and_demand = df.loc[df['SCHED_MW'] < 0].copy()
        df_price_and_demand['MW'] = - df_price_and_demand['SCHED_MW']
        df_price_and_demand['REVENUE'] = df_price_and_demand['MW'] * df_price_and_demand['LMP']
        df_agg = df_price_and_demand.groupby(['local_datetime', 'local_date', 'REGION_NAME']).sum()[['MW', 'REVENUE']].reset_index()
        df_agg['Prices'] = df_agg['REVENUE']/ df_agg['MW']
        df_agg = df_agg.rename(columns={'MW': 'Demand', 'REGION_NAME': 'Region'})
        df_load_and_prices = df_agg.melt(id_vars=['local_date', 'local_datetime', 'Region'], 
                             value_vars=['Prices', 'Demand'],
                             var_name='Metric', value_name='Value')
        df_load_and_prices['Product'] = 'ELE'
        df_load_and_prices.loc[df_load_and_prices['Metric']=='Prices', 'Flow 2'] = 'PHP' 
        #generation
        
        df_gen = df.loc[df['SCHED_MW'] > 0].copy()
        df_gen['RESOURCE_NAME'] = df_gen['RESOURCE_NAME'].apply(lambda x: x[1:] if x[0] == '0' else x)
        df_gen['Product'] = df_gen['RESOURCE_NAME'].apply(lambda x: POWER_PLANTS_MAPPING_EDC[x.replace(' ', '')]
                                                if x.replace(' ','') in POWER_PLANTS_MAPPING_EDC
                                                else 'Other')
        df_generation = df_gen.groupby(['local_datetime', 'local_date', 'REGION_NAME', 'Product']).sum()[['SCHED_MW']].reset_index()
        df_generation = df_generation.rename(columns={'SCHED_MW': 'Value', 'REGION_NAME': 'Region'})
        df_generation['Metric'] = 'Generation'
        df_generation = df_generation[['local_date', 'local_datetime', 'Product',
                                       'Metric', 'Region', 'Value']]
        df_all = pd.concat([df_load_and_prices, df_generation])
        df_all['Country'] = 'PHL'
        df_all['Region'] = df_all['Region'].apply(lambda x: x.title())
        df_all['Source'] = 'IEMOP'
        return df_all
    
    @property
    def df_dw(self):
        return self.df_all_data
        
    def run_date(self, tdate):
        if tdate < self.earliest_available_date:
            logger.info(f'Data is not available for {tdate}. It is only available as of {self.earliest_available_date}')
        else:
            df = self.get_daily_df(tdate)
            self.df_all_data = pd.concat([self.df_all_data, self.format_df(df)])
            logger.info(f'Philippines Demand, Prices and Generation data for {tdate} scraped')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    ph_scraper = factory.get_scraper_job('ph_iemop', 'philippines_power_stats')
    ph_scraper.test_run(folder=folder)
