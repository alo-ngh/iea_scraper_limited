# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys

from iea_scraper.jobs.utils import get_driver, FILE_STORE_PATH
from iea_scraper.settings import BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob
from iea_scraper.core import factory
from iea_scraper.instance import PATH_SSL_CERTIFICATE
from iea_scraper.jobs.ph_iemop.power_plants_ph import POWER_PLANTS_MAPPING_EDC

        
class PhilippinesHistoricalPowerStatsJob(EdcBulkJob):
    
    title: str = 'IEMOP.PH - Philippines Historica Power Statistics'
    '''
    Only works until 25 June 2021
    '''
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.url_gen = 'http://180.232.125.102/csv/dap/DAPEG_%Y%m%d00.csv'
        self.url_load = 'http://180.232.125.102/csv/dap/DAPEL_%Y%m%d00.csv'
        self.df_load_and_prices = pd.DataFrame()
        self.df_generation = pd.DataFrame()
        self.latest_available_date = datetime(2021, 6, 25)

    @property
    def offset_now(self):
        return 90
    
    def get_load_and_prices(self, tdate):
        '''
        tdate : datetime. has the hour as well.
        '''
        df = pd.read_csv(tdate.strftime(format=self.url_load))
        df = df.rename(columns={col: col.replace(' ','') for col in df.columns})
        df['REVENUE'] = df['MW'] * df['PRICE']
        df_agg = df.groupby(['DELIVERY_DATE', 'DELIVERY_HOUR', 'REGION_ID']).sum()[['MW', 'REVENUE']].reset_index()
        df_agg['Prices'] = df_agg['REVENUE']/ df_agg['MW']
        df_agg['local_date'] = pd.to_datetime(df_agg['DELIVERY_DATE'], format='%m/%d/%Y')
        df_agg['local_datetime'] = df_agg.apply(lambda x: x['local_date'] + timedelta(hours=x['DELIVERY_HOUR'] - 1), axis=1)
        df_agg = df_agg.rename(columns={'MW': 'Demand', 'REGION_ID': 'Region'})
        df_load_and_prices = df_agg.melt(id_vars=['local_date', 'local_datetime', 'Region'], 
                             value_vars=['Prices', 'Demand'],
                             var_name='Metric', value_name='Value')
        df_load_and_prices['Product'] = 'ELE'
        df_load_and_prices.loc[df_load_and_prices['Metric']=='Prices', 'Flow 2'] = 'PHP' 
        self.df_load_and_prices = pd.concat([self.df_load_and_prices, df_load_and_prices])
        
    def get_generation(self, tdate):
        '''
        tdate : datetime. has the hour as well.
        '''
        df = pd.read_csv(tdate.strftime(format=self.url_gen))
        df = df.rename(columns={col: col.replace(' ','') for col in df.columns})
        df = df.loc[~pd.isnull(df['RESOURCE_ID'])]
        df['Product'] = df['RESOURCE_ID'].apply(lambda x: POWER_PLANTS_MAPPING_EDC[x.replace(' ', '')]
                                                if x.replace(' ','') in POWER_PLANTS_MAPPING_EDC
                                                else 'Other')
        df_generation = df.groupby(['DELIVERY_DATE', 'DELIVERY_HOUR', 'REGION_ID', 'Product']).sum()[['MW']].reset_index()
        df_generation['local_date'] = pd.to_datetime(df_generation['DELIVERY_DATE'], format='%m/%d/%Y')
        df_generation['local_datetime'] = df_generation.apply(lambda x: x['local_date'] + timedelta(hours=x['DELIVERY_HOUR'] - 1), axis=1)
        df_generation = df_generation.rename(columns={'MW': 'Value', 'REGION_ID': 'Region'})
        df_generation['Metric'] = 'Generation'
        df_generation = df_generation[['local_date', 'local_datetime', 'Product',
                                       'Metric', 'Region', 'Value']]
        self.df_generation = pd.concat([self.df_generation, df_generation])
    
    def format_df(self, df):
        df['Country'] = 'PHL'
        df['Region'] = df['Region'].apply(lambda x: x.title())
        df['Source'] = 'IEMOP'
        return df
        
    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_load_and_prices, self.df_generation])
        return df_dw
        
    def run_date(self, tdate):
        self.get_load_and_prices(tdate)
        self.df_load_and_prices = self.format_df(self.df_load_and_prices)
        logger.info(f'Philippines Demand and Prices data for {tdate} scraped')
        self.get_generation(tdate)
        self.df_generation = self.format_df(self.df_generation)
        logger.info(f'Philippines Generation data for {tdate} scraped')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    ph_scraper = factory.get_scraper_job('ph_iemop', 'philippines_historical_power_stats')
    tdate = datetime(2021,6,25)
    ph_scraper.run_date(tdate)
    ph_scraper.plot_all()
