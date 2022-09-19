# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 14:14:49 2020

@author: TAV_M
"""
import pandas as pd
import sys
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
import logging



logger = logging.getLogger(__name__)


class JapanesePricesStatsJob(EdcBulkJob):

    title: str = 'JEPX.ORG - Japanese Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.all_data_points = []
        self.df_prices_day = pd.DataFrame()
        self.df_prices_all = pd.DataFrame()
        
        
    def format_df(self):
        df_prices = self.df_prices_day
        df_prices.columns = ["local_datetime", "Row Number", "Value", "X"]
        df_prices['local_datetime'] = pd.to_datetime(df_prices['local_datetime'], format='%Y/%m/%d')
        # Localtime. 1st row is 00h00, 2nd row is 00h30, etc...  
        df_prices['local_datetime'] +=  pd.to_timedelta((df_prices['Row Number']-1)/2, unit='h')
        # Convert into UTC Date. #Japan is UTC+9
        df_prices['utc_datetime'] = df_prices['local_datetime'].dt.tz_localize('Asia/Tokyo').dt.tz_convert('UTC')
        df_prices['Export Date'] = self.export_date
        df_prices['Country'] = 'Japan'
        df_prices['Value'] = df_prices['Value'] * 1000
        df_prices['Metric'] = 'Prices'
        df_prices['Product'] = 'ELE'
        df_prices['Source'] = 'jepx.org'
        df_prices['Flow 1'] = 'Spot'
        df_prices['Flow 2'] = 'JPY'
        df_prices = df_prices.drop(columns=['Row Number','X'])
        self.df_prices_all = pd.concat([self.df_prices_all, df_prices])
    
    @property  
    def df_dw(self):
        df_dw = self.df_prices_all
        return df_dw
    
    def scrape_jepx_csvs(self, date_start, date_end):
        for date in pd.date_range(date_start,date_end):
            self.scrape_jepx_csv(date)
        
    def scrape_jepx_csv(self, date):
        csv_url = 'http://www.jepx.org/data/'+date.strftime('%Y%m%d')+'.csv'
        self.df_prices_day = pd.read_csv(csv_url)
        self.format_df()
        logger.info(f'{date.date()} was scraped')

    @property
    def offset_now(self):
        return 2
    
    def run_date(self,tdate):
        self.scrape_jepx_csv(tdate)
        
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs' 
    japanese_prices_stats = JapanesePricesStatsJob()
    japanese_prices_stats.test_run(folder, historical=False)
    
