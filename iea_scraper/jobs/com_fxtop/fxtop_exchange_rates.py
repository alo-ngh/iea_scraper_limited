# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 16:01:55 2021

@author: NGHIEM_A AL-SAIDI_A
"""

from iea_scraper.core.job import BaseJob
from iea_scraper.settings import EXT_DB_STR
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlalchemy
import logging
import locale


CURRENCIES = ['ARS', 'IRR', 'UAH', 'BGN', 'RON', 'TRY']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class FxtopExchangeRatesJob(BaseJob):
    '''
    This scraper gets data from the FXTOP on exchange rates
    '''
    title: str = "FXTOP - Exchange rates"
    def __init__(self):
        self.dates_to_scrape = pd.date_range(datetime.now() - timedelta(days=7),
                                             datetime.now() - timedelta(days=1))
        self.df_exchange_rates = pd.DataFrame()
        self.df_exchange_rates_currency = pd.DataFrame()
        self.earliest_available_date = datetime(2008,1,1) #needs to be updated
        self.last_available_date = datetime.now()
        self.currencies = CURRENCIES
        locale.setlocale(locale.LC_TIME, 'C')
    
    @property
    def bulk(self):
        return True
    
    def get_url(self, date_from, date_to, currency):
        '''
        date_from : datetime
        date_to : datetime
        currency : string
        
        Returns
        str
        
        This method returns the url with the correct timestamps on from and to arguments
        '''
        
        url = f'https://fxtop.com/en/historical-exchange-rates.php?A=1&C1={currency}&C2=XDR&TR=1&DD1={date_from:%d}&MM1={date_from:%m}&YYYY1={date_from:%Y}&B=1&P=&I=1&DD2={date_to:%d}&MM2={date_to:%m}&YYYY2={date_to:%Y}&btnOK=Go%21'
        return url
            
    def get_data_from_dates_and_currency(self, date_from, date_to, currency):
        '''
        date_from : datetime
        date_to : datetime
        currency : string
        
        Returns
        DataFrame
        
        Extract data as a dataframe
        '''
        url = self.get_url(date_from, date_to, currency)
        r = requests.get(url)
        soup = BeautifulSoup(r.text, features='lxml')
        tables = soup.find_all('table', {'border': "1"})
        table = tables[0]
        df_raw_exchange_rates = pd.read_html(str(table), header=0)[0]
        return df_raw_exchange_rates
    
    def format_dataframe(self, df_raw_exchange_rates, currency):
        '''
        df_raw_exchange_rates: DataFrame
        
        formats df coming out of the website to shape it with
        Output columns being Date, Currency and Rate and version_date
        '''
        df_formatted_exchange_rates = df_raw_exchange_rates.rename(columns={'Date':'date', 
                                                                            'Last':'rate', 
                                                                            currency + '/XDR': 'rate'})
        df_formatted_exchange_rates = df_formatted_exchange_rates[['date','rate']]
        df_formatted_exchange_rates['date'] = pd.to_datetime(df_formatted_exchange_rates['date'], format='%A %d %B %Y')
        df_formatted_exchange_rates['currency'] = currency
        df_formatted_exchange_rates['version_date'] = datetime.now()
        df_formatted_exchange_rates = df_formatted_exchange_rates.loc[~pd.isnull(df_formatted_exchange_rates['rate'])]
        return df_formatted_exchange_rates
    
    def send_data_to_database(self, df, db_str=EXT_DB_STR):
        '''
        df: DataFrame with columns Date, Currency, Rate, Version_date
        Sends df to the data warehouse
        '''
        engine = sqlalchemy.create_engine(db_str, fast_executemany=True)
        df.to_sql(name='exchange_rates_data',
                                    con=engine,
                                    schema='edc',
                                    if_exists='append',
                                    index=False)
        
    def to_csv(self, folder):
        ''' Sends self.df_exchange_rates_currency to folder as a csv'''
        csv_path = folder + '\\' + 'fxtop_exchanges_' + start_date.strftime("%Y_%m_%d") + '_' + end_date.strftime("%Y_%m_%d") + '.csv'
        self.df_exchange_rates.to_csv(csv_path, index=False)
            
    def bulk_run_currency(self, start_date, end_date, currency, db_str=EXT_DB_STR):
        '''
        Scrapes data from start_date to end_date, fills self.df_exchange_rates
        and sends to data warehouse for a given currency
        '''
        self.df_exchange_rates_currency = pd.DataFrame()
        period_timedelta = end_date - start_date
        number_of_periods = period_timedelta.days//365 + 1
        start_end_dates = [(start_date + timedelta(days=i*365),
                            min(start_date + timedelta(days=(i + 1) * 365 - 1), end_date))
                           for i in range(number_of_periods)]
        for start_date_p, end_date_p in start_end_dates:
            df_raw_data = self.get_data_from_dates_and_currency(start_date_p, end_date_p, currency)
            df_formatted_data = self.format_dataframe(df_raw_data, currency) 
            self.df_exchange_rates = pd.concat([self.df_exchange_rates, df_formatted_data])
            self.df_exchange_rates_currency = pd.concat([self.df_exchange_rates_currency, df_formatted_data])
            print(start_date_p, currency)
            if db_str is not None:
                self.send_data_to_database(df_formatted_data, db_str=db_str)
            
    def bulk_run(self, start_date, end_date, db_str=EXT_DB_STR):
        '''
        Scrapes data from start_date to end_date, fills self.df_exchange_rates
        and sends to data warehouse for all currencies
        '''
        for currency in CURRENCIES:
            self.bulk_run_currency(start_date, end_date, currency, db_str)
        
    def run(self):
        '''
        Scrapes data from self.dates_to_scrape, fills self.df_exchange_rates
        and sends to data warehouse
        '''
        for currency in CURRENCIES:
            self.bulk_run_currency(self.dates_to_scrape[0], self.dates_to_scrape[-1],
                                   currency)
        
        
if __name__ == '__main__':
    folder = r'C:\Repos\scraper\scraper\jobs\com_fxtop'
    start_date = datetime(2021,7,1)
    end_date = datetime(2021,8,1)
    currency = 'IRR'
    scraper = FxtopExchangeRatesJob()
    # scraper.bulk_run(start_date, end_date, db_str=None) #Not sending anything to DW
    # scraper.to_csv(folder) #Then open the csv and check it looks good  
    scraper.bulk_run(start_date, end_date, None)
    