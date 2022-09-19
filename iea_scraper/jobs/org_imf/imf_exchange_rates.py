# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 16:40:52 2021

@authors: Aloys, Matthew, Amani
"""

from iea_scraper.core.job import BaseJob
from iea_scraper.settings import EXT_DB_STR
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import sqlalchemy


class ImfExchangeRatesJob(BaseJob):
    '''
    This scraper gets data from the IMF on exchange rates
    '''
    title: str = "IMF - Exchange rates"
    def __init__(self):
        self.url_initial = 'https://www.imf.org/external/np/fin/ert/GUI/Pages/Report.aspx?CT=%27AUS%27,%27AUT%27,%27BEL%27,%27BWA%27,%27BRA%27,%27BRN%27,%27CAN%27,%27CHL%27,%27CHN%27,%27COL%27,%27CYP%27,%27CZE%27,%27DNK%27,%27EST%27,%27EMU%27,%27FIN%27,%27FRA%27,%27DEU%27,%27GRC%27,%27IND%27,%27IRL%27,%27ISR%27,%27ITA%27,%27JPN%27,%27KOR%27,%27KWT%27,%27LUX%27,%27MYS%27,%27MLT%27,%27MUS%27,%27MEX%27,%27NLD%27,%27NZL%27,%27NOR%27,%27OMN%27,%27PER%27,%27PHL%27,%27POL%27,%27PRT%27,%27QAT%27,%27RUS%27,%27SMR%27,%27SAU%27,%27SGP%27,%27SVK%27,%27SVN%27,%27ZAF%27,%27ESP%27,%27SWE%27,%27CHE%27,%27THA%27,%27TTO%27,%27URY%27,%27ARE%27,%27GBR%27,%27USA%27&EX=SDRC&P=DateRange&Fr=[date_from]&To=[date_to]&CF=Compressed&CUF=Period&DS=Ascending&DT=Blank'
        self.dates_to_scrape = pd.date_range(datetime.now() - timedelta(days=7),
                                             datetime.now() - timedelta(days=1))
        self.df_exchange_rates = None
        self.earliest_available_date = datetime(2008,1,1)
        self.last_available_date = datetime.now()
        
    def get_url(self, date_from, date_to):
        '''
        date_from : datetime
        date_to : datetime
        
        Returns
        str
        
        This method returns the url with the correct timestamps on fr and to arguments
        '''
        year_zero = datetime(2010,1,1)
        year_zero_timestamp = 633979008000000000
        from_diff = (date_from - year_zero)
        from_sec = int(from_diff.total_seconds() * 10**7) + year_zero_timestamp
        to_diff = (date_to - year_zero)
        to_sec = int(to_diff.total_seconds() * 10**7) + year_zero_timestamp     
        url = f'https://www.imf.org/external/np/fin/ert/GUI/Pages/Report.aspx?CT=%27AUS%27,%27AUT%27,%27BEL%27,%27BWA%27,%27BRA%27,%27BRN%27,%27CAN%27,%27CHL%27,%27CHN%27,%27COL%27,%27CYP%27,%27CZE%27,%27DNK%27,%27EST%27,%27EMU%27,%27FIN%27,%27FRA%27,%27DEU%27,%27GRC%27,%27IND%27,%27IRL%27,%27ISR%27,%27ITA%27,%27JPN%27,%27KOR%27,%27KWT%27,%27LUX%27,%27MYS%27,%27MLT%27,%27MUS%27,%27MEX%27,%27NLD%27,%27NZL%27,%27NOR%27,%27OMN%27,%27PER%27,%27PHL%27,%27POL%27,%27PRT%27,%27QAT%27,%27RUS%27,%27SMR%27,%27SAU%27,%27SGP%27,%27SVK%27,%27SVN%27,%27ZAF%27,%27ESP%27,%27SWE%27,%27CHE%27,%27THA%27,%27TTO%27,%27URY%27,%27ARE%27,%27GBR%27,%27USA%27&EX=SDRC&P=DateRange&Fr={from_sec}&To={to_sec}&CF=Compressed&CUF=Period&DS=Ascending&DT=Blank'
        return url
        
    def get_data_from_dates(self, date_from, date_to):
        '''
        date_from : datetime
        date_to : datetime
        
        Returns
        DataFrame
        
        Gets data in a dataframe
        Hint: use get_url and the solution code below
        '''
        
        url = self.get_url(date_from, date_to)
        s = requests.Session()
        r = s.get(url)
        soup = BeautifulSoup(r.text, features='lxml')
        a = soup.find('div', {'id': 'ctl00_ContentPlaceHolder2_ScrollDiv'})
        b = str(a).replace('colspan="100%"', '')
        df_raw_data = pd.read_html(b, skiprows=1, header=0)[0]
        return df_raw_data
        
    def unpivots_data(self, df_raw_data):
        '''
        df_raw_data : DataFrame
        
        Returns
        DataFrame
        
        Unpivots data. Output columns will be Date, Currency and Rate and version_date
        '''
        df_raw_unpivoted = df_raw_data.melt(id_vars = "Date",
                                            var_name = "Currency",
                                            value_name = "Rate")
        return df_raw_unpivoted        

    def formats_unpivoted_data(self, df_raw_unpivoted):
        '''
        df_raw_unpivoted : DataFrame with columns Date, Currency, Rate

        Returns
        DataFrame 
        
        Formats data to keep only rows with no NAs and rename currency with
        3 letters code (e.g. Uruguayan peso  (UYU) becomes UYU)

        '''
        
        df_formatted = df_raw_unpivoted.dropna().copy()
        df_formatted['Date'] = pd.to_datetime(df_formatted['Date'])
        df_formatted['Currency'] = df_formatted['Currency'].apply(lambda x: x[-4:-1])
        df_formatted['version_date'] = datetime.now()
        return df_formatted
    
    def to_csv(self, folder=None):
        ''' 
        df_formatted : DataFrame
        folder : string
        
        Saves formatted DataFrame in csv format in folder with name YYYYMMDD_IMFexchangerates.csv
        '''
        filename = datetime.now().strftime('%Y%m%d_IMFexchangerates.csv')
        filepath = os.path.join(folder, filename) if folder is not None else filename
        self.df_exchange_rates.to_csv(filepath, index=False)

        
    def send_data_to_database(self, df, db_str=EXT_DB_STR):
        '''
        df: DataFrame with columns Date, Currency, Rate, Version_date

        Sends df to the datawarehouse
        '''
        engine = sqlalchemy.create_engine(db_str, fast_executemany=True)
        df.to_sql(name='exchange_rates_data',
                                    con=engine,
                                    schema='edc',
                                    if_exists='append',
                                    index=False)
        
    def bulk_run(self, start_date, end_date, db_str=EXT_DB_STR):
        '''
        Scrapes data from start_date to end_date, fills self.df_exchange_rates
        and sends to data warehouse
        '''
        df_raw_data = self.get_data_from_dates(start_date, end_date)
        df_raw_unpivoted = self.unpivots_data(df_raw_data)
        self.df_exchange_rates = self.formats_unpivoted_data(df_raw_unpivoted)
        self.send_data_to_database(self.df_exchange_rates, db_str=db_str)
    
    def run(self):
        '''
        Scrapes data from self.dates_to_scrape, fills self.df_exchange_rates
        and sends to data warehouse
        '''
        self.bulk_run(self.dates_to_scrape[0], self.dates_to_scrape[-1])
        
        
if __name__ == '__main__':     
    exchange_rates = ImfExchangeRatesJob()
    exchange_rates.run()