import pandas as pd
from datetime import date, datetime, timedelta
import numpy as np
import logging
import sys

sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SingaporePowerStatsJob(EdcBulkJob):
    title: str = 'EMA - Singapore Power Statistics'
    """  
    There seems to only be prices and load available on EMC
    Generation is available monthly on EMA
    /!\ GENERATION is left aside (for now we are not scraping monthly data).
    """
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.df_load_daily = pd.DataFrame()
        self.df_prices_daily = pd.DataFrame()
        self.df_load = pd.DataFrame()
        self.df_prices = pd.DataFrame()

    def scrape_load_price_daily(self, tdate):
        """
        extract prices and load data from url for one datetime and updates the dataframes from init
        :param tdate: datetime
        :return: dataframes
        """
        self.df_load_daily = pd.DataFrame()
        self.df_prices_daily = pd.DataFrame()
        tdate_to_str_endpoint = datetime.strftime(tdate, '%d-%b-%Y')
        tdate_to_str_dataframe = datetime.strftime(tdate, '%Y-%m-%d')
        endpoint = f"priceinformation?doAccessData=true&USEP_accessAction=dataView&USEP_date={tdate_to_str_endpoint}&dataType=USEP"
        url = f"https://www.emcsg.com/marketdata/{endpoint}"
        #output can be checked against: https://www.emcsg.com/PriceInformation#download
        
        df_final_prices_table = pd.read_html(url)
        numberTables = df_final_prices_table.__len__()
        df_load_prices = df_final_prices_table[numberTables - 1]

        #convert date row to datetime then set it to the correct index
        for i, __ in df_load_prices.iterrows():
            minutes_add = ((df_load_prices.at[i, 'PERIOD'] * 30) - 30)
            df_load_prices.at[i, 'DATE'] = datetime.strptime(tdate_to_str_dataframe, '%Y-%m-%d') + timedelta(
                minutes=minutes_add.astype(np.float64))
        #df_load_prices['DATE'] = pd.to_datetime(df_load_prices['DATE'], format='%Y-%m-%d %H:%M')


        #convert date into utc (SG is UTC+8)
        df_load_prices['utc_datetime'] = pd.to_datetime(df_load_prices['DATE'], format='%Y-%m-%d %H:%M').dt.tz_localize('Asia/Singapore').dt.tz_convert('UTC')
        df_load_prices = df_load_prices.rename(columns={'DATE': 'local_datetime'})
        
        #for clarity and ease of manipulation, split into 2 df
        #load
        df_load_daily= df_load_prices[['utc_datetime', 'local_datetime', 'DEMAND (MW)']]
        df_load_daily= df_load_daily.rename(columns={'DEMAND (MW)': 'Value'}) 
        
        self.df_load_daily = df_load_daily

        #prices
        df_prices_daily = df_load_prices[['utc_datetime', 'local_datetime', 'USEP ($/MWh)']]
        df_prices_daily = df_prices_daily.rename(columns={'USEP ($/MWh)': 'Value'}) 

        self.df_prices_daily = df_prices_daily

        logger.info(f"Prices and Demand were scraped for {datetime.strftime(tdate, '%Y-%m-%d %H:%M')} in Singapore.")

    #def scrape_generation_daily(self, tdate):
    #    pass

    def scrape_load_price_period(self, date_start, date_end):
        """
        extracts price and load data for a range of dates and updates dataframes from init
        :param date_start: datetime
        :param date_end: datetime
        :return: dataframes
        """
     
        df_load_period = pd.DataFrame()
        df_prices_period = pd.DataFrame()
        for tdate in pd.date_range(date_start, date_end, freq='1D'):
            self.scrape_load_price_daily(tdate)
            df_load_period = pd.concat([df_load_period, self.df_load_daily], axis=0)
            df_prices_period = pd.concat([df_prices_period, self.df_prices_daily], axis=0)
        self.df_load = pd.concat([self.df_load, df_load_period], axis=0)
        self.df_prices = pd.concat([self.df_prices, df_prices_period], axis=0)


    #def scrape_generation_period(self, date_start, date_end):
    #    pass
    
    @property
    def offset_now(self):
        return 8

    @property
    def df_dw(self):

        df_load = self.df_load
        df_load['Country'] = 'Singapore'
        df_load['Metric'] = 'Demand'
        df_load['Product'] = 'ELE'
        df_load['Source'] = 'Energy Market Company'
        df_load['Flow 1'] = np.nan
        df_load['Flow 2'] = np.nan
        df_load= df_load[['utc_datetime', 'Country', 'Metric', 'Product', 'Source', 'Flow 1', 'Flow 2', 'Value']]

        df_prices = self.df_prices
        df_prices['Country'] = 'Singapore'
        df_prices['Metric'] = 'Prices'
        df_prices['Product'] = 'ELE'
        df_prices['Source'] = 'Energy Market Company'
        df_prices['Flow 1'] = 'Spot'
        df_prices['Flow 2'] = 'SGD'
        df_prices = df_prices[['utc_datetime', 'Country', 'Metric', 'Product', 'Source', 'Flow 1', 'Flow 2', 'Value']]

        df_dw = pd.concat([df_load, df_prices], axis=0)
        return df_dw

    def perform(self, date_start, date_end, folder):
        self.scrape_load_prices(date_start, date_end)
        #self.scrape_generation(date_start, date_end)
        self.to_csv(folder)

    def run_date(self, tdate):        
        #self.scrape_generation_period(scrap_day, scrap_day)
        self.scrape_load_price_period(tdate, tdate)

if __name__ == '__main__':
    folder =r'C:\Repos\world_electricity_scraper\csvs'
    sg_scraper = SingaporePowerStatsJob()
    sg_scraper.test_run(folder)
