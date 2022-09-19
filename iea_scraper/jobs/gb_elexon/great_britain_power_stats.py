#%%
"""
Created on Feb 2021
``
@author: CHAMBEAU_L
"""
import requests
import pandas as pd
import xml.etree.ElementTree as et
from datetime import datetime, timedelta
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob


class GreatBritainPowerStatsJob(EdcBulkJob):
    title: str = 'ELEXON - GB Demand/Generation/Prices'

    def __init__(self):
        super().__init__(self)

        self.api_features = {
            'host_address': 'https://api.bmreports.com/BMRS',
            'report_name': '',
            'api_key': 'ejmg8dbm1snl65u',
        }

        self.metrics = {
            'Demand': 'B0610',
            'Generation': 'B1620',
            'Prices': 'MID'
        }

        self.df_dict = {
            'Demand': pd.DataFrame(),
            'Generation': pd.DataFrame(),
            'Prices': pd.DataFrame()
        }

        self.fuels = {
            'Biomass': 'Biomass',
            'Hydro Pumped Storage': 'Hydro Pumped Storage',
            'Hydro Run-of-river and poundage': 'Hydro Run-of-river',
            'Fossil Hard coal': 'Hard Coal',
            'Fossil Gas': 'Natural Gas',
            'Fossil Oil': 'Oil',
            'Nuclear': 'Nuclear',
            'Other': 'Other',
            'Wind Onshore': 'Wind Onshore',
            'Wind Offshore': 'Wind Offshore',
            'Solar': 'Solar'
        }

    @property
    def offset_now(self):
        """
        Description:
            Latest available data is the day before today
        """

        return 1

    @property
    def df_dw(self):
        """
        Description:
            Aggregates the different dataframes and formats them to fit the DW. 

        Called by run_date()

        Output(s):
            df_dw [DataFrame]: DataFrame to be loaded in the DW
        """

        df_dw = pd.DataFrame()

        df_dw = pd.concat([self.df_dict['Demand'], self.df_dict['Prices']])
        df_dw['Product'] = 'ELE'
        df_dw = df_dw.append(self.df_dict['Generation'])
        df_dw['Country'] = 'GBR' #note: using GBR( iso3 UK) even if coverage is only Great Britain
        df_dw['Source'] = 'Elexon - BMRS'
        df_dw['Region'] = 'Great Britain'
        return df_dw

    def run_date(self, tdate):
        """
         Description:
            Executes the job for one day without all the primary checks and without loading into db on the current date
            Collects the data and puts it in a DataFrame for each metric.
            Pre-formats the DataFrame to fit the DW for each metric.
            
        Called by EdcBulkJob.pre_run()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_dw [DataFrame]: DataFrame formatted for the DW

        Calls:
            run_date_demand()
            run_date_generation()
            run_date_prices()
        """
        
        self.run_date_demand(tdate)
        self.run_date_generation(tdate)
        self.run_date_prices(tdate)


    def run_date_demand(self, tdate):
        """
         Description:
            Executes the job for one day without all the primary checks and without loading into db on the current date
            Collects the data and puts it in a DataFrame for demand.
            Pre-formats the DataFrame to fit the DW for demand.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_dict['Demand'] [Dict]: Dict with data formatted for the DW

        Calls:
            get_demand_to_df()
            format_demand()
        """        

        df = self.get_demand_to_df(tdate)
        self.format_demand(tdate, df)
        

    def run_date_generation(self, tdate):
        """
         Description:
            Executes the job for one day without all the primary checks and without loading into db on the current date
            Collects the data and puts it in a DataFrame for generation.
            Pre-formats the DataFrame to fit the DW for generation.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_dict['Generation'] [Dict]: Dict with data formatted for the DW

        Calls:
            get_generation_to_df()
            format_generation()
        """  

        df = self.get_generation_to_df(tdate)
        self.format_generation(tdate, df)

    def run_date_prices(self, tdate):
        """
         Description:
            Executes the job for one day without all the primary checks and without loading into db on the current date
            Collects the data and puts it in a DataFrame for prices.
            Pre-formats the DataFrame to fit the DW for prices.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_dict['Prices'] [Dict]: Dict with data formatted for the DW

        Calls:
            get_prices_to_df()
            format_prices()
        """  

        df = self.get_prices_to_df(tdate)
        self.format_prices(tdate, df)

    def get_demand_to_df(self, tdate):
        """
         Description:
            Collects the data and puts it in a DataFrame for demand.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_demand [DataFrame]: dictionary with the dataframe with the data for demand.
        """

        metric = 'Demand'
        settlement_date = tdate.strftime('%Y-%m-%d')
        input_url = f"{self.api_features['host_address']}/{self.metrics[metric]}/v1?APIKey={self.api_features['api_key']}&SettlementDate={settlement_date}&Period=*&ServiceType=csv"

        df_demand = pd.read_csv(input_url, skiprows=4)

        return df_demand

    def get_generation_to_df(self, tdate):
        """
         Description:
            Collects the data and puts it in a DataFrame for generation.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_generation [DataFrame]: dictionary with the dataframe with the data for generation.
        """

        metric = 'Generation'
        settlement_date = tdate.strftime('%Y-%m-%d')
        input_url = f"{self.api_features['host_address']}/{self.metrics[metric]}/v1?APIKey={self.api_features['api_key']}&SettlementDate={settlement_date}&Period=*&ServiceType=csv"

        df_generation = pd.read_csv(input_url, skiprows=4)

        return df_generation
        

    def get_prices_to_df(self, tdate):
        """
         Description:
            Collects the data and puts it in a DataFrame for prices.
            
        Called by run_date()

        Input(s):
            tdate [datetime]

        Output(s):          
            df_prices [DataFrame]: dictionary with the dataframe with the data for prices.
        """

        metric = 'Prices'
        settlement_date = tdate.strftime('%Y-%m-%d')
        input_url = f"{self.api_features['host_address']}/{self.metrics[metric]}/v1?APIKey={self.api_features['api_key']}&FromSettlementDate={settlement_date}&ToSettlementDate={settlement_date}&Period=*&ServiceType=csv"

        df_prices = pd.read_csv(input_url, header=None,
                                            names=['MID', 'Provider', 'Date', 'Period', 'GBP', 'Volume'],
                                            skiprows=1)
        return df_prices

    def format_demand(self, tdate, df_demand):
        """  
        Description:
            Formats the demand dataframe

        Called by run_date()

        Output(s):          
            df_dict [Dict]: dictionary with demand data
        """

        df_demand = df_demand.loc[:, ['SettlementPeriod', 'Quantity']].copy()
        df_demand.rename(columns={'Quantity': 'Value'}, inplace=True)
        df_demand['local_datetime'] = tdate + timedelta(hours=0.5) * (df_demand['SettlementPeriod'] - 1)
        df_demand.drop(['SettlementPeriod'], axis=1, inplace=True)
        df_demand['Metric'] = 'Demand'

        self.df_dict['Demand'] = pd.concat([self.df_dict['Demand'], df_demand.dropna()])

    def format_generation(self, tdate, df_generation):
        """  
        Description:
            Formats the generation dataframe

        Called by run_date()

        Output(s):          
            df_dict [DataFrame]: dictionary with generation data
        """

        df_generation = df_generation.loc[:, ['Settlement Period', 'Quantity', 'Power System Resource  Type']].copy()
        df_generation.rename(columns={'Quantity': 'Value', 'Power System Resource  Type': 'Product'}, inplace=True)
        df_generation['local_datetime'] = tdate + timedelta(hours=0.5) * (df_generation['Settlement Period'] - 1)
        df_generation.drop(['Settlement Period'], axis=1, inplace=True)
        df_generation['Product'] = df_generation['Product'].map(self.fuels)
        df_generation['Metric'] = 'Generation'

        self.df_dict['Generation'] = pd.concat([self.df_dict['Generation'], df_generation.dropna()])

    def format_prices(self, tdate, df):
        """  
        Description:
            Formats the prices dataframe

        Called by run_date()

        Output(s):          
            df_prices [Dict]: dictionary with prices data
        """


        df_prices_GBP = df[df['Provider'] == 'APXMIDP'].copy() #one out of the two providers contains the data we want
        df_prices_GBP['local_datetime'] = tdate + timedelta(hours=0.5) * (df_prices_GBP['Period'] - 1)
        df_prices = pd.melt(df_prices_GBP, id_vars=['local_datetime'], value_vars=['GBP'], var_name='Flow 2',
                            value_name='Value')
        df_prices['Metric'] = 'Prices'

        self.df_dict['Prices'] = pd.concat([self.df_dict['Prices'], df_prices.dropna()])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper = GreatBritainPowerStatsJob()
    scraper.test_run()
