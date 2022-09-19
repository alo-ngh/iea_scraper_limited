""" Description (collapsable)
    Russian scraper
    Hourly data for:
    > demand
    > generation per source/region 
    > prices
    Main Source: https://br.so-ups.ru

    Notes on Russian electricity system
    2 pricing zones
    - price zone 1 (European Russia and Ural)
    - price zone 2 (Siberia)
    - UES: unified energy system - zone 1 + 2
    3 non-pricing zones (where the state regulates prices)
    - Kaliningrad Oblast
    - Far East
    - Arkhangelsk Oblast and Komi Republic

    Notes on Russian power generation sources:
    - АЭС / NPP: Nuclear power plants
    - ГЭС / HPP: Hydroelectric power plants
    - ТЭС: Thermal power plants > can't determine the breakdown
    - БС: Unknown
    - ВИЭ: Renewables?
"""

import pandas as pd
from datetime import  timedelta
import logging
import sys
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
from iea_scraper.core import factory

class RussianPowerStatsUpsJob(EdcBulkJob):

    title: str = 'RU - Russian Demand, Gen/source and Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.data_dict = {
                'Demand': pd.DataFrame(),
                'Generation': pd.DataFrame(),
                'Prices': pd.DataFrame()
        }
        self.metrics = ['Demand', 'Generation', 'Prices']
        self.fuels_source_dict = {
                        'P_AES': 'Nuclear', 
                        'P_GES': 'Hydro', 
                        'P_TES': 'Thermal', 
                        'P_BS': 'Other',
                        'P_REN': 'Renewables'}
        self.power_systems_mapping = {
            530000: 'UES Center',
            550000: 'UES South',
            600000: 'UES of the Middle Volga',
            610000: 'UES Siberia',
            630000: 'UES Ural',
            840000: 'UES of the North-West' 
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
        Called by run_date()

        Input(s):
            None

        Output(s):
            df_dw [DataFrame]: DataFrame to be loaded in the DW

        Description:
            Selects relevant columns from df
        """

        df_dw = pd.concat([
            self.data_dict['Demand'],
            self.data_dict['Prices']])
        df_dw['Product'] = 'ELE'
        df_dw = df_dw.append(self.data_dict['Generation'])
        df_dw['Country'] = 'RUS'
        df_dw['Source'] = 'ATS ENERGO'
        return df_dw 

    def run_date(self, tdate):
        """
        Called by EdcBulkJob.pre_run()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            data_dict [Dict]: dictionary with a DataFrame assigned as a value to the metric key

        Description:
            - Executes the job for one day without all the primary checks and without loading into db on the current date
            - Collects the data and puts it in a DataFrame for each individual metric
            - Formats the DataFrame to fit the DW    

        Calls:
            get_data_prices()
            get_data_demand()
            get_data_generation()

        """
        self.get_data_prices(tdate)
        self.get_data_demand(tdate)
        self.get_data_generation(tdate)
        self.df_dw

    def get_data_prices(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            self.data_dict['Demand] [Dict]: dictionary with a DataFrame assigned as a value to the metric key

        Description:
            - connects to the api and collects the data from an *.xml file to a DataFrame
            - cleans the DataFrame and saves it in a dictionary
        """
        url_components = [
            'https://br.so-ups.ru/webapi/Public/Export/Xml/IndicatorBR.aspx?',
            'date=' + tdate.strftime("%Y.%m.%d"),
            '&territoriesIds=null:530000,null:550000,null:600000,null:610000,null:630000,null:840000&interval=DAY_INTERVAL'
        ]
        url =  ''.join(url_components)

        df_prices_raw = pd.read_xml(url)
        
        df_prices_raw['Region'] = df_prices_raw['POWER_SYS_ID'].map(self.power_systems_mapping)
        df_prices_raw['Value'] = df_prices_raw['AVERAGE_PRICE']
        df_prices_raw['local_datetime'] = tdate + timedelta(hours=1) * (df_prices_raw['INTERVAL'])
        df_prices_raw['Metric'] = 'Prices'
        df_prices_raw['Flow 2'] = 'RUB'

        df_prices = df_prices_raw[['local_datetime','Region', 'Value', 'Metric', 'Flow 2']].dropna().reset_index(drop=True)
        self.data_dict['Prices'] = self.data_dict['Prices'].append(df_prices)
    
    def get_data_demand(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            self.data_dict.Demand [Dict]: dictionary with a DataFrame assigned as a value to the metric key
            
        Description:
            - connects to the api and collects the data from an *.xml file to a DataFrame
            - cleans the DataFrame and saves it in a dictionary            
        """
        url_components = [
            'https://br.so-ups.ru/webapi/Public/Export/Xml/Consuming.aspx?',
            'dates=' + tdate.strftime("%Y.%m.%d"),
            '&priceZonesIds=1,2'
        ]
        url =  ''.join(url_components)

        df_demand_raw = pd.read_xml(url)
        df_demand_raw = df_demand_raw.rename(
            columns={
                'E_USE_FACT': 'Actual',
                'E_USE_PLAN': 'Forecast',
                'PRICE_ZONE_ID': 'Region'
            }
        )
        df_demand_raw['Region'].replace({
            '1': 'Zone 1',
            '2': 'Zone 2'
        })
        df_demand_raw['local_datetime'] = tdate + timedelta(hours=1) * (df_demand_raw['INTERVAL'])
        df_demand_raw = df_demand_raw.drop(
            columns=['INTERVAL', 'INTERVAL1', 'POWER_SYS_ID', 'M_DATE'])

        df_demand = pd.melt(df_demand_raw,
            id_vars = [
                'local_datetime',
                'Region'
                ],
            value_vars = [
                'Actual',
                'Forecast'
            ],
            var_name='Demand',
            value_name = 'Value'
                )
        df_demand = df_demand[df_demand['Demand'] == 'Actual'].copy().dropna().reset_index(drop=True)
        df_demand = df_demand.drop(
            columns = ['Demand']
        )
        df_demand['Metric'] = 'Demand'
        self.data_dict['Demand'] = self.data_dict['Demand'].append(df_demand)
    

    def get_data_generation(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected
            
        Output(s):
            self.data_dict['Demand] [Dict]: dictionary with a DataFrame assigned as a value to the metric key
            
        Description:
            - connects to the api and collects the data from an *.xml file to a DataFrame
            - cleans the DataFrame and saves it in a dictionary            
        """
         
        url_components = [
        'https://br.so-ups.ru/webapi/Public/Export/Xml/PowerGen.aspx?',
        'startDate=' + tdate.strftime("%Y.%m.%d"),
        '&endDate=' + tdate.strftime("%Y.%m.%d"),
        '&territoriesIds=-1:null,1:null,2:null,null:530000,null:550000,null:600000,null:610000,null:630000,null:840000&notCheckedColumnsNames='
        ]
        url =  ''.join(url_components)
        df_gen_raw = pd.read_xml(url)
        df_gen_raw['local_datetime'] = tdate + timedelta(hours=1) * (df_gen_raw['INTERVAL'])

        df_gen = pd.melt(df_gen_raw, 
                        id_vars = ['INTERVAL',
                                    'local_datetime',
                                    'PRICE_ZONE_ID'
                                    ],
                        value_vars = self.fuels_source_dict.keys(),
                        var_name='Fuel source',
                        value_name = 'Value').dropna().reset_index(drop=True)

        df_gen['Metric'] = 'Generation'
        df_gen['Product'] = df_gen['Fuel source'].map(self.fuels_source_dict)
        df_gen.drop(columns=['Fuel source', 'INTERVAL'], inplace=True)
        df_gen = df_gen.rename(columns={'PRICE_ZONE_ID': 'Region'}).replace({
            '1': 'Zone 1',
            '2': 'Zone 2'
        })
        self.data_dict['Generation'] = self.data_dict['Generation'].append(df_gen)

if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    russian_scraper = factory.get_scraper_job('ru_ups', 'russian_power_stats_ups')
    russian_scraper.test_run(folder)