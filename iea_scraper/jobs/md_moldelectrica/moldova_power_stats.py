#-*- coding: utf-8 -*-
"""
Created on Tue Oct 19, 2021
``
@author: CHAMBEAU_L

Moldavian power data:
- Power Generation per fuel source
- Power Demand

"""
import requests
import pandas as pd
import logging
import sys
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core import factory

class MoldovaPowerStatsJob(EdcBulkJob):
    
    title: str = 'MOLDELECTRICA - Modova Power Generation/Consumption statistics'
    
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.r = requests.session()
        self.dict_id = {
            'Demand': 1,
            'Generation': 5
        }
        self.df_gen = pd.DataFrame()
        self.df_cons = pd.DataFrame()

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
            Updates columns with relevant information for generation and demand
        """

        df_dw = pd.DataFrame()
        df_gen = self.df_gen
        df_cons = self.df_cons

        df_gen['Metric'] = 'Generation'

        df_cons['Product'] = 'ELE'
        df_cons['Metric'] = 'Demand'

        df_dw = pd.concat([df_gen, df_cons])
        df_dw['Country'] = 'Moldova'
        df_dw['Source'] = 'ENERGO'

        return df_dw
    
        
    def run_date(self, tdate):
        """
        Called by EdcBulkJob.pre_run()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            df_gen [DataFrame]: Dataframe with generation data.
            df_cons [DataFrame]: Dataframe with consumption data.

        Description:
            - Executes the job for one day without all the primary checks and without loading into db on the current date
            - Requests the data for both generation and demand at the same time
            - Parses the data into DataFrames

        Calls:
            get_data_generation()
            get_df_generation()
            get_data_demand()
            get_df_demand()
        """
       
        data_generation = self.get_data_generation(tdate)
        self.get_df_generation(data_generation)
        data_demand = self.get_data_demand(tdate)
        self.get_df_demand(data_demand)
        self.df_dw

    def get_url(self, metric, tdate):
        """
        Called by EdcBulkJob.pre_run()

        Input(s):
            metric [Str]: metric to be collected
            tdate [datetime]: date to be collected

        Output(s):
            [Str]: url to be queried

        Description:
            - Uses the appropriate table ID and date to build the url to query
        """        
        url_parameters = {
            'base_url': 'https://moldelectrica.md/utils/archive2.php?id=',
            'id': str(self.dict_id[metric]),
            'parameters': '&type=json&lang=ru&date1=',
            'date1': tdate.strftime("%d.%m.%Y"),
            'to': '&date2=',
            'date2': tdate.strftime("%d.%m.%Y")
        }

        return ''.join([url_parameters[key] for key in url_parameters.keys()]) 

    def get_data_generation(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            rep['result'] [Dict]: Dictionary with data collected.

        Description:
            - Sends a query to the website with relevant arguments (date etc)
            - Requests the data for generation
        """
        
        url_generation = self.get_url('Generation', tdate)

        rep = requests.get(url_generation, verify=False).json()

        return rep['result']

    def get_data_demand(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            rep['result'] [Dict]: Dictionary with data collected.

        Description:
            - Sends a query to the website with relevant arguments (date etc)
            - Requests the data for demand
        """

        url_demand = self.get_url('Demand', tdate)

        rep = requests.get(url_demand, verify=False).json()

        return rep['result']

    def get_df_generation(self, data_generation):
        """
        Called by run_date()

        Input(s):
            data_generation [Dict]: dictionary with data collected from the request

        Output(s):
            df_gen [DataFrame]: Dataframe with generation data.

        Description:
            - Reads the dictionary for each 5 min period in the day to build a dictionary with the data
            - Creates a dataframe
            - Formats the dataframe
        """
        
        data_day = []
        for row in data_generation:
            data_row = {}
            data_row['local_datetime'] = datetime.strptime(row['d'], "%d.%m.%Y %H:%M:00")
            data_row['Thermal'] = row['values'][0]
            data_row['Hydro'] = row['values'][1]
            data_row['Renewables'] = row['values'][2]
            data_day += [data_row]

        df_gen = pd.DataFrame(data_day).melt(
                    id_vars=['local_datetime'],
                    value_vars=['Thermal', 'Hydro', 'Renewables'],
                    var_name='Product',
                    value_name='Value'
        )

        self.df_gen = self.df_gen.append(df_gen)
        
    def get_df_demand(self, data_demand):
        """
        Called by run_date()

        Input(s):
            data_demand [Dict]: dictionary with data collected from the request

        Output(s):
            df_cons [DataFrame]: Dataframe with demand data.

        Description:
            - Reads the dictionary for each 5 min period in the day to build a dictionary with the data
            - Creates a dataframe
            - Formats the dataframe
        """
        data_day = []
        for row in data_demand:
            data_row = {}
            data_row['local_datetime'] = datetime.strptime(row['d'], "%d.%m.%Y %H:%M:00")
            data_row['Value'] = row['values'][0]
            data_day += [data_row]

        df_cons = pd.DataFrame(data_day)

        self.df_cons = self.df_cons.append(df_cons)

if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper_moldova = MoldovaPowerStatsJob()
    scraper_moldova.test_run(folder)
    scraper_moldova = factory.get_scraper_job('md_moldelectrica', 'moldova_power_stats')
