import json
from datetime import datetime as dt, timedelta
import pandas as pd
import requests
from pandas import json_normalize
import os
import logging

import sys

logging.basicConfig(level=logging.INFO)
sys.path.append(r'C:\Repos\scraper')
logger = logging.getLogger(__name__)
from iea_scraper.core.job import EdcBulkJob


class TurkishPowerStatsJob(EdcBulkJob):

    title: str = 'EPIAS - Turkish Power Statistics'

    def __init__(self):
        super().__init__(self)
        self.link = 'https://seffaflik.epias.com.tr/transparency/service/'
        self.data_types = {'Generation': 'production/real-time-generation',
                           'Demand': 'consumption/real-time-consumption',
                           'Prices': 'market/day-ahead-mcp'}
        self.output_dfs = {'Generation': pd.DataFrame(),
                           'Demand': pd.DataFrame(),
                           'Prices': pd.DataFrame()}
        self.output_all_dfs = {'Generation': pd.DataFrame(),
                           'Demand': pd.DataFrame(),
                           'Prices': pd.DataFrame()}
        self.fuel_mapping = {'blackCoal': 'Hard Coal',
                             'asphaltiteCoal': 'Hard Coal',
                             'lignite': 'Brown Coal',
                             'importCoal': 'Coal',
                             'fueloil': 'Oil',
                             'gasOil': 'Oil',
                             'naphta': 'Oil',
                             'river': 'Hydro Run-of-river',
                             'dammedHydro': 'Hydro Reservoir',
                             'naturalGas': 'Natural Gas',
                             'lng': 'Natural Gas',
                             'nucklear': 'Nuclear',
                             'importExport': 'Net Exports',
                             'biomass': 'Biomass',
                             'sun': 'Solar',
                             'wind': 'Wind Onshore',
                             'geothermal': 'Geothermal',
                             }
        self.json_data = []

    @property
    def offset_now(self):
        return 1

    def get_request_result(self, query):
        url = self.link + query
        response = requests.request("GET", url)
        json_data = json.loads(response.text.encode('utf8'))
        keys_list = list(json_data['body'].keys())
        key_name = keys_list[0]
        self.json_data = json_data['body'][f'{key_name}']

    def get_data(self, date_start, date_end, data_type):
        if data_type not in ['Generation', 'Demand', 'Prices']:
            raise KeyError("data_type has to  be in ['Generation', 'Demand', 'Prices']")
        query = self.data_types[data_type] + "?startDate=" + f'{date_start.strftime("%Y-%m-%d")}' + \
                "&endDate=" + f'{date_end.strftime("%Y-%m-%d")}'
        self.get_request_result(query)
        self.output_dfs[data_type] = json_normalize(self.json_data)
        self.format_data_type[data_type]()
        self.output_all_dfs[data_type] = pd.concat([self.output_dfs[data_type].copy(), self.output_all_dfs[data_type]])
        logger.info(f'{date_start.date()} to {date_end.date()} was scraped for {data_type}')

    @property
    def format_data_type(self):
        return {'Generation': self.format_generation,
                'Demand': self.format_demand,
                'Prices': self.format_prices}

    def format_generation(self):
        df_gen = self.output_dfs['Generation']
        df_gen = df_gen.rename(columns={'date': 'local_datetime'})
        df_gen = pd.melt(df_gen, id_vars=['local_datetime'],
                         value_vars=[fuel for fuel in self.fuel_mapping.keys()],
                         var_name='Product', value_name='Value')
        df_gen['Product'] = df_gen['Product'].apply(lambda x: self.fuel_mapping[x])
        df_gen = df_gen.groupby(['Product', 'local_datetime'])['Value'].sum().to_frame().reset_index()
        df_gen = df_gen[df_gen['Product'] != 'Net Exports']
        df_gen['Metric'] = 'Generation'
        df_gen = self.format_df(df_gen)
        self.output_dfs['Generation'] = df_gen

    def format_demand(self):
        df_load = self.output_dfs['Demand']
        df_load = df_load.rename(columns={'date': 'local_datetime',
                                          'consumption': 'Value'})
        df_load['Metric'] = 'Demand'
        df_load['Product'] = 'ELE'
        df_load = self.format_df(df_load)
        self.output_dfs['Demand'] = df_load

    def format_prices(self):
        df_prices = self.output_dfs['Prices']
        df_prices = df_prices.rename(columns={'date': 'local_datetime',
                                              'price': 'TRY',
                                              'priceUsd': 'USD',
                                              'priceEur': 'EUR'
                                              })
        df_prices = pd.melt(df_prices, id_vars=['local_datetime'],
                            value_vars=['TRY'], #used to be ['TRY', 'EUR', 'USD']
                            var_name='Flow 2', value_name='Value')
        df_prices['Metric'] = 'Prices'
        df_prices['Product'] = 'ELE'
        df_prices['Flow 1'] = 'Spot'
        df_prices = self.format_df(df_prices)
        self.output_dfs['Prices'] = df_prices

    def format_df(self, df):
        df['local_date'] = pd.to_datetime(df["local_datetime"]).dt.date
        df['local_datetime'] = pd.to_datetime(df["local_datetime"]).dt.strftime('%Y-%m-%d %H:%M')
        df['Export Date'] = self.export_date.strftime('%Y-%m-%d %H:%M')
        df['Country'] = 'Turkey'
        df['Source'] = 'EPIAS'
        df = df.reset_index(drop=True)
        return df

    def perform(self, folder, date_start, date_end):
        date_to_scrape = self.export_date - timedelta(days=1)
        for data_type in self.data_types:
            self.get_data(date_to_scrape, date_to_scrape, data_type)
        self.to_csv(folder)

    @property
    def df_dw(self):
        df_dw = pd.concat(self.output_all_dfs.values(), axis=0)
        return df_dw

    def run_date(self, tdate):
        for data_type in self.data_types:
            self.get_data(tdate, tdate, data_type)


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    epias_scraper = TurkishPowerStatsJob()
    epias_scraper.test_run(folder)
