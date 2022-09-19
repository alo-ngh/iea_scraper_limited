# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""
import numpy as np
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from datetime import datetime
import re

timezone = 'Pacific/Auckland'

import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class NzHourlyPowerStatsJob(EdcJob):
    title: str = 'TRANSPOWER.CO.NZ - New Zealand Hourly Power Statistics'

    def __init__(self):
        EdcJob.__init__(self)
        self.url = 'https://www.transpower.co.nz/power-system-live-data'
        self.data_type = {'Generation': 'soPgenGraph',
                          'Demand': 'soLoadDataGraph',
                          }
        self.region_mapping = {'niPoints': 'North Island',
                               'siPoints': 'South Island'}
        self.old_fuel_mapping = {'Wind': 'Wind Onshore',
                             'Hydro': 'Hydro',
                             'Gas/Coal': 'Thermal',
                             'Diesel/Oil': 'Oil',
                             'Gas': 'Natural Gas',
                             'Geothermal': 'Geothermal',
                             'Co-Gen': 'Cogeneration',
                             }
        self.fuel_mapping = {'Wind': 'Wind Onshore',
                             'Hydro': 'Hydro',
                             'Gas': 'Natural Gas',
                             'Co-Gen': 'Cogeneration',
                             'Geothermal':'Geothermal',
                             # 'Battery': np.nan,
                             'Coal': 'Coal',
                             'Gas': 'Natural Gas',
                             'Liquid': 'Oil',
                             # 'total': np.nan
                             }
        self.generation_data_points = []
        self.json_data = []

    def get_request_result(self):
        r = requests.get(self.url, verify=SSL_CERTIFICATE_PATH)
        soup = BeautifulSoup(r.text, 'html.parser')
        body = soup.find_all('script', text=re.compile('jQuery.extend'))[0].contents[0]
        json_data = json.loads(body.replace('jQuery.extend(Drupal.settings, ', '').replace(');', ''))
        self.json_data = json_data

    def get_generation(self):
        keys_list = list(self.json_data[self.data_type['Generation']].keys())
        timestamp = self.json_data[self.data_type['Generation']][f'{keys_list[2]}']
        tdate = datetime.fromtimestamp(int(timestamp))
        json_generation = self.json_data[self.data_type['Generation']][f'{keys_list[0]}']
        data_points = []
        for fuel in self.fuel_mapping.keys():
            try:
                productions = json_generation['New Zealand'][fuel]
                data_point = {
                    'local_datetime': tdate,
                    'local_date': tdate.date(),
                    'Country': 'New Zealand',
                    'Metric': 'Generation',
                    'Product': self.fuel_mapping[fuel],
                    'Value': productions['generation'],
                    'Source': 'transpower.co.nz',
                }
                data_points += [data_point]
            except:
                logging.info(f'no generation from  {fuel} in New Zealand')
                continue
        self.generation_data_points += data_points

    @property
    def df_generation(self):
        df_gen = pd.DataFrame(self.generation_data_points)
        df_gen = df_gen.groupby(['Product', 'local_datetime', 'local_date', 'Country', 'Metric',
                                 'Source'])['Value'].sum().to_frame().reset_index()
        return df_gen

    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_generation], axis=0)
        return df_dw

    def perform(self, folder):
        self.get_request_result()
        self.get_generation()
        self.to_csv(folder)

    def pre_run(self):
        self.get_request_result()
        self.get_generation()
        logger.info(f'{self.export_datetime} scraped for generation')
        logger.info('Pre run completed')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    nz_scraper = NzHourlyPowerStatsJob()
    nz_scraper.test_run(folder)
