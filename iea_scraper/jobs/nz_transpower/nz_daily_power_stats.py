# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""
import requests
import pandas as pd
# The request library is used to fetch content through HTTP
from datetime import datetime
import json
from bs4 import BeautifulSoup
import re

timezone = 'Pacific/Auckland'

import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob


class NzDailyPowerStatsJob(EdcJob):
    title: str = 'TRANSPOWER.CO.NZ - New Zealand Daily Power Statistics'
    
    def __init__(self):
        EdcJob.__init__(self)
        self.url = 'https://www.transpower.co.nz/power-system-live-data'
        self.data_type = {'Generation': 'soPgenGraph',
                          'Demand': 'soLoadDataGraph',
                          }
        self.region_mapping = {'niPoints': 'North Island',
                               'siPoints': 'South Island'}
        self.demand_data_points = []
        self.json_data = []

    def get_request_result(self):
        r = requests.get(self.url)
        soup = BeautifulSoup(r.text, 'html.parser')
        body = soup.find_all('script', text=re.compile('jQuery.extend'))[0].contents[0]
        json_data = json.loads(body.replace('jQuery.extend(Drupal.settings, ', '').replace(');', ''))
        self.json_data = json_data

    def get_demand(self):
        keys_list = list(self.json_data[self.data_type['Demand']].keys())
        json_demand = self.json_data[self.data_type['Demand']][f'{keys_list[3]}']
        data_points = []
        for region in self.region_mapping.keys():
            for item in json_demand[region]:
                tdate = datetime(int(item[0]['year']), int(item[0]['month']) + 1, int(item[0]['day']),
                                 int(item[0]['hour']), int(item[0]['minute']))
                data_point = {
                    'local_datetime': tdate,
                    'local_date': tdate.date(),
                    'Region': self.region_mapping[region],
                    'Country': 'New Zealand',
                    'Metric': 'Demand',
                    'Product': 'ELE',
                    'Value': item[1],
                    'Source': 'transpower.co.nz',
                }
                data_points += [data_point]
        self.demand_data_points += data_points

    @property
    def df_demand(self):
        return pd.DataFrame(self.demand_data_points)

    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_demand], axis=0)
        return df_dw

    def perform(self, folder):
        self.get_request_result()
        self.get_demand()
        self.to_csv(folder)

    def pre_run(self):
        self.get_request_result()
        self.get_demand()
        logger.info(f'{self.export_date} scraped for demand')
        logger.info('Pre run completed')


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    nz_scraper = NzDailyPowerStatsJob()
    nz_scraper.test_run(folder)
