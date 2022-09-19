# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: DAUGY_M, SILVA_M
"""
from time import sleep

import pandas as pd
from datetime import datetime
import sys
import logging
import requests
import json

from iea_scraper.core.exceptions import EdcJobError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob

from iea_scraper.settings import SSL_CERTIFICATE_PATH, BROWSERDRIVER_PATH

sys.path.append(BROWSERDRIVER_PATH)


class UsaPjmPricesStatsJob(EdcBulkJob):
    title: str = 'USA.PJM - American Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.hub_list = {'EASTERN HUB': 'East Pennsylvnaia',
                         # 'WEST INT HUB':np.nan,
                         'WESTERN HUB': 'West Pennsylvania',
                         'NEW JERSEY HUB': 'New Jersey',
                         # 'CHICAGO GEN HUB': np.nan,
                         'CHICAGO HUB': 'Chicago',
                         'N ILLINOIS HUB': 'North Illinois',
                         'AEP GEN HUB': 'American Electric Power',
                         'AEP-DAYTON HUB': 'American Electric Power Dayton',
                         'OHIO HUB': 'Ohio',
                         'DOMINION HUB': 'Virginia',
                         'ATSI GEN HUB': 'American Transmission System Inc.'}
        self.headers = {'Host': 'api.pjm.com',
                        'Ocp-Apim-Subscription-Key': 'b2621f9a5e6f48fdb184983d55f239ba', #Note: if scraper crashes in the future you might need to update this key
                        'Origin': 'https://dataminer2.pjm.com',
                        'Referer': 'https://dataminer2.pjm.com/'}
        self.request_url = 'https://api.pjm.com/api/v1/da_hrl_lmps?'
        self.df_prices = pd.DataFrame()
        self.json_items = {}

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.df_prices
        return df_dw

    def get_data(self, tdate):
        '''
        This method extracts the data from the PJM website using a GET request.
        We add the query string to the request_url. This query contains the columns we want in 'field' and the
        date range in 'datetime_beginning_ept'
        The data is collected in json format and we extract the value under the key 'items'
        :return: dictionary
        '''
        date_formatted = '{dt.month}/{dt.day}/{dt.year}'.format(dt=tdate).replace('/', '%2F')
        query = "RowCount=50000" \
                "&sort=datetime_beginning_ept" \
                "&order=Asc" \
                "&startRow=1" \
                "&isActiveMetadata=true" \
                "&fields=datetime_beginning_ept%2Cdatetime_beginning_utc%2Cpnode_name%2Ctype%2Ctotal_lmp_da" \
                "&Type=HUB" \
                f"&datetime_beginning_ept={date_formatted}%2000:00to{date_formatted}%2023:59" \
                f"&row_is_current=1"
        r = requests.get(self.request_url + query, verify=SSL_CERTIFICATE_PATH, headers=self.headers)
        if r.status_code == 200:
            json_data = json.loads(r.text)
            self.json_items = json_data['items']
        else:
            raise EdcJobError(f"USA PJM: prices data not available for {tdate.date()}")

    def format_data(self):
        df = pd.DataFrame(self.json_items)
        df = df.loc[df['pnode_name'].isin(self.hub_list)]
        df = df.rename(columns={'datetime_beginning_utc': 'utc_datetime', 'datetime_beginning_ept': 'local_datetime',
                                'pnode_name': 'Flow 4', 'total_lmp_da': 'Value'})
        df['utc_datetime'] = pd.to_datetime(df['utc_datetime'])
        df['utc_date'] = df['utc_datetime'].dt.date
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df['Flow 4'] = df['Flow 4'].map(self.hub_list)
        df = df[['utc_datetime', 'local_datetime', 'Flow 4', 'Value']]
        df = df.dropna()
        df['Country'] = 'United States'
        df['Region'] = 'Mid-Atlantic'
        df['Metric'] = 'Prices'
        df['Source'] = 'PJM'
        df['Product'] = 'ELE'
        df['Flow 2'] = 'USD'
        self.df_prices = pd.concat([df, self.df_prices])

    def run_date(self, tdate):
        self.get_data(tdate)
        self.format_data()
        logger.info(f'Prices scraped for {tdate.date()}')


if __name__ == '__main__':
    usa_prices_stats = UsaPjmPricesStatsJob()
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    tdate = datetime(2021, 9, 6)
    usa_prices_stats.test_run(folder, historical=True)
