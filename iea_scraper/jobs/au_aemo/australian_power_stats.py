"""
Created on Tue 24 November 2020

@authors:
    DAUGY_M
@reviewed by:
    Aloys
"""

from datetime import datetime as dt, timedelta
import pandas as pd
from urllib.request import Request, urlopen
import os
import logging
import numpy as np
import requests
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import sys

sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class AustralianPowerStatsJob(EdcBulkJob):
    title: str = 'AEMO.AU - Australian Power Statistics'

    def __init__(self):
        super().__init__(self)
        self.link = 'https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_'
        self.region_mapping = {
            'New South Wales': 'NSW1',
            'Queensland': 'QLD1',
            'South Australia': 'SA1',
            'Tasmania': 'TAS1',
            'Victoria': 'VIC1'
        }
        self.reverse_region_mapping = {code: region for region, code in
                                       self.region_mapping.items()}
        self.output_all_dfs = pd.DataFrame()

    @property
    def offset_now(self):
        return 1

    def get_csv(self, tdate, region):
        """extracts csv from link"""

        link = {'Date': tdate.strftime('%Y%m'),
                'Region': self.region_mapping[region]}
        url = self.link + '_'.join([value for value in link.values()]) + '.csv'
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0)'
                                   'Gecko/20100101 Firefox/77.0'}
        r = requests.get(url, headers=headers, verify=False)
        s = r.content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        return df

    def format_df(self, df):
        df = df.rename(columns={'REGION': 'Region',
                                'SETTLEMENTDATE': 'local_datetime',
                                'TOTALDEMAND': 'Demand',
                                'RRP': 'Prices'
                                })
        df = df.drop(['PERIODTYPE'], axis=1)
        df = pd.melt(df, id_vars=['Region', 'local_datetime'], value_vars=['Demand', 'Prices'], var_name='Metric',
                     value_name='Value')
        df["local_datetime"] = pd.to_datetime(df["local_datetime"])
        df["local_date"] = pd.to_datetime(df["local_datetime"]).dt.date
        df['Export Date'] = self.export_date
        df['Country'] = 'Australia'
        df['Source'] = 'AEMO NEM'
        df['Product'] = 'ELE'
        df['Region'] = df['Region'].apply(lambda x: self.reverse_region_mapping[x])
        df["Flow 1"] = np.nan
        df.loc[df["Metric"] == "Prices", "Flow 1"] = "Spot"
        df["Flow 2"] = np.nan
        df.loc[df["Metric"] == "Prices", "Flow 2"] = "AUD"
        df = df.reset_index(drop=True)
        return df

    def get_day(self, tdate):
        for region in self.region_mapping.keys():
            output_df = self.get_csv(tdate, region)
            output_df = self.format_df(output_df)
            output_df = output_df.loc[output_df['local_date'] == tdate.date()]
            self.output_all_dfs = pd.concat([self.output_all_dfs, output_df.copy()])
        logger.info(f'Prices and demand dataFrame created for {tdate.date()}')

    def get_bulk(self, date_start, date_end):
        for tdate in pd.date_range(date_start, date_end, freq='M'):
            self.get_day(tdate)

    @property
    def df_load(self):
        df_load = self.output_all_dfs.loc[self.df_dw['Metric'] == 'Demand']
        return df_load

    @property
    def df_prices(self):
        df_prices = self.output_all_dfs.loc[self.df_dw['Metric'] == 'Prices']
        return df_prices

    @property
    def df_dw(self):
        df_dw = self.output_all_dfs
        return df_dw

    def run_date(self, tdate):
        self.get_day(tdate)
        logger.info(f'{tdate.date()} scraped for Prices and Demand')



if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    nem_scraper = AustralianPowerStatsJob()
    nem_scraper.test_run(folder)
