# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: DAUGY_M
"""
import io
from zipfile import ZipFile
import pandas as pd
from datetime import datetime
import sys
import logging
import requests

from iea_scraper.core import factory

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS, BROWSERDRIVER_PATH

sys.path.append(BROWSERDRIVER_PATH)


class UsaNyisoPricesStatsJob(EdcBulkJob):
    title: str = 'USA.NYISO - American Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.hub_list = {'CAPITL': 'Capital Area',
                         'CENTRL': 'Central Area',
                         'DUNWOD': 'Dunwoodie',
                         'GENESE': 'Genesee',
                         'H Q': 'Hydro Quebec Intertie',
                         'HUD VL': 'Hudson Valley',
                         'LONGIL': 'Long Island',
                         'MHK VL': 'Mohawk Valley',
                         'MILLWD': 'Millwood',
                         'N.Y.C.': 'New York City',
                         'NORTH': 'North Area',
                         'NPX': 'ISO-NE Intertie',
                         'O H': 'IESO Intertie',
                         'PJM': 'PJM Intertie',
                         'WEST': 'West Area'}
        self.rt_url = 'http://mis.nyiso.com/public/csv/damlbmp/%Y%m%ddamlbmp_zone.csv'
        self.archive_url = 'http://mis.nyiso.com/public/P-2Alist.htm'
        self.zip_url = 'http://mis.nyiso.com/public/csv/damlbmp/%Y%m01damlbmp_zone_csv.zip'
        self.df_prices = pd.DataFrame()

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.df_prices
        return df_dw

    def get_data_from_zip(self, tdate):
        '''
        the data is stored in zip archives on the following webpage: http://mis.nyiso.com/public/P-2Alist.htm
        each monthly archive contains all daily csvs
        @param tdate: datetime
        :return: DataFrame
        '''
        url = tdate.strftime(self.zip_url)

        r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        z = ZipFile(io.BytesIO(r.content))
        all_files = z.namelist()
        tdate_file = [file for file in all_files if tdate.strftime("%Y%m%d") in file][0]
        tdate_csv = z.open(tdate_file)
        df = pd.read_csv(tdate_csv)
        return df

    def format_data(self, df):
        df = df.rename(columns={'Time Stamp':'local_datetime', 'Name':'Flow 4', 'LBMP ($/MWHr)':'Value'})
        df = df[['local_datetime','Flow 4','Value']]
        df['Flow 4'] = df['Flow 4'].map(self.hub_list)
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df['utc_datetime'] = pd.to_datetime(df['local_datetime']).dt.tz_localize('America/New_York',
                                                                                 nonexistent='shift_forward',
                                                                                 ambiguous='NaT').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Country'] = 'United States'
        df['Region'] = 'New York'
        df['Source'] = 'NYISO'
        df['Metric'] = 'Prices'
        df['Flow 2'] = 'USD'
        df['Product'] = 'ELE'
        df['Export Date'] = self.export_datetime
        self.df_prices = pd.concat([df, self.df_prices])

    def run_date(self, tdate):
        df = self.get_data_from_zip(tdate)
        self.format_data(df)
        logger.info(f'Prices scraped for {tdate.date()}')


if __name__ == '__main__':
    usa_prices_stats = UsaNyisoPricesStatsJob()
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    df = usa_prices_stats.test_run(folder)
    scraper = factory.get_scraper_job('com_nyiso', 'usa_nyiso_prices_stats')
