# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io

import pandas as pd
from datetime import datetime
import logging
from bs4 import BeautifulSoup
import sys

import requests

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob


class JapaneseOkinawaGenerationStatsJob(EdcJapanJob):
    title: str = 'Okinawa Okiden - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric='Generation'
        self.url = "http://www.okiden.co.jp/business-support/service/supply-and-demand/jukyu/csv/{year}.csv"
        self.conversion_mw = 10
        self.fuel_mapping = {'供給力':'Nuclear',
                             '火力':'Thermal',
                             '水力':'Hydro',
                             '地熱':'Geothermal',
                             'バイオマス':'Biomass',
                             '太陽光':'Solar PV',
                             '太陽光制御量':'Solar PV',
                             '風力':'Wind Onshore',
                             '風力制御量':'Wind Onshore',
                             '揚水':'Hydro Pumped Storage'
                             }

        self.source = 'Okiden'
        self.region = 'Okinawa'

    def get_data(self, tdate):
        """
        collects all generation data from csv links and stores in year_df_generation
        the values of the dictionnary are the years
        Warning: data in fiscal year
        @return: dictionary
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            link = self.url.format(year=csv_year)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            if r.status_code == 200:
                df_link = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=3)
                for col in df_link.columns:
                    if "Unnamed" in col:
                        new_col_name = df_link[col].values[0]
                        if pd.isna(new_col_name):
                            if not pd.isna(df_link[col].values[2]):
                                new_col_name = df_link[col].values[1] + df_link[col].values[2]
                            else:
                                new_col_name = df_link[col].values[1]
                        df_link = df_link.rename(columns={col: new_col_name})
                df_link = df_link.iloc[3:]
                df_link = df_link.dropna(axis=1)
                df_link = df_link.rename(columns=lambda x: x.replace('\n', ''))
                self.year_df_generation[csv_year] = df_link

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year]
        if df is None:
            raise EdcJobError(f"JAPAN OKINAWA: generation data not available for {tdate.date()}")
        else:
            df['local_datetime'] = df.apply(
                lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE', 'TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN OKINAWA: generation data not available for {tdate.date()}")
            else:
                df =self.format_generation_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"JAPAN OKINAWA: generation data collected and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ho = JapaneseOkinawaGenerationStatsJob()
    jp_ho.test_run(folder, historical=True)
    # jp_ho.run_date(datetime(2021,3,31))
