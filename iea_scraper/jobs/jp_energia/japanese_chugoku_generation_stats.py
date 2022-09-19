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

from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseChugokuGenerationStatsJob(EdcJapanJob):
    title: str = 'Energia.Chugoku - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.main_page_url = "https://www.energia.co.jp/nw/service/retailer/data/area/"
        self.current_data_url = "https://www.energia.co.jp/nw/service/retailer/eriajyukyu/sys/eria_jyukyu.csv"
        self.bulk_base_url = "https://www.energia.co.jp/nw/service/retailer/data/area/"
        self.fuel_mapping = {'原子力': 'Nuclear',
                             '火力': 'Thermal',
                             '水力': 'Hydro',
                             '地熱': 'Geothermal',
                             'バイオマス': 'Biomass',
                             '太陽光(実績)': 'Solar PV',
                             '太陽光(抑制量)': 'Solar PV',
                             '風力(実績)': 'Wind Onshore',
                             '風力(抑制量)': 'Wind Onshore',
                             '揚水': 'Hydro Pumped Storage',
                             }
        self.source = 'Energia'
        self.region = 'Chugoku'
        self.csvs_available = self.get_all_csv_links()
        self.current_data = None

    def get_all_csv_links(self):
        """
        This method extract allcsv links from webpage and stores them in a list
        :return: list
        """
        r = requests.get(self.main_page_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        csv_available = [elem['href'] for elem in soup.find_all('a', href=True) if ".csv" in elem['href']]
        return csv_available

    def get_data(self, tdate):
        """
        extracts data from csv and stores in year_df_generation
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            csv_links = [csv for csv in self.csvs_available if str(csv_year) in csv]
            if not csv_links:
                r = requests.get(self.current_data_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                df_current_data = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=2)
                first_date_in_current = pd.to_datetime(df_current_data.iloc[1, 0])
                for year in range(first_date_in_current.year, self.last_available_date.year + 1):
                    self.year_df_generation[year] = df_current_data
            else:
                url = self.bulk_base_url + csv_links[0]
                r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                self.year_df_generation[csv_year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=2)

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year].copy()
        df = df.dropna(axis=0)
        df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                        axis=1)
        df['local_date'] = df['local_datetime'].dt.date
        df = df.drop(columns=['DATE', 'TIME'])
        if df.loc[df['local_date'] == tdate.date()].empty:
            logger.warning(f"JAPAN CHUGOKU: generation data not available for {tdate.date()}")
        else:
            df = df.loc[df['local_date'] == tdate.date()]
            df= self.format_generation_for_all_scrapers(df)
            logger.info(f"JAPAN CHUGOKU: generation data collected and formatted for {tdate.date()}")
            self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_chu = JapaneseChugokuGenerationStatsJob()
    jp_chu.test_run(folder, historical=True)
    # jp_chu.run_date(datetime(2019, 3, 31))
