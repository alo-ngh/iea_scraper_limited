# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io
import numpy as np
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


class JapaneseKyushuGenerationStatsJob(EdcJapanJob):
    title: str = 'Kyuden.co.jp - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.url = "https://www.kyuden.co.jp/td_service_wheeling_rule-document_disclosure"
        self.base_url = "https://www.kyuden.co.jp/"
        self.fuel_mapping = {'原子力〔MWh〕': 'Nuclear',
                             '火力〔MWh〕': 'Thermal',
                             '水力〔MWh〕': 'Hydro',
                             '地熱〔MWh〕': 'Geothermal',
                             'バイオマス〔MWh〕': 'Biomass',
                             '実績〔MWh〕': 'Solar PV',
                             '抑制量〔MWh〕': 'Solar PV',
                             '実績〔MWh〕.1': 'Wind Onshore',
                             '抑制量〔MWh〕.1': 'Wind Onshore',
                             '揚水等〔MWh〕': 'Hydro Pumped Storage',
                             '火力等〔MWh〕': 'Thermal',
                             '揚水〔MWh〕': 'Hydro Pumped Storage',
                             }
        self.csv_links = self.get_all_csv_links()
        self.source = 'Kyuden'
        self.region = 'Kyushu'

    def get_all_csv_links(self):
        """
        This method extract allcsv links from webpage and stores them in a list
        :return: list
        """
        r = requests.get(self.url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        csv_links = [self.base_url + elem['href'] for elem in soup.find_all('a', href=True) if ".csv" in elem['href']]
        return csv_links

    def get_data(self, tdate):
        """
        collects all generation data from csv links and stores in year_df_generation
        each quarterly csv is concatenated in the yearly df
        the values of the dictionnary are the years
        Warning: data in fiscal year
        @return: dictionary
        """

        year_elem_dict = {2016: "H28",
                          2017: "H29",
                          2018: "H30"}
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            df_year = pd.DataFrame()
            if csv_year in year_elem_dict:
                all_year_csvs = [link for link in self.csv_links if year_elem_dict[csv_year] in link]
            else:
                all_year_csvs = [link for link in self.csv_links if str(csv_year) in link]
            for link in all_year_csvs:
                r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                df_link = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)
                df_year = pd.concat([df_year, df_link])
                df_year = df_year.loc[~df_year['DATE_TIME'].isnull()]
            self.year_df_generation[csv_year] = df_year

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year]
        if df is None or df.empty:
            raise EdcJobError(f"JAPAN KYUSHU: generation data not available for {tdate.date()}")
        else:
            df['local_datetime'] = pd.to_datetime(df['DATE_TIME'])
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE_TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN KYUSHU: generation data not available for {tdate.date()}")
            else:
                df= self.format_generation_for_all_scrapers(df)
                logger.info(f"JAPAN KYUSHU: generation data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_kyu = JapaneseKyushuGenerationStatsJob()
    jp_kyu.test_run(folder, historical=True)
    # jp_kyu.run_date(datetime(2019,3,31))
