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
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core.japan_job import EdcJapanJob

class JapaneseTohokuGenerationStatsJob(EdcJapanJob):
    title: str = 'Tohoku-Epco.co.jp - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric ='Generation'
        self.source = 'Tohoku-Epco'
        self.region = 'Tohoku'
        self.url = "https://setsuden.nw.tohoku-epco.co.jp/download.html"
        self.base_url = "https://setsuden.nw.tohoku-epco.co.jp/"

        self.fuel_mapping = {'水力〔MWh〕': 'Hydro',
                             '火力〔MWh〕': 'Thermal',
                             '原子力〔MWh〕': 'Nuclear',
                             '太陽光実績〔MWh〕': 'Solar PV',
                             '太陽光抑制量〔MWh〕': 'Solar PV',
                             '風力実績〔MWh〕': 'Wind Onshore',
                             '風力抑制量〔MWh〕': 'Wind Onshore',
                             '地熱〔MWh〕': 'Geothermal',
                             'バイオマス〔MWh〕': 'Biomass',
                             '揚水〔MWh〕': 'Hydro Pumped Storage'
                             }
        self.csv_links = self.get_all_csv_links()

    def get_all_csv_links(self):
        """
        This method extract allcsv links from webpage and stores them in a list
        :return: list
        """
        r = requests.get(self.url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        csv_links = [self.base_url + elem['href'] for elem in soup.find_all('a', href=True) if "Q.csv" in elem['href']]
        return csv_links

    def get_data(self, tdate):
        """
        collects all generation data from csv links and stores in year_df_generation
        each quarterly csv is concatenated in the yearly df
        the values of the dictionnary are the years
        Warning: data in fiscal year
        @return: dictionary
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            df_year = pd.DataFrame()
            all_year_csvs = [link for link in self.csv_links if str(csv_year) in link]
            if all_year_csvs:
                for link in all_year_csvs:
                    r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                    df_link = pd.read_csv(io.StringIO(r.content.decode('mskanji')))
                    df_year = pd.concat([df_year, df_link])
                    df_year = df_year.loc[~df_year['DATE_TIME'].isnull()]
                self.year_df_generation[csv_year] = df_year


    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year]
        if df is None:
            raise EdcJobError(f"JAPAN TOHOKU: generation data not available for {tdate.date()}")
        else:
            df['local_datetime'] = pd.to_datetime(df['DATE_TIME'])
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE_TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN TOHOKU: generation data not available for {tdate.date()}")
            else:
                df = self.format_generation_for_all_scrapers(df)
                logger.info(f"JAPAN TOHOKU: generation data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_toho = JapaneseTohokuGenerationStatsJob()
    jp_toho.test_run(folder, historical=True)

