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


class JapaneseHokurikuGenerationStatsJob(EdcJapanJob):
    title: str = 'Hokuriku Rikuden - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.main_page_url = "https://www.rikuden.co.jp/nw_jyukyudata/area_jisseki.html"
        self.base_url = "https://www.rikuden.co.jp/"
        self.source = 'Rikuden'
        self.region = 'Hokuriku'
        self.fuel_mapping = {'原子力': 'Nuclear',
                             '火力': 'Thermal',
                             '水力': 'Hydro',
                             '地熱': 'Geothermal',
                             'バイオマス': 'Biomass',
                             '太陽光実績': 'Solar PV',
                             '太陽光抑制量': 'Solar PV',
                             '風力実績': 'Wind Onshore',
                             '風力抑制量': 'Wind Onshore',
                             '揚水': 'Hydro Pumped Storage'
                             }
        self.csv_links = self.get_all_csv_links()

    def get_all_csv_links(self):
        """
        This method extract all csv links from webpage and stores them in a list
        :return: list
        """
        r = requests.get(self.main_page_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
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
        if self.year_df_generation[tdate.year] is None:
            df_year = pd.DataFrame()
            all_year_csvs = [link for link in self.csv_links if str(tdate.year) in link]
            for link in all_year_csvs:
                r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                df_link = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=4)
                if len([elem for elem in df_link.columns if "Unnamed" in elem])>2:
                    df_link.columns = ['DATE','TIME']+list(df_link.iloc[0].values)[2:]
                    df_link = df_link.iloc[2:]
                else:
                    df_link = df_link.iloc[1:]
                    df_link = df_link.rename(columns={df_link.columns[0]: "DATE", df_link.columns[1]: "TIME"})
                df_year = pd.concat([df_year, df_link])
                df_year = df_year.loc[~df_year['DATE'].isnull()]
            self.year_df_generation[tdate.year] = df_year

    def format_df(self, tdate):
        df = self.year_df_generation[tdate.year]
        if df is None:
            raise EdcJobError(f"JAPAN HOKURIKU: generation data not available for {tdate.date()}")
        else:
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                            axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE', 'TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN HOKURIKU: generation data not available for {tdate.date()}")
            else:
                df = self.format_generation_for_all_scrapers(df)
                logger.info(f"JAPAN HOKURIKU: generation data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ho = JapaneseHokurikuGenerationStatsJob()
    jp_ho.test_run(folder, historical=True)
