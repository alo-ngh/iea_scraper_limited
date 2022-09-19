# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import sys
import io
from bs4 import BeautifulSoup

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseHokkaidoGenerationStatsJob(EdcJapanJob):
    title: str = 'Hepco - Japan  Power Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.main_url = 'https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/supply_demand_results.html'
        self.base_url = "https://www.hepco.co.jp/network/renewable_energy/fixedprice_purchase/"
        self.all_links = self.get_all_csv_links()
        self.conversion_mw = 10
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
        self.source = 'Hepco'
        self.region = 'Hokkaido'

    def get_all_csv_links(self):
        """
        get all csv links from webpage
        :return: list
        """
        r = requests.get(self.main_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        all_links = [self.base_url + elem['href'] for elem in soup.find_all('a', href=True) if
                     ".csv" in elem['href'] or '.xls' in elem['href']]
        return all_links

    def get_data(self, tdate):
        """
        get generation data for parameter year
        @param year: int
        @return: df
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            df_year = pd.DataFrame()
            all_year_csvs = [link for link in self.all_links if str(csv_year) in link]
            for link in all_year_csvs:
                r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
                try:
                    df_link = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=2)
                    df_link = df_link.drop(df_link.index[0])
                    df_link['月日'] = df_link['月日'].fillna(method='ffill')
                except:
                    logger.warning(f"JAPAN HOKKAIDO: generation data not available for quarter {link[-11:-5]}")
                df_year = pd.concat([df_year, df_link])
                df_year = df_year.loc[~df_year['時刻'].isnull()]
            self.year_df_generation[csv_year] = df_year

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            raise EdcJobError(f"JAPAN HOKKAIDO: generation data not available for {tdate.date()}")
        else:
            df = self.year_df_generation[csv_year].copy()
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['月日'] + ' ' + x['時刻'], "%Y/%m/%d %H時"),
                                            axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['月日', '時刻'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN HOKKAIDO: generation data not available for {tdate.date()}")
            else:
                df = self.format_generation_for_all_scrapers(df)
                logger.info(f"JAPAN HOKKAIDO: generation data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_hepco = JapaneseHokkaidoGenerationStatsJob()
    jp_hepco.test_run(folder, historical=True)
    # jp_hepco.run_date(datetime(2022,3,31))
