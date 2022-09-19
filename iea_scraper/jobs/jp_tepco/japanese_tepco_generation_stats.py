# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
import sys
import numpy as np
import calendar

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseTepcoGenerationStatsJob(EdcJapanJob):
    title: str = 'Tepco - Japan  Power Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.url_generation = 'https://www.tepco.co.jp/forecast/html/images/area-{year}.csv'
        self.conversion_mw = 10
        self.fuel_mapping = {'原子力': 'Nuclear',
                             '火力': 'Thermal',
                             '水力': 'Hydro',
                             '地熱': 'Geothermal',
                             'バイオマス': 'Biomass',
                             '太陽光発電実績': 'Solar PV',
                             '太陽光出力制御量': 'Solar PV',
                             '風力発電実績': 'Wind Onshore',
                             '風力出力制御量': 'Wind Onshore',
                             '揚水': 'Hydro Pumped Storage',
                             '連系線': np.nan,
                             '合計': np.nan}
        self.source = 'Tepco'
        self.region = 'Tokyo'

    def get_data_from_csv(self, year):
        """
        This method extract all annual generation from a csv and fill year_df_generation depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        link = self.url_generation.format(year=year)
        r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        if r.status_code == 200:
            self.year_df_generation[year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), skiprows=2)

    def get_data(self, tdate):
        """
        This method collects data from the csv on the 4th of the month. The online csv is updated on the first day of
        the month. The annual data is then stored in input_dfs['generation'].
        The format method will slice the input_df and retrieve only the latest data (for M-1)
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            self.get_data_from_csv(csv_year)

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year]
        if df is None:
            raise EdcJobError(f"Japan Tepco: generation data not available on {tdate.date()}")
        else:
            df = df.rename(columns={'Unnamed: 0': 'DATE', 'Unnamed: 1': 'TIME', 'Unnamed: 2': 'Supply'})
            df['DATE'] = pd.to_datetime(df['DATE'], format="%Y/%m/%d")
            df['local_date'] = df['DATE'].dt.date
            if df.loc[df['local_date'] == tdate.date()].empty:
                logger.warning(f"Japan Tepco: generation data not available on {tdate.date()}")
            else:
                df = df.loc[df['local_date'] == tdate.date()]
                df['local_datetime'] = df.apply(
                    lambda x: x['DATE'].replace(hour=int(x['TIME'][:-3])), axis=1)
                df = df.dropna()
                df = df.drop(columns=['DATE', 'TIME'])
                df = self.format_generation_for_all_scrapers(df)
                df['Value'] = df['Value'] * self.conversion_mw
                self.output_df = pd.concat([self.output_df, df])
                self.output_df = self.output_df.drop_duplicates()
                logger.info(f"Japan Tepco: generation data extracted and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_tepco = JapaneseTepcoGenerationStatsJob()
    # tdate = datetime.now()
    jp_tepco.test_run(folder, historical=True)
