# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io
import pandas as pd
import logging
import sys
import requests

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseKansaiGenerationStatsJob(EdcJapanJob):
    title: str = 'Kansai TD - Japan  Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.main_page_url = "https://www.kansai-td.co.jp/denkiyoho/csv/area_jyukyu_jisseki_{year}.csv"
        self.fuel_mapping = {'原子力〔MWh〕': 'Nuclear',
                             '火力〔MWh〕': 'Thermal',
                             '水力〔MWh〕': 'Hydro',
                             '地熱〔MWh〕': 'Geothermal',
                             'バイオマス〔MWh〕': 'Biomass',
                             '実績〔MWh〕': 'Solar',
                             '抑制量〔MWh〕': 'Solar',
                             '実績〔MWh〕.1': 'Wind Onshore',
                             '抑制量〔MWh〕.1': 'Wind Onshore',
                             '揚水〔MWh〕': 'Hydro Pumped Storage'
                             }
        self.source = 'Kansai TD'
        self.region = 'Kansai'

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
            link = self.main_page_url.format(year=csv_year)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            if r.status_code == 200:
                df_year = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)
                first_row_found = False
                i = 0
                while not first_row_found and i < len(df_year):
                    cell_value = df_year.iloc[i][0]
                    if cell_value == 'DATE_TIME':
                        first_row_found = True
                        df_year.columns = df_year.iloc[i]
                        df_year = df_year.iloc[i + 1:]
                    else:
                        i += 1
                self.year_df_generation[csv_year] = df_year

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        df = self.year_df_generation[csv_year]
        if df is None:
            raise EdcJobError (f"JAPAN KANSAI: generation data not available for {tdate.date()}")
        else:
            df['local_datetime'] = pd.to_datetime(df['DATE_TIME'])
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE_TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN KANSAI: generation data not available for {tdate.date()}")
            else:
                df = self.format_generation_for_all_scrapers(df)
                logger.info(f"JAPAN KANSAI: generation data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ka = JapaneseKansaiGenerationStatsJob()
    jp_ka.test_run(folder, historical=True)
    # jp_ka.run_date(datetime(2020,3,1))
