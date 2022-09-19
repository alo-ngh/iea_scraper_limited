"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io

import pandas as pd
from datetime import datetime
import logging
import sys

import requests

from iea_scraper.core import factory
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseHokurikuDemandStatsJob(EdcJapanJob):
    title: str = 'Hokuriku Rikuden - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url = "https://www.rikuden.co.jp/nw/denki-yoho/csv/juyo_05_%Y%m%d.csv"
        self.conversion_mw = 10
        self.df_demand = None
        self.source = 'Rikuden'
        self.region = 'Hokuriku'

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """

        link = tdate.strftime(self.url)
        r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)
            self.df_demand = self.find_first_row_of_data(df)

    def format_df(self, tdate):
        if self.df_demand is None:
            raise EdcJobError(f"Japan Hokuriku: demand data not extracted on {tdate.date()}")
        else:
            if self.df_demand.empty:
                logger.warning(f"Japan Hokuriku: demand data not extracted on {tdate.date()}")
            else:
                df = self.df_demand.copy()
            df = df.rename(columns={'当日実績(万kW)': 'Value'})
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = self.format_demand_for_all_scrapers(df)
            self.output_df = pd.concat([self.output_df, df])
            logger.info(f"Japan Hokuriku: demand data extracted and formatted for {tdate.date()}")



if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ho = JapaneseHokurikuDemandStatsJob()
    # jp_ho.run_date(datetime(2016,7,1))
    jp_ho.test_run(folder, historical=True)
    # scraper= factory.get_scraper_job('jp_rikuden','japanese_hokuriku_generation_stats')