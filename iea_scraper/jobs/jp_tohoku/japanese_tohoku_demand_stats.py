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

from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.exceptions import EdcJobError


class JapaneseTohokuDemandStatsJob(EdcJapanJob):
    title: str = 'Tepco - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric ='Demand'
        self.source = 'Tohoku-Epco'
        self.region = 'Tohoku'
        self.url = "https://setsuden.nw.tohoku-epco.co.jp/common/demand/juyo_%Y_tohoku.csv"
        self.conversion_mw = 10
        self.year_df_demand = {year: None for year in range(2016, self.export_date.year + 1)}  # calendar year

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        if self.year_df_demand[tdate.year] is None:
            link = tdate.strftime(self.url)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            self.year_df_demand[tdate.year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)

    def format_df(self, tdate):
        df = self.year_df_demand[tdate.year]
        if self.year_df_demand[tdate.year] is None:
            raise EdcJobError(f"Japan Tohoku: demand data not available on {tdate.date()}")
        else:
            if df.loc[df['DATE'] == '{dt.year}/{dt.month}/{dt.day}'.format(dt=tdate)].empty:
                logger.warning(f"Japan Tohoku: demand data not available on {tdate.date()}")
            else:
                df = df.loc[df['DATE'] == '{dt.year}/{dt.month}/{dt.day}'.format(dt=tdate)]
                df = df.rename(columns={'実績(万kW)': 'Value'})
                df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
                df['local_date'] = df['local_datetime'].dt.date
                df = self.format_demand_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"Japan Tohoku: demand data extracted and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_toho = JapaneseTohokuDemandStatsJob()
    jp_toho.test_run(folder, historical=True)
