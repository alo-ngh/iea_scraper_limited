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

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseShikokuDemandStatsJob(EdcJapanJob):
    title: str = 'Shikoku Yonden - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.source = 'Yonden'
        self.region = 'Shikoku'
        self.url = self.url = "https://www.yonden.co.jp/nw/denkiyoho/csv/juyo_shikoku_%Y.csv"
        self.conversion_mw = 10
        self.input_df = None
        self.year_df_demand = {year: None for year in range(2016, self.export_date.year + 1)}

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        link = tdate.strftime(self.url)
        r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        if r.status_code == 200:
            self.year_df_demand[tdate.year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)

    def format_df(self, tdate):
        df = self.year_df_demand[tdate.year]
        if df is None:
            raise EdcJobError(f"Japan Shikoku: demand data not available on {tdate.date()}")
        else:
            df = df.rename(columns={'実績(万kW)': 'Value'})
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = df.loc[df['local_date'] == tdate.date()]
            if df.empty:
                logger.warning(f"JAPAN SHIKOKU: demand data not available for {tdate.date()}")
            else:
                df = self.format_demand_for_all_scrapers(df)
                logger.info(f"JAPAN Shikoku: demand data collected and formatted for {tdate.date()}")
                self.output_df = pd.concat([self.output_df, df])



if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ho = JapaneseShikokuDemandStatsJob()
    jp_ho.test_run(folder, historical=True)
