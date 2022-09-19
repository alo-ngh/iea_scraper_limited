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
from iea_scraper.core.japan_job import  EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')



class JapaneseOkinawaDemandStatsJob(EdcJapanJob):
    title: str = 'Okinawa Okiden - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url = self.url = "https://www.okiden.co.jp/denki2/juyo_10_%Y%m%d.csv"
        self.conversion_mw = 10
        self.df_demand = None
        self.source = 'Okiden'
        self.region = 'Okinawa'


    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """

        link = tdate.strftime(self.url)
        r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1, index_col=False, usecols=[0,1,2,3])
            self.df_demand = self.find_first_row_of_data(df)
        else:
            logger.warning(f"Japan Okinawa: demand data not available on {tdate.date()}")

    def format_df(self, tdate):
        if self.df_demand.empty:
            raise EdcJobError(f"Japan Okinawa: demand data not extracted on {tdate.date()}")
        else:
            if self.df_demand.empty:
                logger.warning(f"Japan Okinawa: demand data not extracted on {tdate.date()}")
            else:
                df = self.df_demand.copy()
                df = df.rename(columns={'当日実績(万kW)': 'Value'})
                df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
                df['local_date'] = df['local_datetime'].dt.date
                df = self.format_demand_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"Japan Okinawa: demand data extracted and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ho = JapaneseOkinawaDemandStatsJob()
    jp_ho.test_run(folder, historical=True)
    # jp_ho.run_date(datetime(2019,8,31))
