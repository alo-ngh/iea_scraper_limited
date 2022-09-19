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
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseChugokuDemandStatsJob(EdcJapanJob):
    title: str = 'Energia.Chugoku - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url = "https://www.energia.co.jp/nw/jukyuu/sys/juyo-{year}.csv"
        self.conversion_mw = 10
        self.year_df_demand = {year: None for year in range(2018, self.export_date.year + 1)}  # calendar year
        self.source = 'Energia'
        self.region = 'Chugoku'

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_demand[csv_year] is None:
            link = self.url.format(year=csv_year)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            self.year_df_demand[csv_year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), header=1)

    def format_df(self, date):
        csv_year = date.year if date.month >= 4 else date.year - 1
        df = self.year_df_demand[csv_year]
        if df is None:
            raise EdcJobError(f"Japan Chugoku: demand data not available on {date.date()}")
        else:
            df = df.loc[df['DATE'] == '{dt.year}/{dt.month}/{dt.day}'.format(dt=date)]
            if df.empty:
                logger.warning(f"Japan Chugoku: demand data not available on {date.date()}")
            else:
                df = df.rename(columns={'実績(万kW)': 'Value'})
                df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                                axis=1)
                df['local_date'] = df['local_datetime'].dt.date
                df = self.format_demand_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"Japan Chugoku: demand data extracted and formatted for {date.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_toho = JapaneseChugokuDemandStatsJob()
    jp_toho.test_run(folder, historical=True)
