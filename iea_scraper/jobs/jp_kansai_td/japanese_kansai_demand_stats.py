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
from zipfile import ZipFile
import requests

from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob


class JapaneseKansaiDemandStatsJob(EdcJapanJob):
    title: str = 'Kansai TD- Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url = "https://www.kansai-td.co.jp/yamasou/%Y%m_jisseki.zip"
        self.conversion_mw = 10
        self.source = 'Kansai TD'
        self.region = 'Kansai'
        self.all_demand_dict = {}

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        if tdate.strftime("%Y%m%d") not in self.all_demand_dict:
            link = tdate.strftime(self.url)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            zip = ZipFile(io.BytesIO(r.content))
            for file in zip.namelist():
                df = pd.DataFrame()
                j=1
                while df.empty and j <= 16:
                    try:
                        df = pd.read_csv(zip.open(file), encoding='mskanji', skiprows=j,
                                         index_col=False, usecols=[0,1,2,3])
                    except:
                        j += 1
                df = self.find_first_row_of_data(df)
                self.all_demand_dict[file[:8]] = df

    def format_df(self, tdate):
        if tdate.strftime("%Y%m%d") not in self.all_demand_dict:
            logger.warning(f"JAPAN KANSAI: demand data not available on {tdate.date()}")
        else:
            df = self.all_demand_dict[tdate.strftime("%Y%m%d")]
            df = df.rename(columns={'当日実績(万kW)': 'Value'})
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df =self.format_demand_for_all_scrapers(df)
            self.output_df = pd.concat([self.output_df, df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_ka = JapaneseKansaiDemandStatsJob()
    jp_ka.test_run(folder, historical=True)
    # jp_ka.run_date(datetime(2020,11,19))