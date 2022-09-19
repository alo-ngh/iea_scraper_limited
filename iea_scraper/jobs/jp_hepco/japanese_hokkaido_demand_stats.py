# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io
from zipfile import ZipFile
import pandas as pd
from datetime import datetime
import logging
import sys
import requests
from bs4 import BeautifulSoup

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseHokkaidoDemandStatsJob(EdcJapanJob):
    title: str = 'Hepco.co.jp - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url_demand = 'http://denkiyoho.hepco.co.jp/area_download.html'
        self.base_url = "http://denkiyoho.hepco.co.jp/"
        self.all_zip_links = self.get_all_zip_links()
        self.conversion_mw = 10
        self.df_demand= pd.DataFrame()
        self.source = 'Hepco'
        self.region = 'Hokkaido'

    def get_all_zip_links(self):
        """
        gets all zip_links and stores them in a list
        :return: list
        """
        r = requests.get(self.url_demand, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        zip_links = [self.base_url + elem['href'] for elem in soup.find_all('a', href=True) if ".zip" in elem['href']]
        return zip_links

    def get_data(self, tdate):
        """
        filters all_zip_links to get the zip link corresponding to the right quarter and year
        extracts csv data from the quarterly zip file
        stores in DF
        :param date: datetime
        :return: DataFrame
        """
        quarter_str_dict = {1: '01-03',
                            2: '04-06',
                            3: '07-09',
                            4: '10-12'}
        quarter_zip_link = [zip for zip in self.all_zip_links if
                            quarter_str_dict[pd.Timestamp(tdate).quarter] in zip and str(tdate.year) in zip]
        r = requests.get(quarter_zip_link[0], verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        zip_file = ZipFile(io.BytesIO(r.content))
        csv_file = [csv for csv in zip_file.namelist() if tdate.strftime("%Y%m%d") in csv]
        if csv_file:
            self.df_demand = pd.read_csv(zip_file.open(csv_file[0]), encoding='mskanji', header=1)
            self.df_demand = self.find_first_row_of_data(self.df_demand)

    def format_df(self, tdate):
        if self.df_demand is None:
            raise EdcJobError(f"Japan Hokkaido: demand data not extracted on {tdate.date()}")
        else:
            if self.df_demand.empty:
                logger.warning(f"Japan Hokkaido: demand data not extracted on {tdate.date()}")
            else:
                df = self.df_demand.copy()
                df = self.df_demand.rename(columns={'当日実績(万kW)': 'Value'})
                df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                                axis=1)
                df = df[['local_datetime', 'Value']]
                df['local_date'] = df['local_datetime'].dt.date
                df =self.format_demand_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"JAPAN HEPCO: demand data collected and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_hepco = JapaneseHokkaidoDemandStatsJob()
    # tdate = datetime(2019, 9, 22)
    jp_hepco.test_run(folder, historical=True)
    # jp_hepco.run_date(tdate)