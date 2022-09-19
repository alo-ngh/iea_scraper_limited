# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""

import pandas as pd
from datetime import datetime
import logging
import sys

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseTepcoDemandStatsJob(EdcJapanJob):
    title: str = 'Tepco - Japan  Demand Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url_demand = 'https://www4.tepco.co.jp/forecast/html/images/juyo-%Y.csv'
        self.conversion_mw = 10
        self.year_df_demand = {year: None for year in range(2016, self.export_date.year + 1)} # calendar year
        self.source = 'Tepco'
        self.region = 'Tokyo'

    def get_data(self, tdate):
        """
        This method extract all annual generation from a csv and fill year_df_demand depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        link = tdate.strftime(self.url_demand)
        self.year_df_demand[tdate.year] = pd.read_csv(link, engine='python', encoding="shift_jis", skiprows=2)

    def format_df(self, tdate):
        df = self.year_df_demand[tdate.year]
        if df is None:
            raise EdcJobError(f"Japan Tepco: demand data not available on {tdate.date()}")
        else:
            df = df.rename(columns={'実績(万kW)': 'Value'})
            df['local_date'] = pd.to_datetime(df['DATE'], format="%Y/%m/%d").dt.date
            if df.loc[df['local_date'] == tdate.date()].empty:
                logger.warning(f"Japan Tepco: demand data not available on {tdate.date()}")
            else:
                df = df.loc[df['local_date'] == tdate.date()]
                df['Value'] = df['Value'].replace(',', '')
                df['Flow 1'] = 'Forecast'
                df['local_datetime'] = df.apply(
                    lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"), axis=1)
                df = df.drop(columns=['TIME', 'DATE'])
                df = self.format_demand_for_all_scrapers(df)
                self.output_df = pd.concat([self.output_df, df])
                logger.info(f"Japan Tepco: demand data extracted and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    jp_tepco = JapaneseTepcoDemandStatsJob()
    jp_tepco.test_run(folder, historical=True)
