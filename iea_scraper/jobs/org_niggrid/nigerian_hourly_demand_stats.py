# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

from iea_scraper.core import factory
from iea_scraper.settings import SSL_CERTIFICATE_PATH

timezone = 'Pacific/Auckland'

import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob


class NigerianHourlyDemandStatsJob(EdcJob):
    title: str = 'NIGGRIG.org - Nigerian Hourly Demand Statistics'

    def __init__(self):
        EdcJob.__init__(self)
        self.url = 'https://www.niggrid.org/DisCoLoadProfile'
        self.demand_data_points = []
        self.df_demand=pd.DataFrame()

    def get_request_result(self):
        r = requests.get(self.url, verify=SSL_CERTIFICATE_PATH)
        soup = BeautifulSoup(r.text, 'html.parser')
        table_rows = soup.find('table').find_all("tr")
        all_data_points = []
        date_text = soup.find(attrs={"class": "stats"}).get_text()
        local_datetime = datetime.strptime(" ".join(date_text.split()), "Data as at %d/%m/%Y %H:%M:00")
        for row in table_rows[1:]:
            cells = row.find_all("td")
            data_point = {'local_datetime': local_datetime,
                          'company': cells[0].get_text(),
                          'Value': cells[1].get_text()}
            all_data_points += [data_point]
        self.demand_data_points += all_data_points

    def format_demand(self):
        df_demand = pd.DataFrame(self.demand_data_points)
        df_demand = df_demand.loc[df_demand['company'] !='Total: ']
        df_demand['Value'] = df_demand['Value'].apply(lambda x: float(x.replace(',','')))
        df_demand = df_demand.groupby(['local_datetime'])['Value'].sum().reset_index()
        df_demand['local_date'] = df_demand['local_datetime'].dt.date
        df_demand['Country'] = 'Nigeria'
        df_demand['Source'] = 'Transmission Company of Nigeria'
        df_demand['Product'] = 'ELE'
        df_demand['Metric'] = 'Demand'
        df_demand['utc_datetime'] = pd.to_datetime(df_demand['local_datetime']).dt.tz_localize('Africa/Lagos',
                                                                                               nonexistent='shift_forward',
                                                                                               ambiguous='NaT').dt.tz_convert('UTC')
        df_demand['utc_date'] = df_demand['utc_datetime'].dt.date
        df_demand['Export Date'] =self.export_datetime
        self.df_demand = df_demand

    @property
    def df_dw(self):
        return self.df_demand

    def pre_run(self):
        self.get_request_result()
        self.format_demand()
        logger.info(f'Nigeria NIGGRID: {self.export_datetime} scraped for demand')



if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper = NigerianHourlyDemandStatsJob()
    scraper= factory.get_scraper_job('org_niggrid','nigerian_hourly_demand_stats')
    scraper.test_run(folder)
