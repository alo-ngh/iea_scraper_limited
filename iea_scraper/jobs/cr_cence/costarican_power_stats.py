# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class CostaricanPowerStatsJob(EdcBulkJob):
    title: str = 'CENCE - Costa Rican Power Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.urls = {'Generation': "https://apps.grupoice.com/CenceWeb/data/sen/json/EnergiaHorariaFuentePlanta?",
                     'Demand': "https://apps.grupoice.com/CenceWeb/data/sen/json/DemandaMW?"}
        self.fuel_mapping = {'Hidroeléctrica': 'Hydro',
                             'Térmica': 'Thermal',
                             'Geotérmica': 'Geothermal',
                             'Eólica': 'Wind Onshore',
                             'Bagazo': 'Biomass',
                             'Intercambio': np.nan,
                             'Solar': 'Solar'}
        self.output_df = {'Generation': pd.DataFrame(),
                          'Demand': pd.DataFrame()}

    @property
    def df_dw(self):
        df_dw = pd.concat([self.output_df['Demand'], self.output_df['Generation']])
        return df_dw

    @property
    def offset_now(self):
        return 1

    def get_url_metric(self, tdate, metric):
        """
        This method builds the url depending on the date and the metric
        :param tdate: datetime
        :param metric: str (can be generation or demand)
        :return: str (url)
        """
        if metric == 'Generation':
            url_json = self.urls[metric] + "anno=" + str(tdate.year) + "&mes=" + str(tdate.month) + "&dia=" + str(
                tdate.day)
        elif metric == 'Demand':
            url_json = self.urls[metric] + "inicio=" + (tdate + timedelta(days=-1)).strftime("%Y%m%d") + "&fin=" \
                       + tdate.strftime("%Y%m%d")
        return url_json

    def get_json_data(self, tdate, metric):
        """
        This method requests the CENCE website depending on the metric and retrieves the data in a json format
        :param tdate: datetime
        :param metric: str(can be generation or demand)
        :return: list (json_data)
        """
        url_json = self.get_url_metric(tdate, metric)
        r = requests.get(url_json, verify=SSL_CERTIFICATE_PATH)
        json_file = r.json()
        json_data = json_file['data']
        return json_data

    def get_all_data_points(self, metric, json_data):
        """
        This method collects all data points in json_data depending on the metric and stores them in a list.
        :param metric: str (can be Generation or Demand)
        :param json_data: list
        :return: list (all_data_points)
        """
        all_data_points = []
        for item in json_data:
            if metric == 'Generation':
                data_point = self.get_generation_data(metric, item)
            elif metric == 'Demand':
                data_point = self.get_demand_data(metric,item)
            else:
                raise ValueError("metric must be Generation or Demand")
            all_data_points += data_point
        return all_data_points

    def get_generation_data(self, metric, item):
        """
        This method collects generation data from the json file.
        :param metric: str (Generation)
        :param item: str (item in json_data['Data']
        :return: list (data_point)
        """
        if pd.isna(self.fuel_mapping[item['fuente']]):
            data_point = []
        else:
            data_point = [{'local_datetime': datetime.strptime(item['fecha'], '%Y-%m-%d %H:%M:00.0'),
                           'local_date': datetime.strptime(item['fecha'], '%Y-%m-%d %H:%M:00.0').date(),
                           'Product': self.fuel_mapping[item['fuente']],
                           'Value': item['dato'],
                           'Metric': metric}]
        return data_point

    def get_demand_data(self, metric, item):
        """
        This method collects demand data from the json file.
        :param metric: str (Demand)
        :param item: str (item in json_data['Data']
        :return: list (data_point)
        """
        data_point = [{'local_datetime': datetime.strptime(item['fechaHora'], '%Y-%m-%d %H:%M:00.0'),
                       'local_date': datetime.strptime(item['fechaHora'], '%Y-%m-%d %H:%M:00.0').date(),
                       'Product': 'ELE',
                       'Value': item['MW'],
                       'Metric': metric}]
        return data_point

    def format_df(self, df):
        """
        This method formats the input DataFrame to match the DB format
        :param df: DataFrame
        :return: DataFrame
        """
        df['Country'] = 'Costa Rica'
        df['Source'] = 'CENCE'
        df['Export Date'] = self.export_datetime
        df['utc_datetime'] = pd.to_datetime(df['local_datetime']).dt.tz_localize('America/Costa_Rica').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        return df

    def run_date(self, tdate):
        for metric in self.urls:
            json_data = self.get_json_data(tdate, metric)
            all_data_points = self.get_all_data_points(metric, json_data)
            df = self.format_df(pd.DataFrame(all_data_points))
            self.output_df[metric] = pd.concat([self.output_df[metric], df])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    moon_scraper = CostaricanPowerStatsJob()
    moon_scraper.test_run(folder)
