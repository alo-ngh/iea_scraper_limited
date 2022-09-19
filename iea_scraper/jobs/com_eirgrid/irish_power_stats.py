# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""

import numpy as np
import pandas as pd
from datetime import datetime
from time import sleep
import json
import logging
import sys
import requests
from pathlib import Path

from iea_scraper.core.exceptions import EdcJobError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class IrishPowerStatsJob(EdcBulkJob):
    title: str = 'EIRGRID.com - Irish Daily Power Statistics'

    def __init__(self):
        super().__init__(self)
        self.main_url = "https://reports.sem-o.com/api/v1/dynamic/"
        self.report_mapping = {'generation': 'BM-086',
                               'demand': 'BM-010',
                               'prices': 'BM-095'}
        self.power_plant_mapping = self.get_resources_mapping()
        self.fuel_mapping = {'WIND': 'Wind Onshore',
                             'SOLAR': 'Solar',
                             'MULTI_FUEL': 'Other',
                             'COAL': 'Coal',
                             'GAS': 'Natural Gas',
                             'HYDRO': 'Hydro',
                             'PEAT': 'Peat',
                             'PUMP_STORAGE': 'Hydro Pumped Storage',
                             'BIOMASS': 'Biomass',
                             'DISTILLATE': 'Other',
                             'BATTERY': np.nan,
                             'OIL': 'Oil'
                             }
        self.headers = {'origin': 'https://www.sem-o.com',
                        'referer': 'https://www.sem-o.com/'
                        }
        self.output_data = {'prices': pd.DataFrame(),
                            'demand': pd.DataFrame(),
                            'generation': pd.DataFrame()
                            }
        self.region_mapping = {'ROI': 'Republic of Ireland',
                               'NI': 'Northern Ireland'
                               }

    @property
    def offset_now(self):
        return 6

    def get_resources_mapping(self):
        dir_path = Path(__file__).parents[0]
        file_path = dir_path / 'List-of-Registered-Units.xlsx'
        df_resources = pd.read_excel(file_path, header=2, sheet_name="Registered Units TSC")
        df_resources = df_resources.loc[df_resources['Resource Type'] == 'GENERATOR']
        df_resources = df_resources[['Resource Name', 'Fuel Type']]
        dict_resources = df_resources.set_index('Resource Name').to_dict()['Fuel Type']
        return dict_resources

    def get_data_from_url(self, tdate, metric):
        """
        extracts data for metric for yesterday from API url and updates input_df[metric]
        :return: DataFrame
        """
        # sleep(5)
        page_number = 1
        query_param = self.get_query_param(tdate, page_number, metric)
        url = self.main_url + query_param

        r = requests.get(url, headers=self.headers, verify=SSL_CERTIFICATE_PATH)
        json_data = json.loads(r.text)
        total_pages = json_data['pagination']['totalPages']
        json_items = json_data['items']

        if total_pages <=1:
            data = pd.DataFrame(json_items)
        else:
            i = 1
            all_data_downloaded = False
            while i < total_pages + 1 and not all_data_downloaded:
                url = self.main_url + self.get_query_param(tdate, page_number + i, metric)
                r = requests.get(url, headers=self.headers, verify=SSL_CERTIFICATE_PATH)
                json_data = json.loads(r.text)['items']
                json_items += json_data
                if i == total_pages:
                    all_data_downloaded = True
                else:
                    i += 1
            data = pd.DataFrame(json_items)
        return data

    def get_query_param(self, tdate, page_number, metric):
        query_param = self.report_mapping[metric] \
                      + "?StartTime=%3E%3D" + tdate.strftime("%Y-%m-%d") \
                      + "T00%3A00%3A00%3C%3D" \
                      + tdate.strftime("%Y-%m-%d") \
                      + "T23%3A59%3A00&sort_by=StartTime&order_by=ASC&ParticipantName=&ResourceName=&page=" \
                      + str(page_number) \
                      + "&page_size=100000"
        return query_param

    def format_demand(self, df):
        '''
        formats demand data
        '''
        df = df[['StartTime', 'LoadForecastROI', 'LoadForecastNI']]
        df = df.rename(columns={'StartTime': 'local_datetime', 'LoadForecastROI': 'ROI', 'LoadForecastNI': 'NI'})
        df = pd.melt(df, id_vars=['local_datetime'], var_name='Region', value_name='Value')
        df['Metric'] = 'Demand'
        df['Product'] = 'ELE'
        df['Flow 1'] = 'Forecast'
        return df

    def format_prices(self, df):
        '''
        formats prices data
        '''
        df = df[['StartTime', 'ImbalancePrice']]
        df = df.rename(columns={'StartTime': 'local_datetime', 'ImbalancePrice': 'Value'})
        df['Metric'] = 'Prices'
        df['Product'] = 'ELE'
        df['Flow 2'] = 'EUR'
        return df

    def format_generation(self, df):
        '''
        formats generation data
        '''
        df = df[['StartTime', 'ResourceName', 'Jurisdiction', 'MeteredMW']]
        df = df.rename(columns={'StartTime': 'local_datetime', 'ResourceName': 'Product', 'Jurisdiction': 'Region',
                                'MeteredMW': 'Value'})
        df['Product'] = df['Product'].map(self.power_plant_mapping).map(self.fuel_mapping)
        df = df.groupby(['local_datetime', 'Region', 'Product'])['Value'].sum().reset_index()
        df = df.loc[~df['Product'].isnull()]
        df['Metric'] = 'Generation'
        return df

    def format_dfs(self, metric, df):
        """
        This method formats input_df to comply with DW format
        """
        if metric == 'demand':
            df = self.format_demand(df)
        elif metric == 'generation':
            df = self.format_generation(df)
        elif metric == 'prices':
            df = self.format_prices(df)
        else:
            raise EdcJobError("metric must be in ['generation','prices','demand']")
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        if 'Region' in df.columns:
            df['Region'] = df['Region'].map(self.region_mapping)
        df['Country'] = 'Ireland'
        df['Source'] = 'Sem-o'
        self.output_data[metric] = pd.concat([self.output_data[metric], df])

    @property
    def df_dw(self):
        df_dw = pd.DataFrame()
        for metric in self.output_data:
            if not self.output_data[metric].empty:
                df_dw = pd.concat([df_dw, self.output_data[metric]])
        return df_dw

    def run_date(self, tdate):
        for metric in self.output_data:
            data = self.get_data_from_url(tdate, metric)
            if not data.empty:
                self.format_dfs(metric, data)
                logger.info(f"Ireland: {metric} data scraped for {tdate.date()}")
            else:
                logger.warning(f"Ireland: {metric} data not available for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\iea_scraper\iea_scraper\csvs'
    eirgrid = IrishPowerStatsJob()
    eirgrid.test_run(folder, historical=True)
    #eirgrid.run_date(datetime(2021, 10, 30))
    #eirgrid.to_csv(folder)
