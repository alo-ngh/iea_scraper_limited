# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import io

import numpy as np
import requests
import pandas as pd
from datetime import datetime
import logging
import sys

from iea_scraper.core import factory
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.settings import SSL_CERTIFICATE_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob


class SouthAfricanPowerStatsJob(EdcBulkJob):
    title: str = 'Eksom.co.za - South Africa Power Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.urls = {'generation': 'https://www.eskom.co.za/dataportal/wp-content/uploads/%Y/%m/Station_Build_Up.csv',
                     'demand': 'https://www.eskom.co.za/dataportal/wp-content/uploads/%Y/%m/System_hourly_actual_and_forecasted_demand.csv'}
        self.input_dfs = {'generation': pd.DataFrame(),
                          'demand': pd.DataFrame()}
        self.output_dfs = {'generation': pd.DataFrame(),
                          'demand': pd.DataFrame()}
        self.first_date_available = {'generation': datetime(1900, 1, 1),
                                     'demand': datetime(1900, 1, 1)}
        self.fuel_mapping = {'Thermal_Gen_Excl_Pumping_and_SCO': 'Coal',
                             'Eskom_OCGT_SCO_Pumping': 'Natural Gas',
                             'Eskom_Gas_SCO_Pumping': 'Natural Gas',
                             'Hydro_Water_SCO_Pumping': 'Hydro Pumped Storage',
                             'Pumped_Water_SCO_Pumping': 'Hydro Pumped Storage',
                             'Thermal_Generation': np.nan, # equals to Thermal_Gen_Excl_Pumping_and_SCO + SCO_pumping  + nuclear
                             'Nuclear_Generation': 'Nuclear',
                             'International_Imports': np.nan,
                             'Eskom_OCGT_Generation': 'Natural Gas',
                             'Eskom_Gas_Generation': 'Natural Gas',
                             'Dispatchable_IPP_OCGT': 'Natural Gas',
                             'Hydro_Water_Generation': 'Hydro Run-of-river',
                             'Pumped_Water_Generation': 'Hydro Reservoir',
                             'IOS_Excl_ILS_and_MLR': np.nan,
                             'ILS_Usage': np.nan,
                             'Manual_Load_Reduction_MLR': np.nan,
                             'Wind': 'Wind Onshore',
                             'PV': 'Solar PV',
                             'CSP': 'Solar Thermal',
                             'Other_RE': 'Other Renewables'}

    @property
    def offset_now(self):
        return 2

    @property
    def day_lags(self):
        '''
        type : list
        Enables to bypass the 3 weeks logic to scrape the
        last 3 months
        '''
        return list(range(30))

    @property
    def df_dw(self):
        df_dw = pd.concat([self.output_dfs['generation'], self.output_dfs['demand']])
        return df_dw

    def get_data_from_url(self, metric, tdate):
        """
        Request data from url, saves csv as DataFrame and updates first_date_available
        when run_date is called, we need to make sure that the param date is in the csv. We do not have access to the
        archive csv links and the request status code will be 404 if the date is not available
        @param metric: str (demand or generation)
        @param tdate: datetime
        """
        r = requests.get(tdate.strftime(self.urls[metric]), verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 404:
            raise EdcJobError(f"South Africa: {metric} data not available for {tdate.date()}")
        else:
            self.input_dfs[metric] = pd.read_csv(io.StringIO(r.text))
            self.first_date_available[metric] = datetime.strptime(self.input_dfs[metric].iloc[0, 0],
                                                                  '%Y-%m-%d %H:00:00')

    def format_generation(self, df):
        """
        formats generation data
        According to the mapping, there can be negative values for natgas (own use), these are set to 0 as generation
        can only be <0 for hydro pumped storage
        @param df: DataFrame
        @return: DataFrame
        """
        df = pd.melt(df, id_vars=['local_datetime', 'local_date'], var_name='Product', value_name='Value')
        df['Product'] = df['Product'].map(self.fuel_mapping)
        df = df.loc[~df['Product'].isnull()]
        df = df.groupby(['local_datetime', 'local_date', 'Product'])['Value'].sum().reset_index()
        df.loc[(df['Value'] < 0) & (df['Product'] != 'Hydro Pumped Storage'), 'Value'] = 0
        df = df.loc[df['Value'] != 0]
        return df

    def format_demand(self, df):
        """
        Formats demand data
        RSA Contracted Demand – The hourly average demand that needs to be supplied
        by all resources that Eskom has contracts with. It is the residual demand including demand supplied
        by self-dispatched generation (such as the renewables).
        @param df: DataFrame
        @return: DataFrame
        """
        df = df.rename(columns={'RSA Contracted Demand': 'Value'})
        df = df[['local_datetime', 'Value']]
        df['Product'] = 'ELE'
        return df

    def format_data(self, metric, tdate):
        """
        Formats data to match DW tables
        @param metric: str (demand or generation)
        @param tdate: datetime
        @return: DataFrame
        """
        df = self.input_dfs[metric].copy()
        df = df.rename(columns={df.columns[0]: "local_datetime"})
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df = df.loc[df['local_date'] == tdate.date()].copy()
        if metric == 'generation':
            df = self.format_generation(df)
        elif metric == 'demand':
            df = self.format_demand(df)
        else:
            raise KeyError("metric must be demand or generation")
        df['Country'] = 'South Africa'
        df['Source'] = 'Eksom'
        df['Metric'] = metric.capitalize()
        return df

    def run_date(self, tdate):
        for metric in self.urls:
            if tdate < self.first_date_available[metric]:
                raise EdcJobError(f"South Africa: {metric} data not available for {tdate.date()}")
            else:
                self.get_data_from_url(metric, tdate)
                df_formatted = self.format_data(metric, tdate)
                self.output_dfs[metric] = pd.concat([self.output_dfs[metric], df_formatted])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    job = SouthAfricanPowerStatsJob()
    job.test_run(folder, historical=True)
    scraper_test = factory.get_scraper_job('za_eksom', 'south_african_power_stats')

