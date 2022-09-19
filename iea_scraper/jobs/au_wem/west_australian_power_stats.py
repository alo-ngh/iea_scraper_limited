"""
Created on Tue 24 November 2020

@authors:
    NGHIEM_A
    DAUGY_M
    CHAMBEAU_L
"""
import io
from datetime import datetime as datetime, timedelta
import pandas as pd
import logging
import numpy as np
import requests

from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
import sys

sys.path.append(r'C:\Repos\scraper')

from iea_scraper.core.job import EdcBulkJob


class WestAustralianPowerStatsJob(EdcBulkJob):
    title: str = 'Aemo.wa - West Australian Power Statistics'
    """
    Careful: data for WA is D-2  : attention timedelta for export date
    Careful : balancing summary includes demand forecast rather than real-time demand
    Downloads from 2 differents csvs.
    see definitions here : https://data.wa.aemo.com.au/#balancing-summary
    """

    def __init__(self):
        super().__init__(self)
        self.link = 'https://data.wa.aemo.com.au/datafiles/'
        self.document_type_mapping = {
            'doc1': 'balancing-summary/balancing-summary-',
            'doc2': 'operational-measurements/operational-measurements-'
        }
        self.df_dict = {'doc1': pd.DataFrame(),
                        'doc2': pd.DataFrame()}
        self.df_all = pd.DataFrame()
        self.dict_input = self.get_csvs()

    @property
    def offset_now(self):
        return 4

    def get_csv(self, year, doc_type):
        """downloads csv from link and return Dataframe"""
        url = self.link + self.document_type_mapping[doc_type] + str(year) + '.csv'
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        df = pd.read_csv(io.BytesIO(r.content))
        df = self.format_df(df)
        return df

    def get_csvs(self):
        dict = {}
        for doc_type in self.document_type_mapping.keys():
            dict[doc_type] = pd.DataFrame()
            for year in [self.export_datetime.year - 1, self.export_datetime.year]:
                df = self.get_csv(year, doc_type)
                dict[doc_type] = dict[doc_type].append(df, ignore_index=True)
        dict['doc1'] = self.format_df1(dict['doc1'])
        dict['doc2'] = self.format_df2(dict['doc2'])
        return dict

    def format_df1(self, df):
        """Formats DF containing Demand forecast, Generation Total and Prices"""
        df = df.rename(columns={'Load Forecast (MW)': 'Demand',
                                'Total Generation (MW)': 'Generation Total',
                                'Final Price ($/MWh)': 'Prices'
                                })
        df = df.drop(['Forecast As At', 'Scheduled Generation (MW)', 'Non-Scheduled Generation (MW)'], axis=1)
        df = pd.melt(df, id_vars=['local_datetime', 'local_date', 'Export Date', 'Country',
                                  'Region', 'Product', 'Source'],
                     value_vars=['Demand', 'Generation Total',
                                 'Prices'], var_name='Metric', value_name='Value')
        df["Flow 1"] = np.nan
        df.loc[df["Metric"] == "Demand", "Flow 1"] = "Forecast"
        df.loc[df["Metric"] == "Prices", "Flow 1"] = "Spot"
        df["Flow 2"] = np.nan
        df.loc[df["Metric"] == "Prices", "Flow 2"] = "AUD"
        return df

    def format_df2(self, df):
        """Formats DF containing Demand estimate"""
        df = df.rename(columns={'Operational Load Estimate (MW)': 'Demand'})
        df = df.drop(['Measured At'], axis=1)
        df = pd.melt(df, id_vars=['local_datetime', 'local_date', 'Export Date',
                                  'Country', 'Region', 'Product', 'Source'],
                     value_vars=['Demand'], var_name='Metric', value_name='Value')
        df["Flow 1"] = np.nan
        df.loc[df["Metric"] == "Demand", "Flow 1"] = "Estimate"
        return df

    def format_df(self, df):
        df = df.rename(columns={'Trading Interval': 'local_datetime'})
        df = df.drop(['Trading Date', 'Interval Number', 'Extracted At'], axis=1)
        df["local_date"] = pd.to_datetime(df["local_datetime"]).dt.date
        df['Export Date'] = self.export_date
        df['Country'] = 'Australia'
        df['Region'] = 'Western Australia'
        df['Source'] = 'AEMO WEM'
        df['Product'] = 'ELE'
        df = df.reset_index(drop=True)
        return df

    def get_day(self, tdate, doc_type):
        output_df = self.dict_input[doc_type].loc[self.dict_input[doc_type]["local_date"] == tdate.date()]
        output_df = output_df.reset_index(drop=True)
        self.df_dict[doc_type] = output_df.copy()
        logger.info(f'DataFrame created for {doc_type} for {tdate.date()}')

    # def get_bulk(self, date_start, date_end, doc_type):
    #     output_df = pd.DataFrame()
    #     for tdate in pd.date_range(date_start, date_end, freq='Y'):
    #         output_df = self.get_csv(tdate, doc_type)
    #         self.df_dict[doc_type] = pd.concat([self.df_dict[doc_type], output_df.copy()])
    #         self.df_dict['Flow 1'] = 'Bulk'
    #     logger.info(f'DataFrame created for {doc_type} for {tdate.year}')

    @property
    def df_dw(self):
        df_dw = self.df_all.copy()
        return df_dw

    def run_date(self, tdate):
        for doc_type in self.document_type_mapping.keys():
            self.get_day(tdate, doc_type)
            logger.info(f'Scraped for {doc_type} for {tdate.date()}')
        self.df_all = pd.concat([self.df_doc1, self.df_doc2, self.df_all])

    @property
    def df_doc1(self):
        return self.df_dict['doc1']

    @property
    def df_doc2(self):
        return self.df_dict['doc2']


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    wem_scraper = WestAustralianPowerStatsJob()
    wem_scraper.test_run(folder)

