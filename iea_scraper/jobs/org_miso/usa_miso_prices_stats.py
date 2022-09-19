# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import io
import requests
from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import xlrd
import numpy as np


from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS, FILE_STORE_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob


class UsaMisoPricesStatsJob(EdcBulkJob):
    title: str = 'MISO.USA - US Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.miso_url = "https://docs.misoenergy.org/marketreports/%Y%m%d_da_pr.xls"
        self.hub_mapping = {'MISO System': np.nan,
                            'Illinois Hub': 'Illinois',
                            'Michigan Hub': 'Michigan',
                            'Minnesota Hub': 'Minnesota',
                            'Indiana Hub': 'Indiana',
                            'Arkansas Hub': 'Arkansas',
                            'Louisiana Hub': 'Louisiana',
                            'Texas Hub': 'Texas',
                            'MS.HUB': 'Mississippi'}
        self.excel_col_mapping = {1:'MISO System',
                            2:'Illinois Hub',
                            3:'Michigan Hub',
                            4:'Minnesota Hub',
                            5:'Indiana Hub',
                            6:'Arkansas Hub',
                            7:'Louisiana Hub',
                            8:'Texas Hub',
                            9:'MS.HUB'}
        self.output_df = pd.DataFrame()

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.output_df
        return df_dw

    def get_data_from_excel(self, filename: str):
        '''
        this method is used to parse an xls file using the xlrd library
        the method parses the sheet by looking at each row. If the cell value starts with Hour, the values for all
        columns are stored in a data_point dict then added to the all_data_points list
        @param filename: string (path to file located in the filestore)
        @return: list
        '''
        book = xlrd.open_workbook(filename)
        sheet = book.sheets()[0]
        rows = sheet.nrows
        cols = sheet.ncols
        all_data_points = []
        for row in range(rows):
            data_point = {}
            value = sheet.cell_value(row, 0)
            if value.startswith('Hour '):
                data_point = {'Hour': int(value[-2:]) - 1}
                for col in range(1, cols):
                    data_point[self.excel_col_mapping[col]] = float(sheet.cell_value(row, col))
                all_data_points += [data_point]
        return all_data_points

    def get_prices(self, tdate):
        '''
        This method creates the xls url using the date in parameter and downloads the data from the Excel file
        '''
        link = tdate.strftime(format=self.miso_url)
        r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        with open(FILE_STORE_PATH / 'miso_data.xls' , 'wb') as output:
            output.write(r.content)
            df = pd.DataFrame(self.get_data_from_excel(output.name))
            output.close()
        return df

    def format_df(self, df, tdate):
        '''
        This method formats the downloaded data to the DW format
        '''
        df = df.copy()

        df = pd.melt(df, id_vars=['Hour'], value_name='Value', var_name='Flow 4')
        df['local_date'] = tdate.strftime("%Y-%m-%d")
        df['local_datetime'] = df['Hour'].apply(lambda x: tdate.replace(hour=x,
                                                                        minute=0, second=0, microsecond=0))
        df['Flow 4'] = df['Flow 4'].map(self.hub_mapping, na_action='ignore')
        df = df[df['Flow 4'].notna()]
        df['Region'] ='Midwest'
        df['Metric'] = 'Prices'
        df['Export Date'] = self.export_datetime
        df['Flow 2'] = 'USD'
        df['Country'] = 'United States'
        df['Source'] = 'MISO'
        df['Product'] = 'ELE'
        df = df.drop(columns=['Hour'])
        self.output_df = pd.concat([df, self.output_df])

    def run_date(self, tdate):
        df = self.get_prices(tdate)
        self.format_df(df, tdate)


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    miso = UsaMisoPricesStatsJob()
    tdate = datetime(2021, 9, 12)
    miso.test_run(folder)
