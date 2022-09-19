# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: DAUGY_M
"""
import glob
import io
from time import sleep
from zipfile import ZipFile
import pandas as pd
from datetime import datetime, timedelta
import sys
import logging
import requests
import numpy as np
from bs4 import BeautifulSoup

from iea_scraper.core import factory
from iea_scraper.settings import SSL_CERTIFICATE_PATH, FILE_STORE_PATH, BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core.exceptions import EdcJobError

sys.path.append(BROWSERDRIVER_PATH)


class UsaErcotPricesStatsJob(EdcBulkJob):
    title: str = 'USA.ERCOT - American Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.hub_list = {'HB_BUSAVG': np.nan,
                         'HB_HOUSTON': 'Houston Area',
                         'HB_HUBAVG': np.nan,
                         'HB_NORTH': 'North Area',
                         'HB_PAN': 'Panhandle Area',
                         'HB_SOUTH': 'South Area',
                         'HB_WEST': 'West Area'
                         }
        self.urls = {'live': 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12300&mimic_duns=000000000',
                     'bulk': 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13060&mimic_duns=000000000'}
        self.df_prices = pd.DataFrame()
        self.json_items = {}
        self.all_zip_links = {'live': self.get_data('live'),
                              'bulk': self.get_data('bulk')
                              }

        self.bulk_downloaded = False

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.df_prices
        return df_dw

    def get_data(self, key):
        '''
        This method extracts the data from the PJM website using a GET request.
        Depending on the key ('live' or 'bulk') the url is different.
        This method is the first to run to store all available download urls in the dictionary all_zip_links
        :return: dictionary
        '''
        if key not in ['live', 'bulk']:
            raise EdcJobError("Key has to be 'live' or 'bulk'")
        else:
            r = requests.get(self.urls[key], verify=SSL_CERTIFICATE_PATH)
            html_data = r.text
            soup = BeautifulSoup(html_data, features='lxml')
            download_links = {}
            for row in soup.find_all('tr'):
                zip_format = 'csv' if key == 'live' else 'zip'
                if zip_format in str(row) and 'href' in str(row):
                    if key == 'live':
                        date = row.find(attrs={'class': 'labelOptional_ind'}).getText().split('.')[3]
                        time = row.find(attrs={'class': 'labelOptional_ind'}).getText().split('.')[4][:4]
                        date_time = datetime.strptime(date + ' ' + time, "%Y%m%d %H%M")
                    elif key == 'bulk':
                        date_time = row.find(attrs={'class': 'labelOptional_ind'}).getText().split('.')[5][-4:]

                    link = 'http://mis.ercot.com/' + row.find('a').get('href')
                    download_links[date_time] = link

            if key == 'live':
                hourly_links = {}
                for dt in download_links:
                    if dt.minute == 0 or dt.minute == 1:
                        hourly_links[dt.replace(minute=0)] = download_links[dt]
                download_links = hourly_links
        return download_links

    def get_data_for_date(self, tdate, key='live'):
        """
        this method is used to extract data from the previously downloaded zip archives. the parameter key is by default
        'live' as we assume that we are extracting the latest data available
        1. check if tdate is in all_zip_links['live'].
            - if not, data is extracted from all_zip_links['bulk']
            - key is set to 'bulk', which is used in parameter of format function
        2. if data in all_zip_links['live']:
            - filter dictionary to get one datetime per hour for tdate
            - request url, if request timeout wait 10 seconds before re-querying
            - extract csv from zipfile
            - store in DataFrame
        @param tdate: datetime
        @param key: str
        @return: DataFrame, str. 'live' or 'bulk'
        """
        df = pd.DataFrame()
        dates_available = list(set([dt.date() for dt in self.all_zip_links[key]]))
        if tdate.date() not in dates_available:
            df = self.get_data_from_bulk_archive(tdate)
            key = 'bulk'
        else:
            response = 0
            for hour in range(0, 24):
                date = tdate.replace(hour=hour, minute=0, microsecond=0, second=0)
                while not response == 200:
                    resp = requests.get(self.all_zip_links[key][date], verify=SSL_CERTIFICATE_PATH)
                    response = resp.status_code
                    if response != 200:
                        sleep(10)
                response = 0
                zip = ZipFile(io.BytesIO(resp.content))
                df_hour = pd.read_csv(zip.open(zip.namelist()[0]))
                df_hour['local_datetime'] = date
                df = pd.concat([df, df_hour])
        return df, key

    def check_filestore(self, tdate):
        '''
        This method loops in the generation files in FILE_STORE_PATHS to check if the csv for the given month
        has already been downloaded.
        If the file is not in the directory the parameter generation_updated is reset to False
        :param tdate: datetime
        :return: bulk_downloaded = True or False
        '''

        filtered_files = glob.glob(str(FILE_STORE_PATH) + '/*DAMLZHBSPP_*.xlsx')
        for filename in filtered_files:
            if str(tdate.year) not in filename:
                self.bulk_downloaded = False
                continue
            elif str(tdate.year) in filename:
                self.bulk_downloaded = True
                break

    def get_data_from_bulk_archive(self, tdate, key='bulk'):
        """
        Data collected from bulk url.
        This method will extract the data for the parameter date in from the relevant url using xlwings and store
        1. check filestore
        2. if excel file found:
            - open workbook in filestore
        3. if not found:
            - request all_zip_links['bulk'][year]
            - extract excel to filestore
        4. when workbook extracted/found:
            - open worksheet corresponding to tdate.month
            - store data in df
        @param tdate: datetime
        @param key: str
        @return: DataFrame
        """
        response = 0
        self.check_filestore(tdate)
        tdate_available = self.check_last_date_in_df(tdate)
        if not self.bulk_downloaded or not tdate_available:
            while not response == 200:
                resp = requests.get(self.all_zip_links[key][str(tdate.year)], verify=SSL_CERTIFICATE_PATH)
                response = resp.status_code
                if response != 200:
                    sleep(10)
            zip = ZipFile(io.BytesIO(resp.content))
            zipname = zip.namelist()[0]
            zip.extract(zipname, FILE_STORE_PATH)
            iteration = 0
            xl_found = False
            while not xl_found and iteration <= 5:
                if not glob.glob(str(FILE_STORE_PATH) + '/' + zipname):
                    sleep(2)
                    iteration += 1
                else:
                    xl_found = True
            xlname = glob.glob(str(FILE_STORE_PATH) + '/' + zipname)[0]
        else:
            xlname = glob.glob(str(FILE_STORE_PATH) + f'/*DAMLZHBSPP_{str(tdate.year)}.xlsx')[0]

        df = pd.read_excel(xlname, sheet_name=tdate.month-1)
        # os.remove(os.path.join(FILE_STORE_PATH, xlname))
        return df

    def check_last_date_in_df(self, tdate):
        """
        This method is used to verify what the last date available in the archive document is.
        It could be removed if filestore is cleaned after EdcBulkJob runs. In the meantime, if the archive excel is
        in the filestore, we need to check if tdate is available or else archive needs to be re-opened.
        @param tdate: datetime
        @return: boolean
        """
        tdate_available = True
        if self.bulk_downloaded and tdate >= datetime.now().replace(day=1) - timedelta(days=1):
            xlname = glob.glob(str(FILE_STORE_PATH) + f'/*DAMLZHBSPP_{str(tdate.year)}.xlsx')[0]
            df_month = pd.read_excel(xlname, sheet_name=tdate.strftime("%b"))
            date_list = [datetime.strptime(dt, "%m/%d/%Y") for dt in df_month['Delivery Date'].unique().tolist()]
            if tdate not in date_list:
                tdate_available = False
        return tdate_available

    def format_live_data(self, df):
        """
        format data downloaded from live url
        @param df: DataFrame
        @return: DataFrame
        """
        df = df.loc[df['SettlementPoint'].isin(self.hub_list)]
        df = df.rename(columns={'SettlementPoint': 'Flow 4', 'LMP': 'Value'})
        df = df.drop(columns=['SCEDTimestamp', 'RepeatedHourFlag'])
        return df

    def format_bulk_data(self, df, tdate):
        """
        format data downloaded from bulk url
        @param df: DataFrame
        @param tdate: datetime
        @return: DataFrame
        """
        df = df.loc[df['Settlement Point'].isin(self.hub_list)]
        df = df.rename(columns={'Settlement Point': 'Flow 4', 'Settlement Point Price': 'Value',
                                'Delivery Date': 'local_datetime'})
        df = df.loc[df['local_datetime'] == tdate.strftime("%m/%d/%Y")]
        df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['local_datetime']
                                                                    + " " + str(int(x['Hour Ending'][:2]) - 1),
                                                                    "%m/%d/%Y %H"), axis=1)
        df = df.drop(columns=['Hour Ending', 'Repeated Hour Flag'])
        return df

    def format_data(self, df, tdate, key):
        """
        Format df to match DW
        @param df: DataFrame
        @param tdate: datetime
        @param key: str
        @return: DataFrame
        """
        if key == 'live':
            df = self.format_live_data(df)
        elif key == 'bulk':
            df = self.format_bulk_data(df, tdate)

        df['local_date'] = df['local_datetime'].dt.date
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize('America/Chicago',
                                                                 nonexistent='shift_forward',
                                                                 ambiguous='NaT').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Flow 4'] = df['Flow 4'].map(self.hub_list)
        df = df.dropna()
        df['Region'] = 'Texas'
        df['Country'] = 'United States'
        df['Metric'] = 'Prices'
        df['Source'] = 'ERCOT'
        df['Product'] = 'ELE'
        df['Flow 2'] = 'USD'
        self.df_prices = pd.concat([df, self.df_prices])

    def run_date(self, tdate):
        df, key = self.get_data_for_date(tdate)
        self.format_data(df, tdate, key)
        logger.info(f'USA ERCOT: Prices scraped for {tdate.date()}')


if __name__ == '__main__':
    scraper = UsaErcotPricesStatsJob()
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper.test_run(folder)
    scraper_test= factory.get_scraper_job('com_ercot','usa_ercot_prices_stats')
