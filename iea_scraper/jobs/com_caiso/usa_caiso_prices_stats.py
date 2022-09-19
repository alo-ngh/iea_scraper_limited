# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: DAUGY_M, SILVA_M
"""
import io
from time import sleep
from zipfile import ZipFile, BadZipFile

import pandas as pd
from datetime import datetime, timedelta
import sys
import logging
import requests

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.settings import SSL_CERTIFICATE_PATH, BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob

sys.path.append(BROWSERDRIVER_PATH)


class UsaCaisoPricesStatsJob(EdcBulkJob):
    title: str = 'USA.CAISO - American Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.base_url = "http://oasis.caiso.com/oasisapi/SingleZip?"
        self.df_prices = pd.DataFrame()
        self.query_mapping = {"DAM": "PRC_LMP",
                              "RTM": "PRC_INTVL_LMP",
                              "RTPD": "PRC_RTPD_LMP"}
        self.query_columns = ["INTERVALSTARTTIME_GMT", "INTERVALENDTIME_GMT", "OPR_DT", "OPR_HR", "OPR_INTERVAL",
                              "NODE_ID_XML", "NODE_ID", "NODE", "MARKET_RUN_ID", "LMP_TYPE", "XML_DATA_ITEM",
                              "PNODE_RESMRID", "GRP_TYPE", "POS", "MW", "GROUP"]
        self.caiso_nodes = {"SP15- Los Angeles Area": "TH_SP15_GEN-APND",
                            "NP15- San Francisco Area": "TH_NP15_GEN-APND",
                            "ZP26- Central Generation Area": "TH_ZP26_GEN-APND",
                            "Palo Verde Intertie": "PALOVRDE_ASR-APND", 
                            "California Oregon Intertie": "MALIN_5_N101"}

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.df_prices
        return df_dw

    def get_response(self, tdate, node):
        """
        Creates query url for param node and date. The response url is a link to a ZipFile. The data is stored in a
        csv within the ZipFile
        if status_code is 429, then waits 7 seconds before retrying the query. Exits when status_code is 200
        @param tdate: datetime
        @param node: str
        @return: request.response
        """
        params = {"queryname": "PRC_LMP",
                  "market_run_id": "DAM",
                  "startdatetime": tdate.strftime("%Y%m%dT00:00-0000"),
                  "enddatetime": (tdate + timedelta(days=1)).strftime("%Y%m%dT00:00-0000"),
                  "version": 1,
                  "node": self.caiso_nodes[node],
                  "resultformat": 6}
        successful_request = False
        retries = 0
        while not successful_request and retries < 10:
            resp = requests.get(self.base_url, params=params, timeout=20, verify=SSL_CERTIFICATE_PATH)
            if resp.status_code == 200:
                successful_request = True
            elif resp.status_code == 429:
                logger.warning("USA CAISO: too many requests, wait 7 seconds")
                sleep(7)
                retries += 1
        if retries == 9:
            raise EdcJobError(f"USA CAISO: No data available for {tdate.date()}")
        return resp

    def get_data_from_zip(self, response):
        """
        Reads csv in ZipFile and stores it into a DataFrame
        @param response: requests.response
        @return: DataFrame
        """
        df = pd.DataFrame()
        with io.BytesIO() as buffer:
            try:
                buffer.write(response.content)
                buffer.seek(0)
                z = ZipFile(buffer)

            except BadZipFile as e:
                logger.warning("Bad zip file", e)
            else:
                csv = z.open(z.namelist()[0])  # ignores all but first file in zip
                df = pd.read_csv(csv, parse_dates=True)
        return df

    def format_data(self, df, node):
        """
        Formats data from csv
        @param df: DataFrame
        """
        df = df.loc[df['LMP_TYPE'] == 'LMP']
        df = df.rename(columns={'INTERVALSTARTTIME_GMT': 'local_datetime', 'NODE': 'Flow 4', 'MW': 'Value'})
        df = df[['local_datetime', 'Flow 4', 'Value']]
        df['Flow 4'] = node
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df['utc_datetime'] = df['local_datetime'].dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Country'] = 'United States'
        df['Source'] = 'CAISO'
        df['Metric'] = 'Prices'
        df['Flow 2'] = 'USD'
        df['Product'] = 'ELE'
        df['Region'] ='California'
        df['Export Date'] = self.export_datetime
        self.df_prices = pd.concat([df, self.df_prices])

    def run_date(self, tdate):
        for node in self.caiso_nodes:
            response = self.get_response(tdate, node)
            df_response = self.get_data_from_zip(response)
            self.format_data(df_response, node)


if __name__ == '__main__':
    usa_prices_stats = UsaCaisoPricesStatsJob()
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    tdate = datetime(2021, 9, 6)
    usa_prices_stats.test_run(folder)
