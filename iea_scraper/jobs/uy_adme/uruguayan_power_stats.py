"""
Created on Mon Oct 18, 2021
``
@author: DAUGY_M

Uruguayan power data:
- Power Generation per fuel source
- Power Demand
- Power Prices

"""
import io

import requests
import pandas as pd
from datetime import timedelta
import logging
import sys
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil.parser import parse


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\iea_scraper')

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core import factory
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS


class UruguayanPowerStatsJob(EdcBulkJob):
    title: str = 'ADME.com.uy - Uruguay Power  statistics'

    def __init__(self):
        super().__init__(self, config='electricity')
        self.url_gen_dem = "https://pronos.adme.com.uy/gpf.php?fecha_ini="
        self.url_prices = 'https://adme.com.uy/mmee/spot/spotSancionadoDetalle.php?remota=1&a={year}&m={month:02d}'
        self.input_df = None
        self.product_mapping = {'Salto Grande': 'Hydro',
                                'Bonete': 'Hydro',
                                'Baygorria': 'Hydro',
                                'Palmar': 'Hydro',
                                'Eólica': 'Wind Onshore',
                                'Solar': 'Solar',
                                'Térmica': 'Thermal',
                                'Biomasa': 'Biomass',
                                'Demanda': 'ELE'}
        self.df_prices = pd.DataFrame()
        self.df_gen_dem = pd.DataFrame()
        self.data_url = None

    def get_data_url(self, tdate: datetime):
        """
        requests date url and parses response contents to extract the link to the source file
        @param tdate: datetime
        @return: str
        """
        next_day = tdate + timedelta(days=1)
        date_format = "{day}%2F{month}%2F{year}".format(day=tdate.day,
                                                        month=tdate.month,
                                                        year=tdate.year)
        next_day_format = "{day}%2F{month}%2F{year}".format(day=next_day.day,
                                                            month=next_day.month,
                                                            year=next_day.year)
        link = self.url_gen_dem + date_format \
               + "&fecha_fin=" + next_day_format + "&send=MOSTRAR"
        r = requests.get(url=link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        soup = BeautifulSoup(r.content, 'html.parser')
        href_tags = soup.find_all('a', href=True)
        for tag in href_tags:
            if tag.button is not None:
                if tag.button.string == "Archivo Scada Detalle Horario":
                    if tag.get('href') !="":
                        self.data_url = "https://pronos.adme.com.uy" + tag.get('href')
                    else:
                        logger.warning(f"Uruguay: demand and generation data not available for {tdate.date()}")


    def get_demand_generation_data_from_url(self, tdate: datetime):
        """
        gets data from link to source file
        source file in ODT format
        @param tdate: datetime
        @return: pd.DataFrame
        """
        self.get_data_url(tdate)
        if self.data_url is not None:
            r = requests.get(url=self.data_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            if r.status_code == 200:
                df_data = pd.read_excel(io.BytesIO(r.content), engine='odf', header=2)
                self.input_df = df_data
            else:
                raise EdcJobError(f"Uruguay: demand and generation data not available for {tdate.date()}")

    def format_demand_generation_data(self):
        """
        formats demand and generation data
        @return: pd.DataFrame
        """
        if self.input_df is not None:
            df = self.input_df.copy()
            df = df.rename(columns={'Fecha': 'local_datetime'})
            df = pd.melt(df, id_vars=['local_datetime'],
                         var_name='Product',
                         value_name='Value')
            df['Product'] = df['Product'].map(self.product_mapping)
            df['local_datetime'] = pd.to_datetime(df['local_datetime'])
            df['local_date'] = df['local_datetime'].dt.date
            df = df.loc[~df['Product'].isnull()]
            df = df.groupby(['local_datetime', 'Product']).sum()['Value'].reset_index()
            df['Metric'] = df['Product'].apply(lambda x: "Demand" if x == "ELE" else "Generation")
            df['Country'] = 'Uruguay'
            df['Source'] = 'ADME'
            self.df_gen_dem = pd.concat([self.df_gen_dem, df])

    def get_prices_data(self, tdate):
        '''
        get prices data from url
        spot prices are available with more lag that generation and demand (it looks like they are published on a
        monthly basis)
        @param tdate: datetime
        @return: pd.DataFrame or warning
        '''

        r = requests.get(self.url_prices.format(year=tdate.year, month=tdate.month))
        if r.status_code == 200 and "failed" not in r.text:
            soup = BeautifulSoup(r.content, 'html.parser')
            table = soup.find(attrs={'class': 'table'})
            table_rows = table.find_all('tr', attrs={'align': 'center'})
            all_data_points = []
            for tr in table_rows:
                table_cols = tr.find_all('td')
                for td in range(1, len(table_cols)):
                    td_datetime = parse(table_cols[0].string, fuzzy=True).replace(hour=td - 1)
                    data_point = {'local_datetime': td_datetime,
                                  'local_date': td_datetime.date(),
                                  'Value': float(table_cols[td].string.replace(',','.')),
                                  'Metric': 'Prices',
                                  'Product': 'ELE',
                                  'Country': 'Uruguay',
                                  'Source': 'ADME',
                                  'Flow 2': 'USD'}
                    all_data_points += [data_point]
            df_all_prices = pd.DataFrame(all_data_points)
            df_prices_tdate = df_all_prices.loc[df_all_prices['local_date'] == tdate.date()]
            self.df_prices = pd.concat([self.df_prices, df_prices_tdate])
        else:
            logger.warning(f"Uruguay: prices not availble for {tdate.date()}")

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        return pd.concat([self.df_gen_dem, self.df_prices])

    def run_date(self, tdate):
        self.get_demand_generation_data_from_url(tdate)
        self.format_demand_generation_data()
        self.get_prices_data(tdate)

if __name__ == '__main__':

    scraper = UruguayanPowerStatsJob()
    scraper.test_run(folder=r'C:\Repos\scraper\csvs', historical=True)
