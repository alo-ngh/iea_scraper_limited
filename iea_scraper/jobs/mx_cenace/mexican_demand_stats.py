# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: TAV_M, DAUGY_Mn CHAMBEAU_L
"""
import glob
import os

import pandas as pd
import numpy as np
from datetime import datetime
from time import sleep
import locale
import logging
import sys

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
sys.path.append(r'C:\Repos\iea_scraper')

from iea_scraper.core.job import EdcBulkJob
from iea_scraper.jobs.utils import get_driver, FILE_STORE_PATH
from iea_scraper.settings import BROWSERDRIVER_PATH
from iea_scraper.core.exceptions import EdcJobError

sys.path.append(BROWSERDRIVER_PATH)


class MexicanDemandStatsJob(EdcBulkJob):
    title: str = 'Mexico.Cenace - Mexico Demand Statistics'
    def __init__(self):
        super().__init__(self)
        self.driver = get_driver(headless=True)
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        self.url = 'https://www.cenace.gob.mx/Paginas/SIM/Reportes/EstimacionDemandaReal.aspx'
        self.input_df = None #has generation_total data has well
        self.output_df = {'Demand': pd.DataFrame(),
                           'Generation Total': pd.DataFrame()}
        self.file_store_path = os.path.realpath(FILE_STORE_PATH)
        self.xpath_inputdate =  '//*[@id="ctl00_ContentPlaceHolder1_RadDatePickerVisualizarPorBalance_dateInput"]'
        self.tbody_xpath =  'PorBalance'
        self.table_row = 0
        self.filename_string = 'Demanda Real Balance'

    @property
    def offset_now(self):
        return 16

    def choose_date(self, tdate):
        '''
        This methods inputs in the correct format the date for which the data is collected on the webpage
        :param tdate: datetime
        :return: input date
        '''
        xpath_inputdate = self.xpath_inputdate
        WebDriverWait(self.driver, 3).until(EC.visibility_of_element_located((By.XPATH, xpath_inputdate))).clear()
        input_element = WebDriverWait(self.driver, 3).until(
            EC.visibility_of_element_located((By.XPATH, xpath_inputdate)))
        input_element.send_keys(tdate.strftime("%d/%m/%Y"))
        input_element.send_keys(Keys.RETURN)
        sleep(3)

    def click_csv_img(self, iteration):
        '''
        :return: download csv for chosen month (offset : 1 month for generation, 16 days for demand)
        '''
        rows = self.driver.find_elements_by_xpath(f'//*[@id="ctl00_ContentPlaceHolder1_GridRad{self.tbody_xpath}_ctl00"]/tbody/tr')
        self.table_row = len(rows) - (iteration)
        if iteration == 1:
            xpath_tbody = f'//*[@id="ctl00_ContentPlaceHolder1_GridRad{self.tbody_xpath}_ctl00"]/tbody/tr[last()]/td[3]/input'
        if iteration > 1:
            xpath_tbody = f'//*[@id="ctl00_ContentPlaceHolder1_GridRad{self.tbody_xpath}_ctl00"]/tbody/tr[{self.table_row +1}]/td[3]/input'
        sleep(3)
        csv_img = WebDriverWait(self.driver, 3).until(EC.visibility_of_element_located((By.XPATH, xpath_tbody)))
        csv_img.click()
        sleep(3)

    def get_data_for_date(self, tdate):
        '''
        This method opens the webpage and selects the correct date
        :param tdate: datetime
        '''
        self.driver.get(self.url)
        sleep(3)
        self.choose_date(tdate)

    def download_data_from_url(self, tdate, iteration):
        '''
        This method downloads data in csv format and stores it in FILE_STORE_PATH
        :param tdate: datetime
        :return: data in csv format in FILE_STORE_PATH
        '''
        try:
            self.click_csv_img(iteration)
        except:
            logger.info(f'Demand data not downloaded on {self.export_datetime}')

    def download_csv_from_filestore(self, tdate):
        '''
        wait for file to appear in filestore directory before calling method to extract data from csv

        :param tdate: datetime
        '''
        filtered_files = glob.glob(self.file_store_path + f'/{self.filename_string}_{self.table_row}*.csv')
        date_string = datetime.strftime(tdate, "%Y-%m-%d")

        iteration = 0
        file_found = False
        while not file_found and iteration < 40:
            sleep(1)
            for filename in filtered_files:
                if filename.find(date_string) != -1:
                    self.get_data_from_csv(filename)
                    file_found = True
            iteration += 1
        if iteration == 40:
            raise EdcJobError(f"Could not find Demand for {tdate.date()} in filestore")

    def get_data_from_csv(self, filename):
        '''
        extracts data from csv in filestore directory and store it in input_df
        :param tdate: datetime
        :param filename: string
        :return: input_dfs[metric]
        '''
        header = 1
        df = pd.read_csv(filename, sep=',', skiprows=7, header=header)
        self.input_df = df

    def format_demand_generation_total(self, tdate):
        '''
        Formats input_dfs['Demand'] and fills in output_dfs['Demand'] and output_dfs['Generation Total']
        :param tdate: datetime
        '''
        metric1 = 'Demand'
        metric2 = 'Generation Total'
        df = self.input_df.copy()
        df = df.rename(columns=lambda x: x.strip())
        df['Region'] = df['Sistema'].map(str) + '-' + df['Area'].map(str)
        df['local_date'] = tdate
        df['local_datetime'] = df['local_date'] + pd.to_timedelta((df['Hora'] - 1), unit='h')
        df = df.drop(columns={'Sistema', 'Hora', 'Area', 'Importacion Total (MWh)', 'Exportacion Total (MWh)',
                              'Intercambio neto entre Gerencias (MWh)'})
        df = df.rename(columns={'Generacion (MWh)': 'Generation Total',
                                'Estimacion de Demanda por Balance (MWh)': 'Demand'})
        df = pd.melt(df, id_vars=['local_date', 'local_datetime', 'Region'],
                     var_name='Metric', value_name='Value')
        df["Flow 1"] = np.nan
        df.loc[df["Metric"] == "Demand", "Flow 1"] = "Estimate"
        df['Product'] = 'ELE'
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize(
            'America/Mexico_City', nonexistent='shift_forward', ambiguous='NaT').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].apply(lambda x: x.date())
        df['Export Date'] = self.export_datetime
        df['Country'] = 'Mexico'
        df['Source'] = 'cenace.gob.mx'
        self.output_df[metric1] = pd.concat([self.output_df[metric1], df.loc[df["Metric"] == "Demand"]],
                                            ignore_index=True)
        self.output_df[metric2] = pd.concat([self.output_df[metric2], df.loc[df["Metric"] == "Generation Total"]],
                                            ignore_index=True)
        self.input_df = None
        logger.info(f'Demand data downloaded and formatted for {tdate}')

    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_demand, self.df_generation_total])
        return df_dw

    @property
    def df_demand(self):
        return self.output_df['Demand']

    @property
    def df_generation_total(self):
        return self.output_df['Generation Total']

    def run_date(self, tdate):
        iteration = 1
        self.driver.close()
        self.driver = get_driver(headless=True)
        self.get_data_for_date(tdate)
        while self.input_df is None:
            self.download_data_from_url(tdate, iteration)
            self.download_csv_from_filestore(tdate)
            iteration += 1
        self.format_demand_generation_total(tdate)


if __name__ == '__main__':
    folder = r'C:\Repos\iea_scraper\csvs'
    mx_scraper = MexicanDemandStatsJob()
    mx_scraper.test_run()
    # date_list = [datetime(2022,4,29), datetime(2022,4,30)]
    # for date in date_list:
        # mx_scraper.run_date(tdate=date)
    
