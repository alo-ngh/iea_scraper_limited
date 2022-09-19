# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: TAV_M, DAUGY_M
"""
import glob
import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import sleep
import locale
import logging
import sys

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
sys.path.append(r'C:\Repos\scraper')

from iea_scraper.core.job import EdcBulkJob
from iea_scraper.jobs.utils import get_driver, FILE_STORE_PATH
from iea_scraper.settings import BROWSERDRIVER_PATH
from iea_scraper.core.exceptions import EdcJobError

sys.path.append(BROWSERDRIVER_PATH)

class MexicanGenerationStatsJob(EdcBulkJob):
    title: str = 'Mexico.Cenace - Mexico Generation Statistics'

    def __init__(self):
        super().__init__(self)
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        self.driver = get_driver(headless=True)
        self.url = 'https://www.cenace.gob.mx/Paginas/SIM/Reportes/EnergiaGeneradaTipoTec.aspx'
        self.input_df = pd.DataFrame()
        self.output_df = pd.DataFrame()
        self.fuel_mapping = {'Eolica': 'Wind Onshore',
                             'Fotovoltaica': 'Solar',
                             'Biomasa': 'Biomass',
                             'Carboelectrica': 'Coal',
                             'Ciclo Combinado': 'Thermal',
                             'Combustion Interna': 'Thermal',
                             'Geotermoelectrica': 'Geothermal',
                             'Hidroelectrica': 'Hydro',
                             'Nucleoelectrica': 'Nuclear',
                             'Termica Convencional': 'Thermal',
                             'Turbo Gas': 'Thermal'}
        self.file_store_path = os.path.realpath(FILE_STORE_PATH)
        self.xpath_inputdate = '//*[@id="ctl00_ContentPlaceHolder1_FechaConsulta_dateInput"]'
        self.tbody_xpath = 'Resultado'
        self.filename_string = 'Generacion Liquidada'
        self.generation_updated = False
        self.export_days = range(1,32) #[6,15, 20, 28]  # will only export on those days

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
        WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath_inputdate))).clear()
        input_element = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, xpath_inputdate)))

        tdate_month_before = (tdate.replace(day=1) - timedelta(days=1)).replace(day=1) #remove one month
        input_element.send_keys(tdate_month_before.strftime('"%m/%d/%Y"'))

        # Click outside to trigger the validation (no submit button)
        outside_path = '//*[@id="labelMensajeDescarga"]'
        WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, outside_path))).click()
        sleep(20)

    def click_csv_img(self):
        '''
        :return: download csv for chosen month (offset : 1 month for generation, 16 days for demand)
        '''
        xpath_tbody = f'//*[@id="ctl00_ContentPlaceHolder1_GridRad{self.tbody_xpath}_ctl00"]/tbody/tr[last()]/td[3]/input'
        csv_img = WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located((By.XPATH, xpath_tbody)))
        csv_img.click()
        sleep(10)

    def download_data_from_url(self, tdate):
        '''
        This method launches the Selenium part of this scraper. Once on the webpage, it will input the correct date
        and click on the csv icon to download the latest data available. It then store it in the filestore
        :param tdate: datetime
        :return:
        '''
        if not self.generation_updated:
            self.get_csv(tdate)
            self.generation_updated = True

    def get_csv(self, tdate):
        '''
        This method downloads data in csv format and stores it in FILE_STORE_PATH
        :param tdate: datetime
        :return: data in csv format in FILE_STORE_PATH
        '''
        try:
            self.driver.get(self.url)
            sleep(5)
            self.choose_date(tdate)
            self.click_csv_img()
            logger.info(f'Generation data scraped for {tdate}')
        except:
            logger.info(f'Generation data not scraped on {tdate}')


    def download_csv_from_filestore(self, tdate):
        '''
        wait for file to appear in filestore directory before calling method to extract data from csv
        :param tdate: datetime
        '''

        filtered_files = glob.glob(self.file_store_path + f'/{self.filename_string}*.csv')
        date_string = (tdate.replace(day=1) - timedelta(days=1)).strftime('%B %Y')

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
            raise EdcJobError(f"Could not find Generation for {tdate.date()} in filestore")

    def get_data_from_csv(self, filename):
        '''
        extracts data from csv in filestore directory and store it in input_df
        :param tdate: datetime
        :param filename: string
        :return: input_dfs[metric]
        '''
        header = 0
        df = pd.read_csv(filename, sep=',', skiprows=7, header=header)
        self.input_df = df

    def format_generation(self):
        '''
        formats input_df and updates output_df
        '''
        df = self.input_df.copy()
        df = df.rename(columns=lambda x: x.strip())
        df['local_date'] = pd.to_datetime(df['Dia'], format=('%d/%m/%Y'))
        df['local_datetime'] = df['local_date'] + pd.to_timedelta((df['Hora'] - 1), unit='h')
        df = df.drop(columns={'Sistema', 'Hora', 'Dia'})
        df = pd.melt(df, id_vars=['local_date', 'local_datetime'],
                     var_name='Product', value_name='Value')
        df['Product'] = df['Product'].apply(lambda x: self.fuel_mapping[x])
        df['Metric'] = 'Generation'
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize(
            'America/Mexico_City',  nonexistent='shift_forward', ambiguous='NaT').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].apply(lambda x: x.date())
        df['Export Date'] = self.export_datetime
        df['Country'] = 'Mexico'
        df['Source'] = 'cenace.gob.mx'
        return df

    @property
    def df_dw(self):
        df_dw = self.output_df
        return df_dw

    def check_filestore(self, tdate):
        '''
        This method loops in the generation files in FILE_STORE_PATHS to check if the csv for the given month
        has already been downloaded.
        If the file is not in the directory the parameter generation_updated is reset to False
        :param tdate: datetime
        :return: generation_updated = True or False
        '''

        date_string = (tdate.replace(day=1) - timedelta(days=1)).strftime('%B %Y')
        filtered_files = glob.glob(self.file_store_path + f'/{self.filename_string}*.csv')
        for filename in filtered_files:
            if date_string not in filename:
                self.generation_updated = False
                continue
            elif date_string in filename:
                self.generation_updated = True
                break

    def run_date(self, tdate):
        if self.export_datetime.day in self.export_days:
            self.check_filestore(tdate)
            self.download_data_from_url(tdate)
            self.download_csv_from_filestore(tdate)
            self.output_df = pd.concat([self.output_df, self.format_generation()])



if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    mx_scraper = MexicanGenerationStatsJob()
    date = datetime(2020,4,1)
    mx_scraper.test_run(folder, historical=False)
