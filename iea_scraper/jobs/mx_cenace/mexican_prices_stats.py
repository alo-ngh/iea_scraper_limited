# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 10:18:04 2020

@author: TAV_M
"""
import glob
import os

import pandas as pd
from datetime import datetime, timedelta

from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import sys
import logging
import locale

from iea_scraper.settings import FILE_STORE_PATH, BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.jobs.utils import get_driver
from time import sleep

sys.path.append(BROWSERDRIVER_PATH)


class MexicanPricesStatsJob(EdcBulkJob):
    title: str = 'Mexico.Cenace - Mexico Prices Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        self.all_data_points = []
        self.spanish_month_dict = {datetime(1900, n, 1).strftime('%B').upper(): n
                                   for n in range(1, 13)}
        self.regions_mapping = {'VDM CENTRO': 'Mexico City',
                                'GUADALAJARA': 'Guadalajara',
                                'PUEBLA': 'Puebla City',
                                'JUAREZ': 'Ciudad Juarez',
                                'MORELOS': 'Ecatepec',
                                'TIJUANA': 'Tijuana',
                                'LA PAZ': 'La Paz'}
        self.driver = get_driver(headless=True)
        self.old_url = 'https://www.cenace.gob.mx/Paginas/SIM/Reportes/H_PreciosEnergiaSisMEM.aspx?N=26&opc=divCssPreEnergia&site=Precios%20de%20la%20energ%C3%ADa/Precios%20de%20Nodos%20Distribuidos/MDA/Diarios&tipoArch=C&tipoUni=SIN&tipo=Diarios&nombrenodop=Precios%20de%20Nodos%20Distribuidos'
        self.url = 'https://www.cenace.gob.mx/Paginas/SIM/Reportes/PreEnerServConMTR.aspx'
        self.df_prices = pd.DataFrame()
        self.input_dfs = {'SIN': pd.DataFrame(),
                          'BCA': pd.DataFrame(),
                          'BCS': pd.DataFrame()}
        self.xpath_tbody = {'SIN': '//*[@id="products"]/tbody/tr[2]/td[2]/table/tbody',
                            'BCA': '//*[@id="products"]/tbody/tr[4]/td[2]/table/tbody',
                            'BCS': '//*[@id="products"]/tbody/tr[6]/td[2]/table/tbody'}
        self.xpath_input_date = '//*[@id="ContentPlaceHolder1_txtPeriodo"]'
        self.xpath_region = '//*[@id="ContentPlaceHolder1_ddlSistema"]'
        self.xpath_report = '//*[@id="ContentPlaceHolder1_ddlReporte"]'
        self.filename_string = 'PreciosNodosDistrib'
        self.file_store_path = os.path.realpath(FILE_STORE_PATH)
        self.calendar_region = {'SIN': '5',
                                'BCA': '8',
                                'BCS': '11'}

    @property
    def offset_now(self):
        return 5

    @property
    def df_dw(self):
        df_dw = self.df_prices
        return df_dw

    def choose_day(self, tdate, region):
        '''
        This methods selects the correct date range (date-date) in the datepicker
         for which the data is collected on the webpage
        :param tdate: datetime
        :return: input date
        '''
        xpath_inputdate = self.xpath_input_date
        WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, xpath_inputdate))).click()

        year_left_elem = WebDriverWait(self.driver, 15).until(EC.visibility_of_element_located((By.XPATH,
                                                                                                '//*[@id="top"]/div[2]/div[2]/div[1]/table/thead/tr[1]/th[2]/select[2]')))
        year_left_selec = Select(year_left_elem)
        year_left_selec.select_by_visible_text(str(tdate.year))
        month_left_elem = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH,
                                                                                                '//*[@id="top"]/div[2]/div[2]/div[1]/table/thead/tr[1]/th[2]/select[1]')))
        month_left_selec = Select(month_left_elem)
        month_left_selec.select_by_value(str(tdate.month - 1))

        tbody_calendar = self.driver.find_element_by_xpath(
            '//div[@class="drp-calendar left"]')
        tbody_rows = tbody_calendar.find_elements_by_tag_name('tr')
        for row in tbody_rows:
            tbody_columns = row.find_elements_by_tag_name('td')
            for col in tbody_columns:
                if  'available' in  col.get_attribute("class"):
                    if col.text == str(tdate.day):
                        data_title = col.get_attribute("data-title")
                        tr = str(int(data_title[1]) + 1)
                        td = str(int(data_title[-1]) + 1)
                        # col.click()
                        # sleep(5)

        button_start_day = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH,
                                                                                                  f'//*[@id="top"]/div[2]/div[2]/div[1]/table/tbody/tr[{tr}]/td[{td}]')))
        button_start_day.click()
        sleep(10)
        button_end_day = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH,
                                                                                                f'//*[@id="top"]/div[2]/div[2]/div[1]/table/tbody/tr[{tr}]/td[{td}]')))
        button_end_day.click()
        sleep(10)
        button_accept = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH,
                                                                                               '//button[@class="applyBtn btn btn-sm btn-primary"]')))
        button_accept.click()
        sleep(5)

    def choose_region(self, region):
        '''
        this method updates the region in the dropdown menu on the webpage
        '''
        region_elem = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, self.xpath_region)))
        region_selec = Select(region_elem)
        region_selec.select_by_visible_text(region)
        sleep(10)

    def chose_report(self):
        '''
        This method updates the report chosen in the dropdown menu on the webpage
        '''
        report_elem = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, self.xpath_report)))
        report_selec = Select(report_elem)
        report_selec.select_by_visible_text('Precios en Nodos Distribuidos MTR')
        sleep(10)

    def click_csv_img(self):
        '''
        :return: download csv for chosen month (offset : 1 month for generation, 16 days for demand)
        '''
        xpath_csv = '//input[@type="image" and contains(@name, "CSV")]'
        not_found = True
        iteration = 0
        while not_found and iteration <10:
            try:
                csv_img = WebDriverWait(self.driver, 25).until(EC.element_to_be_clickable((By.XPATH, xpath_csv)))
                self.driver.execute_script("window.scrollTo(0, 0)")
                csv_img.click()
                not_found = False
            except:
                print (iteration)
                iteration += 1
                sleep(5)
        if not_found:
            raise KeyError('Could not find csv image')
        sleep(10)

    def get_data_from_url(self, tdate):
        '''
        This method opens the Selenium driver and choses the report, the region and the date. Then the csv is downloaded
        and stored in the filestore
                '''
        self.driver.get(self.url)
        self.driver.maximize_window()
        sleep(5)
        self.chose_report()
        logger.info(f'{tdate.strftime("%d/%m/%Y")} selected')
        for region in self.input_dfs:
            self.choose_region(region)
            self.choose_day(tdate, region)
            self.click_csv_img()
            logger.info(f'{tdate.strftime("%d/%m/%Y")} {region} downloaded')

    def get_data_from_csv(self, filename, region):
        '''
        extracts data from csv in filestore directory and store it in input_df
        :param tdate: datetime
        :param filename: string
        :return: input_dfs[metric]
        '''
        header = 1
        df = pd.read_csv(filename, sep=',', skiprows=6, header=header)
        if df.empty:
            df = pd.read_csv(filename, sep=',', skiprows=7, header=header)
        if df.empty:
            df = pd.read_csv(filename, sep=',', skiprows=8, header=header)
            
        self.input_dfs[region] = df

    def download_csv_from_filestore(self, tdate, region):
        '''
        Waits for file to appear in filestore directory before calling method to extract data from csv

        tdate: datetime
        '''
        filtered_files = glob.glob(self.file_store_path + f'/{self.filename_string} {region}*.csv')
        date_string = datetime.strftime(tdate, "%Y-%m-%d")

        iteration = 0
        file_found = False
        while not file_found and iteration < 40:
            sleep(1)
            for filename in filtered_files:
                if filename.find(date_string) != -1:
                    self.get_data_from_csv(filename, region)
                    file_found = True
            iteration += 1
        if iteration == 40:
            raise EdcJobError(f"Could not find Prices for {region} for {tdate.date()} in filestore")

    def format_df(self, tdate, df):
        '''
        Formats input_df that was downloaded from the webpage to DW format
        Converts in UTC datetime : Mexico_city is UTC-6
        :param tdate: datetime
        :param df: DataFrame
        :return: df_prices
        '''
        df = df.copy()
        df.columns = df.columns.str.strip()
        df = df.rename(columns={'Precio Zonal  ($/MWh)': 'Precio Zonal ($/MWh)'})
        df['Value'] = df['Precio Zonal ($/MWh)']
        df = df[df['Zona de Carga'].isin(self.regions_mapping)]
        df['Region'] = df['Zona de Carga'].apply(lambda x: self.regions_mapping[x])
        df['local_date'] = tdate
        df['local_datetime'] = df['local_date'] + pd.to_timedelta((df['Hora'] - 1), unit='h')
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize('America/Mexico_City').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].apply(lambda x: x.date())
        df['Export Date'] = self.export_datetime
        df['Country'] = 'Mexico'
        df['Metric'] = 'Prices'
        df['Product'] = 'ELE'
        df['Flow 1'] = 'Spot'
        df['Flow 2'] = 'MXN'
        df['Source'] = 'cenace.gob.mx'
        cols_keep = ['Value', 'Region', 'local_date', 'local_datetime', 'utc_datetime', 'utc_date', 'Export Date',
                     'Country', 'Metric', 'Product', 'Flow 1', 'Flow 2', 'Source']
        df = df[cols_keep]
        self.df_prices = pd.concat([df, self.df_prices])

    def run_date(self, tdate):
        # self.scrape_prices(tdate)
        self.get_data_from_url(tdate)
        for region in self.input_dfs.keys():
            self.download_csv_from_filestore(tdate, region)
            self.format_df(tdate, self.input_dfs[region])
        logger.info(f'Prices scraped for {tdate.date()}')


if __name__ == '__main__':
    mexican_prices_stats = MexicanPricesStatsJob()
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    tdate = datetime(2020, 11, 23)
    mexican_prices_stats.run_date(tdate)
