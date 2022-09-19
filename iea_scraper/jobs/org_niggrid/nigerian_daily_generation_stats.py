# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: Daugy Mathilde
"""
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re

from iea_scraper.core import factory
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.settings import SSL_CERTIFICATE_PATH

timezone = 'Pacific/Auckland'

import logging
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.jobs.utils import get_driver


class NigerianDailyGenerationStatsJob(EdcBulkJob):
    title: str = 'NIGGRIG.org - Nigerian Daily Generation Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.driver = get_driver(headless=True)
        # self.url = 'https://www.niggrid.org/GenerationLoadProfileBinary?readingDate=%d/%m/%Y&readingTime=%H:00'
        self.url = 'https://www.niggrid.org/GenerationProfile'
        self.generation_data_points = []
        self.df_gen = pd.DataFrame()
        self.product_mapping = {'GAS': 'Natural Gas',
                                'STEAM': 'Natural Gas',
                                'HYDRO': 'Hydro'}
        self.missing_hours = 0
        
    @property
    def offset_now(self):
        return 0

    def get_data_for_datetime(self, tdate, hour):
        """
        uses selenium to choose the correct date and time and the click on the "GET GENERATION" profile to load data for
        the selected datetime
        @return:
        """
        self.driver.get(self.url)
        self.choose_date(tdate)
        try:
            self.choose_time(tdate, hour)
            WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="MainContent_btnGetReadings"]'))).click()
            self.get_data_from_table(tdate, hour)
        except:
            logger.warning(f"Nigerian generation: {hour}h data not available on {tdate.date()}")
            self.missing_hours += 1
            pass


    def choose_date(self, tdate):
        """
        pick date from calendar menu
        Compare the selected month and year to the displayed ones and modifies using the select menu then clicks on
        selected day
        @param tdate:
        @return:
        """
        date_picker_xpath = '//*[@id="MainContent_txtReadingDate"]'
        WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, date_picker_xpath))).click()

        datepicker_year = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="ui-datepicker-div"]/div/div/select[2]')))
        years_available = Select(datepicker_year)
        year_selected = years_available.first_selected_option.text
        if tdate.year != int(year_selected):
            years_available.select_by_visible_text(str(tdate.year))

        datepicker_month = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="ui-datepicker-div"]/div/div/select[1]')))
        months_available = Select(datepicker_month)
        month_selected = months_available.first_selected_option.text
        if tdate.month != datetime.strptime(month_selected, "%b").month:
            months_available.select_by_visible_text(tdate.strftime("%b"))

        day_elem = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f'//*[text()="{tdate.day}"]')))
        day_elem.click()

    def choose_time(self, tdate, hour):
        """
        picks selected time from Time dropdown menu
        """
        time_picker = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="MainContent_ddlTime"]')))
        hours_available = Select(time_picker)
        hours_available.select_by_visible_text(tdate.replace(hour=hour).strftime("%H:%M"))

    def get_data_from_table(self, date, hour):
        """
        when correct webpage is loaded, collects data for datetime and stores in generation_data_points list
        this step is repeated for each hour of the date selected
        @param date: datetime
        @return: list
        """
        tbody_table = WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="MainContent_gvGencoLoadProfiles"]/tbody')))
        data_html = tbody_table.get_attribute('innerHTML')
        soup = BeautifulSoup(data_html, features='lxml')
        table_rows = soup.find_all('tr')
        all_data_points = []
        for row in table_rows[1:]:
            cells = row.find_all("td")
            data_point = {'local_datetime': date.replace(hour=hour),
                          'local_date': date.date(),
                          'company': re.sub('\s+', '', cells[1].get_text()),
                          'Value': re.sub('\s+', '', cells[2].get_text())}
            all_data_points += [data_point]
        self.generation_data_points += all_data_points

    def get_fuel_mapping(self, company):
        """
        used to map power plants to product
        @param company: str
        """
        for fuel in self.product_mapping:
            if fuel in company:
                return self.product_mapping[fuel]

    def format_generation(self):
        df_gen = pd.DataFrame(self.generation_data_points)
        df_gen = df_gen.loc[df_gen['company'] != 'TotalGeneration']
        df_gen['Value'] = df_gen['Value'].apply(lambda x: float(x.replace(',', '')))
        df_gen['Product'] = df_gen['company'].apply(lambda x: self.get_fuel_mapping(x))
        df_gen = df_gen.groupby(['local_datetime', 'local_date', 'Product'])['Value'].sum().reset_index()
        df_gen['Country'] = 'Nigeria'
        df_gen['Source'] = 'Transmission Company of Nigeria'
        df_gen['Metric'] = 'Generation'
        df_gen['utc_datetime'] = pd.to_datetime(df_gen['local_datetime']).dt.tz_localize(
            'Africa/Lagos',
            nonexistent='shift_forward', ambiguous='NaT').dt.tz_convert('UTC')
        df_gen['utc_date'] = df_gen['utc_datetime'].dt.date
        df_gen['Export Date'] = self.export_datetime
        self.df_gen = df_gen

    @property
    def df_dw(self):
        return self.df_gen


    def run_date(self, tdate, hours_missing_limit=2):
        self.missing_hours = 0
        for hour in range(1, 24):
            self.get_data_for_datetime(tdate, hour)
        if self.missing_hours > hours_missing_limit:
            raise EdcJobError(f'Too many hours missing to load the Nigerian data: {self.missing_hours}. Max allowed is {hours_missing_limit}.')
        self.format_generation()


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper = NigerianDailyGenerationStatsJob()
    scraper.test_run(folder)
    # scraper= factory.get_scraper_job('org_niggrid','nigerian_daily_generation_stats')
