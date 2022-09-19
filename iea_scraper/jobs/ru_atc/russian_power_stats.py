"""
Russian scraper
Hourly demand and prices (buyer, seller, minimum and maximum)
Per region
Source: http://www.atsenergo.ru/results/rsv/oes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import sys

sys.path.append(r'C:\Repos\scraper')

from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import BROWSERDRIVER_PATH
sys.path.append(BROWSERDRIVER_PATH)
from iea_scraper.core.job import EdcBulkJob
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from math import ceil

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class RussianPowerStatsJob(EdcBulkJob):
    
    title: str = 'RU - Russian Power Statistics'
    
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.target_url = 'http://www.atsenergo.ru/results/rsv/oes'
        self.driver = get_driver()
        self.text_data = []
        self.df = pd.DataFrame()
        self.months_RU = {
            'Январь': 1,
            'Февраль': 2,
            'Март': 3,
            'Апрель': 4,
            'Май': 5,
            'Июнь': 6,
            'Июль': 7,
            'Август': 8,
            'Сентябрь': 9,
            'Октябрь': 10,
            'Ноябрь': 11,
            'Декабрь': 12
                }
        self.headers_RU_to_ENG = {
            'ОЭС': 'Region',
            'Дата': 'Local_date',
            'Час': 'Hour',
            'Объем полного планового потребления, МВт*ч': 'Demand',
            'Объем планового производства, МВт*ч': 'Generation Total',
            'Индекс равновесных цен на покупку электроэнергии, руб./МВт*ч': 'Price (buyer)',
            'Индекс равновесных цен на продажу электроэнергии, руб.МВт*ч': 'Price (seller)',
            'Максимальный индекс равновесной цены, руб.МВт*ч': 'Minimum price',
            'Минимальный индекс равновесной цены, руб.МВт*ч': 'Maximum price'
            }
        self.regions_RU_to_ENG = {
            'ОЭС Урала': 'UES Ural',
            'ОЭС Средней Волги': 'UES of the Middle Volga',
            'ОЭС Юга': 'UES South',
            'ОЭС Северо-Запада': 'UES of the North-West',
            'ОЭС Центра': 'UES Center',
            'ОЭС Сибири': 'UES Total Siberia'
            }
        self.metric_mapping = {
            'Demand': 'Demand',
            'Generation Total': 'Generation Total',
            'Prices': 'Prices'
            }
        self.product_mapping = {
            'Demand': 'ELE',
            'Generation Total': 'ELE',
            'Prices': 'ELE',
            'Price (buyer)': 'ELE',
            'Price (seller)': 'ELE',
            'Minimum price': 'ELE',
            'Maximum price': 'ELE'
            }
        self.variables_renaming = {
            'Demand': 'Demand',
            'Generation Total': 'Generation Total',
            'Price (buyer)': 'Prices',
            'Price (seller)': 'Prices',
            'Minimum price': 'Prices',
            'Maximum price': 'Prices'
            }
        self.flow1_mapping = {
            'Demand': 'Forecast',
            'Generation Total': 'Forecast',
            'Prices': 'Spot'
            }
        self.flow2_mapping = {
            'Demand': np.nan,
            'Generation Total': np.nan,
            'Prices': 'RUB'
            }
        self.flow3_mapping = {
            'Price (buyer)': 'Buyer',
            'Price (seller)': 'Seller',
            'Minimum price': 'Minimum',
            'Maximum price': 'Maximum'
            }            

    @property
    def offset_now(self):
        """
        Description:
            Latest available data is the day before today
        """

        return 1

    def run_date(self, tdate):
        """
        Called by EdcBulkJob.pre_run()

        Input(s):
            tdate [datetime]

        Output(s):
            text_data [list]: html information contained on the websiet's table            
            df [DataFrame]: contains the data from the website
            df_dw [DataFrame]: DataFrame formatted for the DW

        Description:
            Executes the job for one day without all the primary checks and without
                loading into db on the current date
            Sets up the interface on the website on the correct dates
            Collects the data and puts it in a DataFrame
            Formats the DataFrame to fit the DW

        Calls:
            tdate_select()
            get_data_to_df()
            df_dw()
        """

        self.tdate_select(tdate, tdate)
        self.get_data_to_df()
        self.df_dw
        pass

    def compute_offset(self, target_year, target_month, which_day):
        """
        Called by click_month()

        Input(s):
            target_year [int]: year we want to select on the view
            target_month [int]: month we want to select on the view
            which_day [int]: table being manipulated (1 for both tdate_start and tdate_end)

        Output(s):
            offset [int]: the number of months away from the target (also number of clicks remaining)
        
        Description:
            Based on the target year and month, determines how far the current view is
        """

        #determines current month/year to derive the offset to the target
        month_current_selection_RU = self.driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/div/div/span[1]')
        month_current_selection = self.months_RU[month_current_selection_RU.text]
        year_current_selection = int(self.driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/div/div/span[2]').text)

        offset = 12 * (target_year - year_current_selection) + (target_month - month_current_selection)

        return offset

    def click_month(self, tdate, which_day):
        """
        Called by click_day()

        Input(s): 
            tdate [datetime]
        Output(s): 
            none, makes the appropriate interface visible to Selenium

        Description:
            Recursion to look for the target month
            Based on offset's sign, left or right click the month
            Ends when zero offset is found, meaning we are on the right view

        Calls:
            compute_offset()
        """
        target_year = tdate.year
        target_month = tdate.month

        #to move backwards
        left_click_Xpath = '//*[@id="ui-datepicker-div"]/div/a[1]/span'
        #to move foreward
        right_click_Xpath = '//*[@id="ui-datepicker-div"]/div/a[2]/span'

        offset = self.compute_offset(target_year, target_month, which_day)
        if offset == 0:
            pass
        elif offset < 0:
            WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, left_click_Xpath))).click()
            self.click_month(tdate, which_day)
        else:
            WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, right_click_Xpath))).click()
            self.click_month(tdate, which_day)


    def click_day(self, tdate, which_day):
        """
        Called by:
            tdate_select()

        Input(s):
            tdate [datetime]: day to be clicked
            which_day [int]: determines if start date or end date. 1 is start, 2 is end

        Output(s):
            None

        Description:
            Calls a series of sub functions to set the view for the given day

        Calls:
            click_month()
        """
        self.driver.get(self.target_url)
        #make the date table visible
        WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="oes_stats_1_date{which_day}"]'))).click()

        #select the correct month
        self.click_month(tdate, which_day)

        #we play a bit with the months/days to determine the cell position
        first_day = tdate.replace(day=1)
        day_of_month = tdate.day
        adjusted_day_of_month = day_of_month + first_day.weekday()
        week_of_month = int(ceil(adjusted_day_of_month/7.0)) #used to determine the row in the table
        day_of_week = tdate.weekday() + 1

        #click on the right day
        x_path_day = f'//*[@id="ui-datepicker-div"]/table/tbody/tr[{week_of_month}]/td[{day_of_week}]/a'
        
        #delay to make sure it is visible
        WebDriverWait(self.driver,5).until(EC.element_to_be_clickable((By.XPATH,x_path_day)))
        time.sleep(1)
        self.driver.find_element_by_xpath(x_path_day).click()

        #click on the refresh button
        refresh_Xpath = '//*[@id="oes_header"]/div[4]/input'
        WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.XPATH, refresh_Xpath))).click()
        time.sleep(5)
    
    def tdate_select(self, tdate_start, tdate_end):
        """ 
        Called by:
            run_date()

        Input(s):
            tdate_start [datetime]: start date of the period to extract
            tdate_end [datetime]: end date of the period to extract
        
        Output(s):
            text_data [list]: html information contained on the websiet's table

        Description:
            Used to select the date range to extract
            The oldest available data at this source is Jan 2018.
            But the website has other displays with older data. 

        Calls:
            click_day()
        """

        self.driver.get(self.target_url)

        #set the right view for the start date
        self.click_day(tdate_start, 1)
        #set the right view for the end date
        self.click_day(tdate_end, 2)

        #get the content of the refreshed table
        html_table_Xpath = '//*[@id="oes_stats_table"]'
        html_data_table = self.driver.find_element_by_xpath(html_table_Xpath)
        self.text_data = BeautifulSoup(html_data_table.get_attribute('innerHTML'), 'html.parser')

    def get_data_to_df(self):
        """
        Called by run_date()

        Input(s):
            None

        Output(s):
            df [DataFrame]: contains the data from the website

        Description:
            Finds the relevant elements in the list text_data
            Reconstitutes the data table
            Manipulates the DataFrame to fit the DW
        """

        headers = []
        text_data = self.text_data
        for el in text_data.find_all('th'):
            headers.append(el.text)

        rows_table = text_data.tbody.find_all('tr')
        data_table = {}
        hour = 0
        for row in rows_table:
            hourly_data = []
            row_data = row.find_all('td')
            hourly_data = [col.text.replace(u'\xa0', '') for col in row_data]
            hour += 1
            data_table[hour] = hourly_data


        df = pd.DataFrame.from_dict(data_table, orient='index', columns=self.headers_RU_to_ENG.values())
        df['Region'] = df['Region'].map(self.regions_RU_to_ENG)
        df['local_datetime'] = pd.to_datetime(df['Local_date'],  format='%d.%m.%Y') + timedelta(hours=1) * df['Hour'].astype('int64')
        df = df.drop(columns=['Local_date', 'Hour'])

        melt_id_vars = ['local_datetime', 'Region']
        melt_value_vars = ['Demand', 'Generation Total','Price (buyer)', 'Price (seller)', 'Minimum price', 'Maximum price']
        df = pd.melt(df,melt_id_vars,melt_value_vars, value_name='Value')

        df['Country'] = 'RUS'
        df['Flow 3'] = df['variable'].map(self.flow3_mapping)
        df['Variable'] = df['variable'].map(self.variables_renaming)
        df['Product'] = df['Variable'].map(self.product_mapping)
        df['Metric'] = df['Variable'].map(self.metric_mapping)    
        df['Flow 1'] = df['Variable'].map(self.flow1_mapping)
        df['Flow 2'] = df['Variable'].map(self.flow2_mapping)
        df['Source'] = 'ATS'

        self.df = self.df.append(df) 

    @property
    def df_dw(self):
        """
        Called by run_date()

        Input(s):
            None

        Output(s):
            df_dw [DataFrame]: DataFrame to be loaded in the DW

        Description:
            Selects relevant columns from df
        """

        df_dw = self.df[['local_datetime', 'Country', 'Region', 'Metric', 'Product', 'Flow 1', 'Flow 2', 'Flow 3', 'Source', 'Value']]
        
        return df_dw 

if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    russian_scraper = RussianPowerStatsJob()
    russian_scraper.test_run(folder)
  
