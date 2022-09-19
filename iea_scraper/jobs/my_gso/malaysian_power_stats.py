"""
Created on Wed Dec  2 10:18:04 2020

@author: Nghiem_A
"""

import pandas as pd
from datetime import datetime
import time
import sys
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import BROWSERDRIVER_PATH
sys.path.append(BROWSERDRIVER_PATH)


class MalaysianPowerStatsJob(EdcBulkJob):
    
    title: str = 'GSO.ORG.MY - Malaysian Power Statistics'
    
    def __init__(self):
        super().__init__(self)
        self.url_generation = 'https://www.gso.org.my/SystemData/FuelMix.aspx'
        self.url_demand = 'https://www.gso.org.my/SystemData/SystemDemand.aspx'
        self.driver = get_driver(headless=True)
        self.fuel_mapping = {'ST-Gas': 'Natural Gas',
                             'ST-Coal': 'Coal',
                             'OCGT-Gas': 'Natural Gas',
                             'Hydro': 'Hydro',
                             'LSS': 'Other',
                             'Gas': 'Natural Gas',
                             'Co-Gen': 'Cogeneration',
                             'CCGT-Gas': 'Natural Gas',
                             'Distillate': 'Other'}
        self.df_generation = pd.DataFrame()
        self.df_load = pd.DataFrame()

    @property  
    def offset_now(self):
        return 2
    
    @property  
    def df_dw(self):
        df_dw = pd.concat([self.df_generation, self.df_load])
        return df_dw
    
    def scrape_bulk(self, date_start, date_end):
        for tdate in pd.date_range(date_start, date_end, freq='-1D'):
            self.scrape_demand(tdate)
            self.scrape_generation(tdate)
            
    def select_dates(self, date_start, date_end, metric):
        '''
        Selects the date with the open calendar once the month is selected
        date_start : datetime
        date_end : datetime
        metric : str 'Generation' or 'Demand'
        '''
        start_date_element = self.driver.find_elements_by_css_selector("i[class='date-calendar']")[0]
        self.click_date(start_date_element, date_start, 'start', metric)
        end_date_element = self.driver.find_elements_by_css_selector("i[class='date-calendar']")[1]
        self.click_date(end_date_element, date_end, 'end', metric)
        time.sleep(5)
        plot_button = self.driver.find_element_by_xpath('//*[@value="Plot"]')
        plot_button.click()
        time.sleep(5)
    
    def find_month(self, metric, start_end):
        '''
        Finds the month element in the open calendar
        start_end : 'start' or 'end'
        metric : str 'Generation' or 'Demand'
        '''
        if metric == 'Demand':
            month_text = self.driver.find_element_by_xpath('/html/body/div[2]/div[1]/table/thead/tr[1]/th[2]').text
        elif metric == 'Generation':
            month_text = self.driver.find_element_by_xpath('/html/body/div[2]/div[1]/table/thead/tr[1]/th[2]').text
        else:
            raise KeyError("Metric has to be 'Generation' or 'Demand'")
        month = datetime.strptime(month_text, '%B %Y').date()
        return month
            
    def click_date(self, date_elem, tdate, start_end, metric):
        ''' 
        Finds the right month and clicks on date
        date_elem: Selenium WebElement
        tdate: datetime
        start_end: str 'start' or 'end'
        metric : str 'Generation' or 'Demand'
        '''
        tdate_month = tdate.replace(day=1).date()
        date_elem.click()
        time.sleep(1)
        date_found = False
        while not date_found:
            month = self.find_month(metric, start_end)
            if tdate_month == month:
                WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//*[(@class="day") and text()="{tdate.day}"]'))).click()
                date_found = True
            elif tdate_month < month:
                WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@class="prev"]'))).click()
            else:
                WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@class="next"]'))).click()
            
    def scrape_generation(self, tdate):
        '''
        tdate: datetime
        '''
        self.driver.get(self.url_generation)
        self.select_dates(tdate, tdate, 'Generation')
        df_gen = pd.read_html(self.driver.page_source)[-1]
        df_gen.columns = ['Product', 'Value']
        df_gen = df_gen[1:]
        df_gen['Value'] = df_gen['Value'].astype(float)
        df_gen['Product'] = df_gen['Product'].apply(lambda x: self.fuel_mapping[x])
        df_gen = df_gen.groupby('Product').sum().reset_index()
        df_gen['local_date'] = tdate.date()
        df_gen['Metric'] = 'Generation'       
        self.df_generation = pd.concat([self.df_generation, self.format_df(df_gen)])
        logger.info(f'{tdate.date()} Generation was scraped')
        
    def scrape_demand(self, tdate):
        '''
        tdate: datetime
        '''
        self.driver.get(self.url_demand)
        self.select_dates(tdate, tdate, 'Demand')
        df_load = pd.read_html(self.driver.page_source)[-1]
        df_load.columns = ['local_datetime', 'Value']
        df_load = df_load[1:].copy()
        df_load['local_datetime'] = pd.to_datetime(df_load['local_datetime'])
        df_load['local_date'] = df_load['local_datetime'].dt.date
        df_load['Metric'] = 'Demand'       
        df_load['Product'] = 'ELE'       
        self.df_load = pd.concat([self.df_load, self.format_df(df_load)])
        logger.info(f'{tdate.date()} Demand was scraped')
        
    def format_df(self, df):
        df['Country'] = 'Malaysia'     
        df['Export Date'] = self.export_date  
        df['Source'] = 'GSO'
        return df
        
    def run_date(self, tdate):
        self.scrape_generation(tdate)
        self.scrape_demand(tdate)
        
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    malaysian_scraper = MalaysianPowerStatsJob()
    tdate = datetime(2019, 9, 30)
    # malaysian_scraper.run_date(tdate)
    malaysian_scraper.test_run(folder, historical=False)
    
