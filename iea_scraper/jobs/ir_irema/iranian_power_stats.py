"""
Created on Wed Dec  2 10:18:04 2020

@author: Nghiem_A
"""

import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from datetime import timedelta
import sys
import logging
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.jobs.utils import get_driver
from iea_scraper.jsettings import BROWSERDRIVER_PATH
sys.path.append(BROWSERDRIVER_PATH)


class IranianPowerStatsJob(EdcBulkJob):
    title: str = 'IREMA.IR - Iranian Power Statistics'
    
    def __init__(self):
        super().__init__(self)
        self.main_url = 'http://www.irema.ir/market-data/hourly-data/'
        self.urls = {'Demand': self.main_url + 'volume/req',
                     'Prices': self.main_url + 'price/average'}
        self.xpath_code = {'Demand': 756,
                           'Prices': 749}
        self.all_data_points = []
        self.driver = get_driver()
    
    @property
    def offset_now(self):
        return 1
    
    @property  
    def df_dw(self):
        df_dw = pd.DataFrame(self.all_data_points)
        return df_dw
    
    def scrape_bulk(self, date_start, date_end):
        for tdate in pd.date_range(date_start, date_end, freq='1D'):
            self.scrape_day(tdate, 'Demand')
            self.scrape_day(tdate, 'Prices')
            
    def scrape_day(self, tdate, metric):
        '''metric is either 'Prices' or 'Demand' '''
        self.driver.get(self.urls[metric])
        start_date_element = self.driver.find_element_by_xpath(f'//*[@id="cal_dnn_ctr{self.xpath_code[metric]}_Main_MainControls_rptParameters_PdpValue_0"]')
        start_date_element.click()
        start_date_element.clear()
        start_date_element.send_keys(tdate.strftime('%Y/%m/%d'))
        end_date_element = self.driver.find_element_by_xpath(f'//*[@id="cal_dnn_ctr{self.xpath_code[metric]}_Main_MainControls_rptParameters_PdpValue_1"]')
        end_date_element.click()
        end_date_element.clear()
        end_date_element.send_keys(tdate.strftime('%Y/%m/%d'))
        run_report_element = self.driver.find_element_by_xpath(f'//*[@id="dnn_ctr{self.xpath_code[metric]}_Main_MainControls_lnkRunReport"]')
        run_report_element.click()
        data_element = self.driver.find_element_by_css_selector(f'#dnn_ctr{self.xpath_code[metric]}_ModuleContent')
        data_html = data_element.get_attribute('innerHTML')
        soup = BeautifulSoup(data_html, features='lxml')
        rows = soup.find_all('tr')
        for row in rows[1:25]:
            data = row.find_all('td')
            data_point = {}
            data_point['local_date'] = datetime.strptime(data[1].text, '%Y/%m/%d')
            data_point['local_datetime'] = data_point['local_date'] + timedelta(hours=float(data[2].text) - 1)
            data_point['Value'] = float(data[3].text.replace(',',''))
            data_point['Metric'] = metric
            data_point['Product'] = 'ELE'
            data_point['Country'] = 'Iran'
            data_point['Source'] = 'IREMA'
            if metric == 'Prices':
                data_point['Flow 1'] = 'Spot'
                data_point['Flow 2'] = 'IRR'
            self.all_data_points += [data_point] 
        logger.info(f'{tdate.date()} {metric} was scraped')
        
    def run_date(self, tdate):
        self.data_points = {}
        self.scrape_day(tdate, 'Demand')
        self.scrape_day(tdate, 'Prices')
  
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    iranian_scraper = IranianPowerStatsJob()
    iranian_scraper.test_run(folder)
    
        


   

