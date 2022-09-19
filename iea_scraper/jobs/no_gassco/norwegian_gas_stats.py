"""
Norwegian gas trade flows (LNG) from GASSCO (https://umm.gassco.no/)
- Aggregated daily nominations per gasday in MSm3, per location.
- No historical data available.

Louis Chambeau, June 2022
"""

import pandas as pd
from datetime import datetime

import sys
sys.path.append(r"C:\Repos\iea_scraper")

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from iea_scraper.core.job import EdcGasJob
from iea_scraper.jobs.utils import get_driver, BROWSERDRIVER_PATH
sys.path.append(BROWSERDRIVER_PATH)

from bs4 import BeautifulSoup
from time import sleep

class NorwegianGasStatsJob(EdcGasJob):
    title: str = 'GASSCO.NO - Norwegian Gas Statistics'

    def __init__(self):
        super().__init__()
        self.driver = get_driver(headless=True)
        self.url = "https://umm.gassco.no/"
        self.df = pd.DataFrame()

    def clear_disclaimer(self):
        """
        Simply accepts pop-up disclaimer when accessing the website.
        """

        self.driver.get(self.url)
        xpath_accept = '//*[@id="wrapper-primary"]/div/div/div/form/input[1]'
        WebDriverWait(self.driver, 2).until(EC.visibility_of_element_located((By.XPATH, xpath_accept))).click()
        sleep(3)

    def get_data(self):
        """
        Collects the available information in the table displayed
        """

        xpath_table = '//*[@id="wrapper-primary"]/table[1]/tbody/tr[2]/td/table/tbody/tr'
        html_data_table = self.driver.find_element_by_xpath(xpath_table)

        xpath_date = '//*[@id="wrapper-primary"]/table[1]/tbody/tr[1]/td/div/div[2]/span'
        gas_date = self.driver.find_element_by_xpath(xpath_date).text
        gas_date = datetime.strptime(gas_date, '%Y-%m-%d')

        data = []
        tds = BeautifulSoup(html_data_table.get_attribute('innerHTML'), 'html.parser').find_all('td')
        for td in tds:
            data_dict = {}
            divs = td.find_all('div')
            data_dict['Flow 2'] = divs[0].text
            data_dict['Value'] = float(divs[2].text)
            data += [data_dict]
        df =  pd.DataFrame(data)
        df['local_date'] = gas_date
        self.df = df

    @property
    def df_dw(self):
        df_dw = self.df
        df_dw['Country'] = "Norway"
        df_dw['Metric'] = "Trade"
        df_dw['Product'] = "Liquefied natural gas"
        df_dw['Unit'] = "MSm3"
        df_dw['Source'] = "GASSCO"
        df_dw['Flow 1'] = "Exit"
        df_dw['Flow 3'] = "LNG Partner"
        df_dw['Type'] = "Observed"
        df_dw = df_dw[df_dw["Flow 2"] != "Sum Exit Nominations NCS"]
        return df_dw

    def pre_run(self):

        self.clear_disclaimer()
        self.get_data()
        logger.info(f'LNG trade scraped from the website!')
        logger.info('Pre run completed!')

if __name__ == '__main__':
    worker = EuropeanGasStatsJob()
    folder = "C:\Repos\iea_scraper\iea_scraper\csvs"
    worker.test_run(folder)

