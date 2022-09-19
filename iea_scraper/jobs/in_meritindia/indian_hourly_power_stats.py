"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import requests
import pandas as pd
from datetime import datetime 
import logging
from bs4 import BeautifulSoup
import sys
sys.path.append(r'C:\Repos\iea_scraper')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
from iea_scraper.core.job import EdcJob 
from pytz import timezone

class IndianHourlyPowerStatsJob(EdcJob):
    
    title: str = 'meritindia.in - Indian Power Statistics'
    
    def __init__(self):
        EdcJob.__init__(self)
        self.url = 'https://meritindia.in/'
        self.mapping = {'DEMANDMET': 'ELE',
                        'THERMAL GENERATION': 'Coal',
                        'GAS GENERATION': 'Natural Gas',
                        'NUCLEAR GENERATION': 'Nuclear',
                        'HYDRO GENERATION': 'Hydro',
                        'RENEWABLE GENERATION': 'Renewables'}
        self.df_all = None
        
    def get_production_and_demand(self):
        r = requests.get(self.url)
        soup = BeautifulSoup(r.content, features='lxml')
        data_table = soup.find_all("div", {'class': 'marginal_cost_details'})[0]
        titles = data_table.find_all('div', {'class': 'gen_title_sec'} )
        values = data_table.find_all('div', {'class': 'gen_value_sec'} )
        data =[{'title': titles[i].text, 'value': values[i].text.strip()} for i in range(len(titles))]
        df_all = pd.DataFrame(data)
        df_all['Product'] = df_all['title'].apply(lambda x: self.mapping[x])
        df_all['Value'] = df_all['value'].apply(lambda x: "".join(filter(str.isdigit, x)))
        df_all['Metric'] = df_all['Product'].apply(lambda x: 'Demand' if x=='ELE' else 'Generation')
        df_all['local_datetime'] = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:00')
        df_all['Country'] = 'IND'
        df_all['Source'] = 'Merit India'
        self.df_all = df_all[['local_datetime', 'Country', 'Product', 'Metric', 
                              'Value', 'Source']]
        
    @property
    def df_dw(self):
        df_dw = self.df_all
        return df_dw
    
    def pre_run(self):
        self.get_production_and_demand()
        
if __name__ == '__main__':
    folder = r'C:\Repos\iea_scraper\iea_scraper\csvs'
    indian_scraper = IndianHourlyPowerStatsJob()
    indian_scraper.test_run(folder)
