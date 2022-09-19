# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
from pathlib import Path
import requests
import io


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import  EdcBulkJob
from iea_scraper.settings import PROXY_DICT
        
class ColombianPowerStatsJob(EdcBulkJob):
    
    title: str = 'XM.COM.CO - Colombian Power Statistics'
    
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.df_plant_mapping = self.get_plant_mapping()
        self.url_generation = 'https://portalanterior.xm.com.co/despachonacional/%Y-%m/dDEC%m%d_NAL.TXT'
        self.url_demand = 'https://portalanterior.xm.com.co/pronosticooficial/%Y-%m/PRONSIN%m%d.txt'
        self.url_demand_bis = 'https://portalanterior.xm.com.co/pronosticooficial/%Y-%m/PRON_SIN%m%d.txt'
        self.df_demand = pd.DataFrame()
        self.df_generation = pd.DataFrame()
        self.df_demand_all = pd.DataFrame()
        self.df_generation_all = pd.DataFrame()
        
    @property
    def offset_now(self):
        return 2
    
    def scrape_generation(self, tdate):
        self.get_generation_per_plant(tdate)
        self.map_generation_with_plant_mapping()
        self.format_generation()
        logger.info(f'{tdate} Colombian generation scraped')
        
    def get_generation_per_plant(self, tdate):
        url = tdate.strftime(self.url_generation)
        s = requests.get(url, proxies=PROXY_DICT, verify=False).text
        df_generation_per_plant = pd.read_csv(io.StringIO(s), encoding='latin1')
        df_generation_per_plant.columns = ['plant_name'] + list(range(24))
        df_generation_per_plant['local_date'] = tdate
        df_generation_per_plant = df_generation_per_plant.loc[df_generation_per_plant['plant_name']!='Total']
        self.df_generation_per_plant = df_generation_per_plant
    
    def map_generation_with_plant_mapping(self):
        df_generation_per_plant = self.df_generation_per_plant
        df_generation_per_plant = df_generation_per_plant.merge(self.df_plant_mapping,
                                                               on='plant_name', how='left'
                                                               )
        df_generation_per_plant.loc[df_generation_per_plant['type'].isnull(), 'type'] = 'Other' 
        df_generation_per_plant = df_generation_per_plant.melt(id_vars=['local_date', 'plant_name', 'type'], 
                                                               value_name='Value',
                                                               var_name='hour')
        df_generation = df_generation_per_plant.groupby(['local_date', 'hour', 'type']).sum().reset_index()
        self.df_generation_per_plant = df_generation_per_plant
        self.df_generation = df_generation
        
    def format_generation(self):
        df_generation = self.df_generation
        df_generation['local_datetime'] = df_generation.apply(lambda x: x['local_date'] + timedelta(hours=x['hour']), axis=1)
        df_generation['Metric'] = 'Generation'
        df_generation['Country'] = 'Colombia'
        df_generation['Source'] = 'xm.com.co'
        df_generation = df_generation.rename(columns={'type': 'Product'})
        df_generation = df_generation.drop(columns=['hour'])
        self.df_generation = df_generation
        self.df_generation_all = pd.concat([self.df_generation_all, df_generation])
        
    def scrape_demand(self, tdate):
        self.get_demand(tdate)
        self.format_demand()
        logger.info(f'{tdate} Colombian demand scraped')
     
    def get_demand(self, tdate):
        start_week = tdate - timedelta(days=tdate.weekday())
        try:
            url = start_week.strftime(self.url_demand)
            s = requests.get(url, proxies=PROXY_DICT, verify=False).text
            df_week_demand = pd.read_csv(io.StringIO(s))
            df_week_demand.columns = ['hour'] + [start_week + timedelta(days=i) for i in range(7)]
        except:
            # Backup solution when first url doesn't work
            url = start_week.strftime(self.url_demand_bis)
            s = requests.get(url, proxies=PROXY_DICT, verify=False).text
            df_week_demand = pd.read_csv(io.StringIO(s))
            try:
                df_week_demand.columns = ['na', 'hour'] + [start_week + timedelta(days=i) for i in range(7)]
            except:
                #csv format can vary
                df_week_demand.columns = ['hour'] + [start_week + timedelta(days=i) for i in range(7)]
                
        df_week_demand = df_week_demand.melt(id_vars=['hour'], value_name='Value',
                                              var_name='local_date')
        self.df_demand = df_week_demand.loc[df_week_demand['local_date']==tdate]
        
    def format_demand(self):
        df_demand = self.df_demand
        df_demand['local_datetime'] = df_demand.apply(lambda x: x['local_date'] + timedelta(hours=x['hour']), axis=1)
        df_demand['Metric'] = 'Demand'
        df_demand['Country'] = 'Colombia'
        df_demand['Source'] = 'xm.com.co'
        df_demand['Product'] = 'ELE'
        df_demand = df_demand.drop(columns=['hour'])
        self.df_demand = df_demand
        self.df_demand_all = pd.concat([self.df_demand_all, df_demand])
        
    def get_plant_mapping(self):
        dir_path = Path(__file__).parents[0]
        file_path = dir_path / 'plant_mapping_colombia.csv'
        df = pd.read_csv(file_path, encoding='latin1')
        df = df[['plant_name', 'type']]
        df = df.loc[~df['plant_name'].isnull()]
        return df
       
    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_generation_all, self.df_demand_all])
        return df_dw
        
    def run_date(self, tdate):
        self.scrape_generation(tdate)
        self.scrape_demand(tdate)
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    xm_scraper = ColombianPowerStatsJob()
    xm_scraper.test_run()
    # xm_scraper.test_run(folder)