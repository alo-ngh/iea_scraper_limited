# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
from lxml import html
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core.exceptions import EdcJobError

class BrazilianPowerStatsJob(EdcBulkJob):
    title: str = 'ONS.BR - Brazilian Power Statistics'
    def __init__(self):
        super().__init__()
        self.df_month_region = pd.DataFrame()
        self.df_all = pd.DataFrame()
        
        self.translation = {'Data': 'local_date', 'Total':'Total',
                       'HidrÃ¡ulica': 'Hydro', 'TÃ©rmica': 'Thermal', 
                       'EÃ³lica': 'Wind Onshore', 'Solar':'Solar',
                       'IntercÃ¢mbio': 'Exchange',
                       'Carga': 'Demand'}
        self.production_fields = ['Hydro', 'Thermal', 'Wind Onshore', 'Solar']
        self.unused_fields = ['Total', 'Exchange']
        self.region_mapping = {'SUL': 'Sul',
                               'SUDESTE': 'Sudeste/Centro Oeste',
                               'NORDESTE': 'Nordeste',
                               'NORTE': 'Norte'}
        self.fuel_mapping = {'val_geracaohidraulicamwmed': 'Hydro',
                             'val_geracaotermicamwed': 'Thermal',
                             'val_geracaoeolicamwmed': 'Wind Onshore',
                             'val_geracaofotovoltaicamwmed': 'Solar',
                             'val_demanda': 'Demand'
                             }
        self.url_all = 'https://ons-dl-prod-opendata.s3.amazonaws.com/dataset/balanco_energia_dessem_tm/BALANCO_ENERGIA_DESSEM_{year}.csv'
        self.yearly_data = {}
        
    @property
    def offset_now(self):
        return 2
        
    def scrape_yearly_data(self, year):
        """Gets yearly data, reformats it and adds it to self.yearly_data"""
        url = self.url_all.replace('{year}', str(year))
        try:
            df_year = pd.read_csv(url, delimiter=';')
        except:
            logger.warning(f'Could not load {year}')
            self.yearly_data[year] = None
            return
        df_year = df_year.rename(columns=self.fuel_mapping)
        df_year = df_year.rename(columns={'nom_subsistema': 'Region',
                                          'din_instante': 'local_datetime'})
        df_year['Region'] = df_year['Region'].apply(lambda x: self.region_mapping[x])
        df_demand = df_year[['local_datetime', 'Region', 'Demand']].copy()
        df_demand['Product'] = 'ELE'
        df_demand['Metric'] = 'Demand'
        df_demand = df_demand.rename(columns={'Demand': 'Value'})
        df_generation = df_year.melt(value_vars=['Hydro', 'Thermal', 'Wind Onshore', 'Solar'],
                                            id_vars=['local_datetime', 'Region'],
                                            var_name='Product', value_name='Value'
                                            )
        df_generation['Metric'] = 'Generation'
        df_load_and_demand = pd.concat([df_demand, df_generation])
        df_load_and_demand['Country'] = 'BRA'
        df_load_and_demand['Source'] = 'ONS'
        df_load_and_demand['local_datetime'] = pd.to_datetime(df_load_and_demand['local_datetime'])
        df_load_and_demand['local_date'] = df_load_and_demand['local_datetime'].dt.date
        self.yearly_data[year] = df_load_and_demand
        logger.info(f'Brazil data loaded for year {year}')
     
    @property
    def df_dw(self):
        df_dw = self.df_all
        return df_dw
    
    def run_date(self, tdate):
        if tdate.year not in self.yearly_data:
            self.scrape_yearly_data(tdate.year)
        if self.yearly_data[tdate.year] is not None:
            df_year = self.yearly_data[tdate.year].copy()
            self.df_all = pd.concat([self.df_all, df_year.loc[df_year['local_date']==tdate.date()]])
        
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    ons_scraper = BrazilianPowerStatsJob()
    ons_scraper.test_run(historical=False)
