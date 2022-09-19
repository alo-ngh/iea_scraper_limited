import re
import requests
import pandas as pd
import logging
import sys
import io
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zipfile import ZipFile
import os
import numpy as np
from nemosis import data_fetch_methods

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.settings import SSL_CERTIFICATE_PATH

REQUESTS_HEADER = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                                  + ' AppleWebKit/537.36 (KHTML, like Gecko) '
                                  + 'Chrome/80.0.3987.87 Safari/537.36')}


class AustralianGenerationStatsJob(EdcBulkJob):
    title: str = 'Australia.NEM - Australian Generation Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.main_url = 'http://nemweb.com.au'
        self.df_generators = pd.DataFrame()
        self.updated_generators = False
        self.power_plants_url = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"
        self.urls = {'current': 'http://nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/',
                     'archive': 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM'}
        self.df_generation_per_plant = {'current': pd.DataFrame(),
                                        'archive': pd.DataFrame()}
        self.df_generation = {'current': pd.DataFrame(),
                              'archive': pd.DataFrame()}
        self.fuel_mapping = {'Landfill Methane / Landfill Gas': 'Biomass',
                             'Diesel': 'Oil',
                             'Waste Coal Mine Gas': 'Coal',
                             'Wind': 'Wind Onshore',
                             'Natural Gas': 'Natural Gas',
                             'Solar': 'Solar',
                             'Grid': np.nan,
                             'Battery load': np.nan,
                             'Water': 'Hydro',
                             'Basslink supply': np.nan,
                             'Black Coal': 'Hard Coal',
                             'Coal Seam Methane': 'Hard Coal',
                             'Bagasse': 'Biomass',
                             'Natural Gas / Diesel': 'Thermal',
                             'Kerosene': 'Oil',
                             'Hydro pump supply': 'Hydro Pumped Storage',
                             'Brown Coal': 'Brown Coal',
                             'Load': np.nan,
                             'Sewerage / Waste Water': 'Waste',
                             'Natural Gas / Fuel Oil': 'Thermal',
                             'Hydro pump load': 'Hydro Pumped Storage',
                             'Load - guess': np.nan,
                             'Black coal': 'Hard Coal',
                             'Dummy Generator': np.nan,
                             'Coal Tailings': 'Hard Coal',
                             'Reserve Trader': np.nan,
                             'Hydro pump': 'Hydro Pumped Storage',
                             'Hydro': 'Hydro'}
        self.zipped_links = {}
        self.region_mapping = {
            'NSW1': 'New South Wales',
            'QLD1': 'Queensland',
            'SA1': 'South Australia',
            'TAS1': 'Tasmania',
            'VIC1': 'Victoria'
        }
        self.utc_tz_mapping = {
            'New South Wales': 'Australia/NSW',
            'Queensland': 'Australia/Queensland',
            'South Australia': 'Australia/South',
            'Tasmania': 'Australia/Tasmania',
            'Victoria': 'Australia/Victoria'
        }
        self.last_updated_month = ''
        self.archive_downloaded = False

    def get_plant_mapping(self):
        '''
        This method updates the 'NEM Registration and Exemption List' xls file 3 times a month
        to include new registered power plants. The method calls data_fetch_methods from the NEMOSIS project
        which gives direct access to the power plant list.
        If the list is not updated, the methods gets the information from the xls file in the directory
        :return: DataFrame
        '''
        dir_path = os.path.dirname(os.path.realpath(__file__))
        if not self.updated_generators:
            try:
                df = data_fetch_methods.static_table_xl('Generators and Scheduled Loads', dir_path)
            except:
                df = pd.read_excel(os.path.join(dir_path, 'NEM Registration and Exemption List.xls'),
                                   sheet_name='Generators and Scheduled Loads')
            df = df[['Station Name', 'Region', 'Fuel Source - Descriptor', 'DUID']]
            df = df.loc[~df['Fuel Source - Descriptor'].isnull()]
            df['Fuel Source - Descriptor'] = df['Fuel Source - Descriptor'].apply(lambda x: x.strip())
            self.df_generators = df
            self.updated_generators = True

    def get_links(self, tdate, doc_type):
        '''
        This methods call get_links_current or get_links_archive depending on tdate.
        If tdate is last_available date then the methods called is get_links_current else calls get_links_archive
        :param tdate: datetime
        :param doc_type: determined in run_date, depends on tdate
        :return:
        '''
        if doc_type == 'current':
            self.get_links_current(tdate, doc_type)
        elif doc_type == 'archive':
            self.get_links_archive(tdate, doc_type)
        else:
            raise EdcJobError("metric has to be 'current' or 'archive'")

    def get_links_current(self, tdate, doc_type='current'):
        '''
        This method accesses the 'current' directory of NEMWEB and returns a dictionary containing  timestamps (str) as
        keys and links of zipped csvs as values
        :param tdate: datetime
        :param doc_type: str (here 'current')
        :return: dictionary {date: link}
        '''

        url = self.urls[doc_type]
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADER)
        soup = BeautifulSoup(r.content, 'html.parser')
        all_links = [elem['href'] for elem in soup.find_all('a', href=True)]
        filtered_links = [x for x in all_links if tdate.strftime("%Y%m%d") in x]
        date_char = -33
        zip_links = {link[date_char:date_char + 12]: self.main_url + link
                     for link in filtered_links}
        self.zipped_links = zip_links

    def get_links_archive(self, tdate, doc_type='archive'):
        '''
        This method accesses the data_archives in the NEMWEB directory and return a dictionary containing the date as key
        and the link of the zipped csv as values
        :param tdate: datetime
        :param doc_type: str (here 'archive')
        :return: dictionary {date : link}
        '''
        if tdate.strftime("%m%Y") == self.last_updated_month and self.archive_downloaded:
            logger.info(f'Generation data already downloaded for {tdate.strftime("%m%Y")}')
        elif tdate.strftime("%m%Y") != self.last_updated_month:
            self.archive_downloaded = False
            url = "/".join([self.urls[doc_type], str(tdate.year),
                            f'MMSDM_{tdate.year}_{tdate.month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/'])
            try:
                r = requests.get(url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADER)
                soup = BeautifulSoup(r.content, 'html.parser')
                data_elem = soup.find(text=re.compile('PUBLIC_DVD_DISPATCH_UNIT_SCADA'))
                zip_links = {tdate: "/".join([url, data_elem])}
            except:
                logger.info(f'{tdate} File is not available in data archive')
                zip_links = {}
            self.last_updated_month = tdate.strftime("%m%Y")
            self.zipped_links = zip_links

    def download_zips(self, tdate, doc_type, zip_links):
        '''
        This method downloads data from the zipped csv and feeds df_generation_per_plant
        :param tdate: datetime
        :param doc_type: str ('current' or 'archive')
        :param zip_links: dict
        :return:
        '''
        for date in zip_links.keys():
            try:
                r = requests.get(zip_links[date], verify=SSL_CERTIFICATE_PATH)
                z = ZipFile(io.BytesIO(r.content))
                file_name = z.infolist()[0].filename
                df_csv = pd.read_csv(z.open(file_name), delimiter=',', header=1, encoding='unicode_escape')
                if doc_type == 'current':
                    self.df_generation_per_plant[doc_type] = pd.concat([self.df_generation_per_plant[doc_type], df_csv])
                elif doc_type =='archive':
                    self.df_generation_per_plant[doc_type] = df_csv
            except:
                logger.info(f'{tdate} File is not a zip file')
                continue
        self.archive_downloaded = True if doc_type == 'archive' else False

    def format_df(self, doc_type):
        df_generation_per_plant = self.df_generation_per_plant[doc_type]
        df_generation_per_plant = df_generation_per_plant.merge(self.df_generators, on=['DUID'], how='left')
        df_generation_per_plant = df_generation_per_plant.drop(columns={'I', 'DISPATCH', 'UNIT_SCADA', '1'})
        df_generation_per_plant = df_generation_per_plant.dropna(axis=0, how='all')
        df_generation_per_plant = df_generation_per_plant.rename(columns={'SETTLEMENTDATE': 'local_datetime',
                                                                          'SCADAVALUE': 'Value',
                                                                          'DUID': 'Power plant ID',
                                                                          'Fuel Source - Descriptor': 'Product'})
        df_generation_per_plant['Product'] = df_generation_per_plant['Product'].map(self.fuel_mapping,
                                                                                    na_action='ignore')
        df_generation_per_plant['local_datetime'] = pd.to_datetime(df_generation_per_plant['local_datetime'])
        df_generation_per_plant['local_date'] = pd.to_datetime(df_generation_per_plant['local_datetime']).dt.date
        df_generation_per_plant['Region'] = df_generation_per_plant['Region'].map(self.region_mapping,
                                                                                  na_action='ignore')
        df_generation = df_generation_per_plant.groupby(
            ['local_datetime', 'local_date', 'Product', 'Region']).sum().reset_index()
        df_generation['utc_datetime'] = df_generation.apply(
            lambda x: x['local_datetime'].tz_localize(self.utc_tz_mapping.get(x['Region']),
                                                      nonexistent='shift_forward', ambiguous='NaT').tz_convert('UTC'), axis=1)
        df_generation['utc_date'] = pd.to_datetime(df_generation['utc_datetime']).dt.date
        df_generation['Metric'] = 'Generation'
        df_generation['Export Date'] = self.export_datetime
        df_generation['Country'] = 'Australia'
        df_generation['Source'] = 'AEMO NEM'
        df_generation.loc[(~df_generation['Product'].isin(['Hydro', 'Hydro Pumped Storage']))
                          & (df_generation['Value'] < 0), 'Value'] = 0
        self.df_generation_per_plant[doc_type] = pd.concat(
            [self.df_generation_per_plant[doc_type], df_generation_per_plant])
        self.df_generation[doc_type] = pd.concat([self.df_generation[doc_type], df_generation])

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = pd.concat([self.df_generation['current'], self.df_generation['archive']])
        return df_dw

    def run_date(self, tdate):
        if tdate == self.last_available_date:
            doc_type = 'current'
        else:
            doc_type = 'archive'
        self.get_plant_mapping()
        self.get_links(tdate, doc_type)

        if bool(self.zipped_links):
            if doc_type == 'current' or (doc_type == 'archive' and not self.archive_downloaded):
                self.download_zips(tdate, doc_type, self.zipped_links)
                self.format_df(doc_type)
                logger.info(f'DataFrame created for {doc_type} for {tdate.date()}')
        else:
            logger.info(f'No data available for {tdate} - for {doc_type}')




if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    aus_scraper = AustralianGenerationStatsJob()
    aus_scraper.test_run(folder)
