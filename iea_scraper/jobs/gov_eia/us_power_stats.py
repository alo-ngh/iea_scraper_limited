"""
Notes:  
    Source:             Energy Information Administration (EIA)
    API Query Browse:   https://www.eia.gov/opendata/qb.php
    API Documentation:  https://www.eia.gov/opendata/commands.php
                        https://www.eia.gov/developer/


    Currently, EIA's API contains the following main data sets:
        -Hourly electricity operating data, including actual and forecast demand, net generation, and the power flowing between electric systems
        -408,000 electricity series organized into 29,000 categories
        -30,000 State Energy Data System series organized into 600 categories
        -115,052 petroleum series and associated categories
        -34,790 U.S. crude imports series and associated categories
        -11,989 natural gas series and associated categories
        -132,331 coal series and associated categories
        -3,872 Short-Term Energy Outlook series and associated categories
        -368,466 Annual Energy Outlook series and associated categories
        -92,836 International energy series


    1/ API Commands:
        [api_key]    Required. '086ae35784082682b2fbae061a5ebc5f'
        [out]        Optional. Valid values are “xml” or “json”.

    2/ API Series Query
        Properties:
        -name, 
        -series key, 
        -first date in series, 
        -last date in series, 
        -units long, 
        -units abbreviated, 

        -periodicity, 
        -last updated date, 
        -notes, 
        -copyright

        Note: For the Electricity branch of the EIA API,
        the series' notes field is a concatenation of the data set's definition,
        the fuel filter's definition, and the sector's definition, and,
        (if it exists) the geography definition.

        api_call = http://api.eia.gov/series/?series_id=sssssss&api_key=YOUR_API_KEY_HERE[&num=][&out=xml|json]

        Note: series_id: Required. 
        The series id (also called source key) is a case-insensitive string consisting of letters, 
        numbers, dashes ("-") and periods (".") that uniquely identifies an EIA series. 
        Multiple series can be fetched in a single request by using a semi-colon separated list of 
        series id's. The number of series in a single request is limited to 100.

    3/ API Geoset Query

        api_call = http://api.eia.gov/geoset/?geoset_id=sssssss&regions=region1,region2,region3,...&api_key=YOUR_API_KEY_HERE[&start=|&num=][&end=][&out=xml|json]

        [geoset_id]:  Required.   The series id (also called source key) is a case-insensitive string 
        consisting of letters, numbers, dashes ("-") and periods (".") that uniquely identifies an 
        EIA series.
        [regions]:    Required.   A semicolon-separated list of region codes requested. 
        Series whose geoset_id and region fields match will be returned.

    4/ API Relation Query

        Gets a set of the series belonging to the relation requested for the region requested. 
        A relation is an EIA defined metadata structure that indicates breakdowns or details of 
        summary statistics into composite statistics. Relations are defined between geosets, 
        and therefore apply to all of the geoset's time series.

        api_call = http://api.eia.gov/relation/?relation_id=rrrrrrr&region=region1&api_key=YOUR_API_KEY_HERE[&start=|&num=][&end=][&out=xml|json]

        [geoset_id]:    Required.   The series id (also called source key) is a case-insensitive 
        string consisting of letters, numbers, dashes ("-") and periods (".") that uniquely identifies 
        an EIA series.
        [regions]:      Required.   A semicolon-separated list of region codes requested. 
        Series whose geoset_id and region fields match will be returned.

    5/ API Category Query
        Gets name and id for a single category, and also lists its children categories' names and ids.

        api_call = http://api.eia.gov/category/?api_key=YOUR_API_KEY_HERE[&category_id=nn][&out=xml|json]

        [category_id]:  Optional.   A unique numerical id of the category to fetch. If missing, 
        the API's root category is fetched.


    6/ API Series Categories Query

        Gets a list of category names and IDs the series is a member of.

        api_call = http://api.eia.gov/series/categories/?series_id=&api_key=YOUR_API_KEY_HERE[&out=xml|json]

        [series_id]:  Required.   The series id (also called source key) is a case-insensitive string 
        consisting of letters, numbers, dashes ("-") and periods (".") that uniquely identifies an 
        EIA series.

    7/ API Updated Data Query

        The update query avoids continuously requesting all the series  by allowing your application 
        to find out if anything has been updated in electricity prices for example, 
        and only quest data is the series have been updated using the series/data query.

        Returns a paginated list of series in descending order by the series' last updated date 
        (i.e. most recent updates first). Only the series_id and the series updated date are returned. 
        If a category_id is specified, only series belonging to that category are checked. 
        If a start category is not specified, the query defaults to the API's root category. 
        If the optional variable "deep" is set to true, the entire branch of the category tree if 
        checked for updates, otherwise only series belonging to the specified category are checked.

        api_call = http://api.eia.gov/updates/?api_key=YOUR_API_KEY_HERE[&category_id=X][&deep=true|false][&firstrow=nnnnn][&rows=nn][&out=xml|json]

        [category_id]:      Optional.   A unique numerical id of the start category to fetch. If missing, the API's root category is fetched.
        [deep]:             Optional.   If true, include the series in all descendent categories. If missing or false, only series directly in the start category will be returned.
        [rows]:             Optional.   Determines the maximum number of rows returned for each request, up to 10,000. Missing or invalid value results a default value of 50 as the maximum rows returned with each call.
        [firstrow]:         Optional.   Integer specifying the zero-based index of the first row to return, providing a means to page through the updated series. Note that it is possible to page through the all of the API's series in this manner

    8/ API Search Data Query

        Returns the series ID as an array followed by series facet data as an array. 
        Additional codes may be defined in future releases.

        8.1 Series ID Search

        api_call = http://api.eia.gov/search/?search_term=series_id&search_value="PET.MB"

        8.2 Keyword search

        api_call = http://api.eia.gov/search/?search_term=name&search_value="crude oil"

        8.3 Date search

        api_call = http://api.eia.gov/search/?search_term=last_updated&search_value=[2015-01-01T00:00:00Z TO 2015-01-01T23:59:59Z]

        8.4 Pagination on Search (default page number = 1)

        api_call = http://api.eia.gov/search/?search_term=name&search_value="crude oil"&page_num=4

        8.5 Manipulate Rows Per Page

        api_call = http://api.eia.gov/search/?search_term=name&search_value="crude oil"&rows_per_page=25&page_num=4

"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import sys
sys.path.append(r'C:\Repos\Scraper')

from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.settings import PROXY_DICT, SSL_CERTIFICATE_PATH

class UsPowerStatsJob(EdcBulkJob):
    title: str = "EIA - US electricity data"
    def __init__(self):
        EdcBulkJob.__init__(self)
        self.api_key = 'b3b22187c8108c7f2bfdc5a7ad1e4641'
        self.load_data = []
        self.load_all_data = []
        self.generation_data = []
        self.generation_all_data = []
        self.region_mapping = self.get_region_mapping()
        self.region_mapping_id = self.get_region_mapping_id()
        self.fuel_mapping = self.get_fuel_mapping()
        
    def get_region_mapping(self):
        region_mapping = {      
            'California' : 'CAL',
            'Carolinas' : 'CAR',
            'Central': 'CENT',
            'Florida' : 'FLA',
            'Mid-Atlantic': 'MIDA',
            'Midwest': 'MIDW',
            'New England' : 'NE',
            'New York' : 'NY',
            'Northwest' : 'NW',
            'Southeast' : 'SE',
            'Southwest' : 'SW',
            'Tennessee' : 'TEN',
            'Texas' : 'TEX'}
        return region_mapping

    def get_region_mapping_id(self):
        region_mapping_id = {
            'California': '3390106',
            'Carolinas': '3390107',
            'Central': '3390108',
            'Florida': '3390109',
            'Mid-Atlantic': '3390110',
            'Midwest': '3390111',
            'New England': '3390112',
            'New York': '3390113',
            'Northwest': '3390114',
            'Southeast': '3390115',
            'Southwest': '3390116',
            'Tennessee': '3390117',
            'Texas': '3390118'
        }
        return region_mapping_id
    
    def get_fuel_mapping(self):
        fuel_mapping = { 
                        'Wind': 'Wind Onshore',
                        'Petroleum': 'Oil'}
        return fuel_mapping
    
    def scrape_load_region(self, tdate, region):
        url_api = 'https://api.eia.gov/series/?'
        self.load_data = []
        payload = {'api_key': self.api_key,
                   'series_id': f'EBA.{self.region_mapping[region]}-ALL.D.H',
                   'start': tdate.strftime('%Y%m%dT00Z'),
                   'end': tdate.strftime('%Y%m%dT23Z')}
        api_call = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        json_file = requests.get(api_call, verify=SSL_CERTIFICATE_PATH, proxies=PROXY_DICT).json()
        self.json_file = json_file
        data_points = [{
            'utc_datetime': datetime.strptime(tdatetimestr,'%Y%m%dT%HZ'),
            'utc_date': datetime.strptime(tdatetimestr,'%Y%m%dT%HZ').date(),
            'Region': region,
            'Export Date': self.export_date,
            'Value': value} for tdatetimestr, value in json_file['series'][0]['data']]
        self.load_data = data_points
        logger.info(str(tdate.date()) + ' ' + region + ' was scraped for load.')

    def scrape_load_usa(self, tdate_start, tdate_end):
        for tdate in pd.date_range(tdate_start, tdate_end, freq='1D'):
            for region in self.region_mapping:
                self.scrape_load_region(tdate, region)                
                self.load_all_data += self.load_data

    def scrape_generation_region(self, tdate, region):
        key = self.api_key
        self.generation_data = []
        url_api = 'https://api.eia.gov/series/?'
        category_id = self.region_mapping_id[region]
        category_call = 'https://api.eia.gov/category/?api_key=' + key + '&category_id=' + category_id
        json_file_cat = requests.get(category_call, verify=SSL_CERTIFICATE_PATH, proxies=PROXY_DICT).json()
        if 'data' in json_file_cat and 'error' in json_file_cat['data']:
            raise EdcJobError(json_file_cat['data']['error'])
        fuels = {element['series_id']: element['name'] for element in json_file_cat['category']['childseries'] if element['f'] == 'H'}
        for fuel in fuels:
            payload_series = {
                'api_key': key,
                'series_id': fuel,
                'start': tdate.strftime('%Y%m%dT00Z'),
                'end': tdate.strftime('%Y%m%dT23Z')}
            api_call = url_api + '&'.join([param + '=' + value for param, value in payload_series.items()])
            json_file = requests.get(api_call, verify=SSL_CERTIFICATE_PATH).json()    
            product_name = (fuels[fuel][fuels[fuel].find('from') + 5:fuels[fuel].find('for')]).title().strip()
            product_name = self.fuel_mapping[product_name] if product_name in self.fuel_mapping else product_name
            data_points = [{
                'utc_datetime': datetime.strptime(tdatetimestr,'%Y%m%dT%HZ'),
                'utc_date': datetime.strptime(tdatetimestr,'%Y%m%dT%HZ').date(),
                'Region': region,
                'Product': product_name,
                'Export Date': self.export_date,
                'Value': value} for tdatetimestr, value in json_file['series'][0]['data']] 
            self.generation_data += data_points
        logger.info(str(tdate.date()) + ' ' +  region + ' was scraped for generation.')           

    def scrape_generation_usa(self, tdate_start, tdate_end):
        for tdate in pd.date_range(tdate_start, tdate_end, freq='1D'):
            for region in self.region_mapping:
                self.scrape_generation_region(tdate, region)                
                self.generation_all_data += self.generation_data      
                #the output for generation can be checked against this source: https://www.eia.gov/beta/electricity/gridmonitor/expanded-view/electric_overview/US48/US48/GenerationByEnergySource-4/edit  

    @property
    def offset_now(self):
        return 1

    @property
    def df_load(self):
        return pd.DataFrame(self.load_all_data)

    @property
    def df_generation(self):
        return pd.DataFrame(self.generation_all_data)

    @property
    def df_dw(self):
        df_load = self.df_load
        df_load['Country'] = 'United States'
        df_load['Metric'] = 'Demand'
        df_load['Product'] = 'ELE'
        df_load['Source'] = 'EIA'
        df_load['Flow 1'] = np.nan
        df_load['Flow 2'] = np.nan
        df_load = df_load[['utc_datetime','utc_date', 'Export Date', 'Country',
         'Region', 'Metric', 'Product', 'Source', 'Flow 1', 'Flow 2', 'Value']]
        df_generation = self.df_generation
        df_generation['Country'] = 'United States'
        df_generation['Metric'] = 'Generation'
        df_generation['Source'] = 'EIA'
        df_generation['Flow 1'] = np.nan
        df_generation['Flow 2'] = np.nan
        df_generation = df_generation[['utc_datetime','utc_date', 'Export Date', 'Country',
         'Region', 'Metric', 'Product', 'Source', 'Flow 1', 'Flow 2', 'Value']]
        
        df_dw = pd.concat([df_load, df_generation], axis=0)
        return df_dw

    def perform(self, date_start, date_end, folder):
        self.scrape_load_usa(date_start, date_end)
        self.scrape_generation_usa(date_start, date_end)
        self.to_csv(folder)
    
    def run_date(self, tdate):
        self.scrape_generation_usa(tdate, tdate)
        self.scrape_load_usa(tdate, tdate)

if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    eia_scraper = UsPowerStatsJob()
    eia_scraper.test_run()
