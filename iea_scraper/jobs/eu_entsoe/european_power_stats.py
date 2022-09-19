"""
Created on Mon Oct 19 17:46:03 2020

@authors: 
    NGHIEM_A
    DAUGY_M
    CHAMBEAU_L
"""
import requests
import pandas as pd
import xml.etree.ElementTree as et
from datetime import datetime, timedelta
import pycountry
import logging

import sys
sys.path.append(r'C:\Repos\scraper')
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH

class EuropeanPowerStatsJob(EdcBulkJob):
    
    title: str = 'Entsoe.eu - European Power Statistics'
    
    def __init__(self):
        super().__init__()
        self.token = 'f3486b40-13f6-4558-afcd-03dd94152095'
        self.document_type_mapping = {
            'load': 'A65',
            'generation': 'A73',
            'generation_per_type': 'A75',
            'day_ahead_prices': 'A44'
        }
        self.country_mapping = {
            'Albania': '10YAL-KESH-----5',
            'Austria': '10YAT-APG------L',
            'Bosnia and Herzegovina': '10YBA-JPCC-----D',
            'Belgium': '10YBE----------2',
            'Bulgaria': '10YCA-BULGARIA-R',
            'Belarus': '10Y1001A1001A51S',
            'Switzerland': '10YCH-SWISSGRIDZ',
            'Cyprus': '10YCY-1001A0003J',
            'Czechia': '10YCZ-CEPS-----N',
            'Germany': '10Y1001A1001A83F',
            'Denmark': '10Y1001A1001A65H',
            'Estonia': '10Y1001A1001A39I',
            'Spain': '10YES-REE------0',
            'Finland': '10YFI-1--------U',
            'France': '10YFR-RTE------C',
            'United Kingdom': '10YGB----------A',
            'Greece': '10YGR-HTSO-----Y',
            'Croatia': '10YHR-HEP------M',
            'Hungary': '10YHU-MAVIR----U',
            'Ireland': '10YIE-1001A00010',
            'Italy': '10YIT-GRTN-----B',
            'Lithuania': '10YLT-1001A0008Q',
            'Luxembourg': '10YLU-CEGEDEL-NQ',
            'Latvia': '10YLV-1001A00074',
            'Moldova': 'MD',
            'Montenegro': '10YCS-CG-TSO---S',
            'North Macedonia': '10YMK-MEPSO----8',
            'Malta': '10Y1001A1001A93C',
            'Netherlands': '10YNL----------L',
            'Norway': '10YNO-0--------C',
            'Poland': '10YPL-AREA-----S',
            'Portugal': '10YPT-REN------W',
            'Romania': '10YRO-TEL------P',
            'Serbia': '10YCS-SERBIATSOV',
            'Russia': '10Y1001A1001A49F',
            'Slovenia': '10YSI-ELES-----O',
            'Slovakia': '10YSK-SEPS-----K',
            'Sweden': '10YSE-1--------K',
            'Turkey': '10YTR-TEIAS----W',
            # 'Ukraine': '10YUA-WEPS-----0',
            'Ukraine': '10Y1001C--00003F',
        }
        self.fuel_mapping_complete = {
            'B01': 'Biomass',
            'B02': 'Fossil Brown coal/Lignite',
            'B03': 'Fossil Coal-derived gas',
            'B04': 'Fossil Gas',
            'B05': 'Fossil Hard coal',
            'B06': 'Fossil Oil',
            'B07': 'Fossil Oil shale',
            'B08': 'Fossil Peat',
            'B09': 'Geothermal',
            'B10': 'Hydro Pumped Storage',
            'B11': 'Hydro Run-of-river and poundage',
            'B12': 'Hydro Water Reservoir',
            'B13': 'Marine',
            'B14': 'Nuclear',
            'B15': 'Other renewable',
            'B16': 'Solar',
            'B17': 'Waste',
            'B18': 'Wind Offshore',
            'B19': 'Wind Onshore',
            'B20': 'Other'
        }
        self.fuel_mapping = {
            'B01': 'Biomass',
            'B02': 'Brown Coal',
            'B03': 'Coal',
            'B04': 'Natural Gas',
            'B05': 'Hard Coal',
            'B06': 'Oil',
            'B07': 'Oil',
            'B08': 'Peat',
            'B09': 'Geothermal',
            'B10': 'Hydro Pumped Storage',
            'B11': 'Hydro Run-of-river',
            'B12': 'Hydro Reservoir',
            'B13': 'Tide',
            'B14': 'Nuclear',
            'B15': 'Other Renewables',
            'B16': 'Solar', # includes both solar pv and thermal 
            'B17': 'Waste',
            'B18': 'Wind Offshore',
            'B19': 'Wind Onshore',
            'B20': 'Other'
        }
        self.bidding_zone_mapping = {
            'AL': '10YAL-KESH-----5',  # Albania
            'AT': '10YAT-APG------L',  # Austria
            'BA': '10YBA-JPCC-----D',  # Bosnia
            'BE': '10YBE----------2',  # Belgium
            'BG': '10YCA-BULGARIA-R',  # Bulgaria
            'BY': '10Y1001A1001A51S',  # Belarus
            'CH': '10YCH-SWISSGRIDZ',  # Swiss
            'CY': '10YCY-1001A0003J',  # Cyprus
            'CZ': '10YCZ-CEPS-----N',  # Czech
            # 'CZ-DE-SK': '10YDOM-CZ-DE-SKK',
            # 'DE-50HzT': '10YDE-VE-------2',  # DE(50HzT) BZA
            'DE': '10Y1001A1001A83F',
            'DE-LU': '10Y1001A1001A82H',
            # 'DE-AT-LU': '10Y1001A1001A63L',
            'DK-DK1': '10YDK-1--------W',  # Denmark 1
            'DK-DK2': '10YDK-2--------M',  # Denmark 2
            'EE': '10Y1001A1001A39I',  # Estonia
            'ES': '10YES-REE------0',  # Spain REE
            'FI': '10YFI-1--------U',  # Finland
            'FR': '10YFR-RTE------C',  # France
            'GB': '10YGB----------A',  # UK
            'GB-NIR': '10Y1001A1001A016',  # Northern Ireland
            'GR': '10YGR-HTSO-----Y',  # Greece
            'HR': '10YHR-HEP------M',  # Croatia
            'HU': '10YHU-MAVIR----U',  # Hungary
            'IE': '10Y1001A1001A59C',  # Ireland
            'IT-BR': '10Y1001A1001A699',  # IT-Brindisi BZ
            'IT-CNO': '10Y1001A1001A70O',  # IT-Centre-North BZ
            'IT-CSO': '10Y1001A1001A71M',  # IT-Centre-South BZ
            'IT-FO': '10Y1001A1001A72K',  # IT-Foggia BZ
            'IT-GR': '10Y1001A1001A66F',  # IT-GR BZ
            'IT-MA': '10Y1001A1001A877',  # IT-Malta BZ
            'IT-NO': '10Y1001A1001A73I',  # IT-North BZ
            'IT-AT': '10Y1001A1001A80L',  # IT-North-AT BZ
            'IT-CH': '10Y1001A1001A68B',  # IT-North-CH BZ
            'IT-FR': '10Y1001A1001A81J',  # IT-North-FR BZ
            'IT-SI': '10Y1001A1001A67D',  # IT-North-SI BZ
            'IT-PR': '10Y1001A1001A76C',  # IT-Priolo BZ
            'IT-RO': '10Y1001A1001A77A',  # IT-Rossano BZ
            'IT-SAR': '10Y1001A1001A74G',  # IT-Sardinia BZ
            'IT-SIC': '10Y1001A1001A75E',  # IT-Sicily BZ
            'IT-SO': '10Y1001A1001A788',  # IT-South BZ
            'LT': '10YLT-1001A0008Q',  # Lithuania
            'LU-DE': '10Y1001A1001A82H',
            'LV': '10YLV-1001A00074',  # Latvia
            'ME': '10YCS-CG-TSO---S',  # Montenegro
            'MK': '10YMK-MEPSO----8',  # North Macedonia
            'MO': '10Y1001A1001A990',  # Moldova
            'MT': '10Y1001A1001A93C',  # Malta
            'NL': '10YNL----------L',  # Netherlands
            'NO-NO1': '10YNO-1--------2',  # Norway
            'NO-NO2': '10YNO-2--------T',
            'NO-NO3': '10YNO-3--------J',
            'NO-NO4': '10YNO-4--------9',
            'NO-NO5': '10Y1001A1001A48H',
            'PL': '10YPL-AREA-----S',  # Poland
            'PT': '10YPT-REN------W',  # Portugal
            'RO': '10YRO-TEL------P',  # Romania
            'RS': '10YCS-SERBIATSOV',  # Serbia
            # RU': '10Y1001A1001A49F', Russia
            # 'RU-KGD': '10Y1001A1001A50U',  # RU- Kaliningrad
            'SE-SE1': '10Y1001A1001A44P',  # Sweden
            'SE-SE2': '10Y1001A1001A45N',
            'SE-SE3': '10Y1001A1001A46L',
            'SE-SE4': '10Y1001A1001A47J',
            'SI': '10YSI-ELES-----O',  # Slovenia
            'SK': '10YSK-SEPS-----K',  # Slovakia
            # 'TR': '10YTR-TEIAS----W',# Turkey
            'UA-BEI': '10YUA-WEPS-----0',  # Ukraine
            'UA-IPS': '10Y1001C--000182',  # Ukraine
        }
        self.iso2_to_iso3 = self.get_iso2_to_iso3_mapping()
        self.countryname_to_iso3 = self.get_countryname_to_iso3_mapping()
        self.load_data_points = []
        self.load_all_data_points = []
        self.generation_data_points = []
        self.generation_all_data_points = []
        self.prices_data_points = []
        self.prices_all_data_points = []
        self.trade_data_points = []
        self.trade_all_data_points = []
        self.missing_country_report = {country: {'generation': [], 'demand': [], 'import': []}
                                       for country in self.country_mapping.keys()}
        self.missing_prices_report = {bzn: {'prices': []}
                                      for bzn in self.bidding_zone_mapping.keys()}

    @property
    def offset_now(self):
        return 0
            
    def get_iso2_to_iso3_mapping(self):
        code_country = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
        return code_country
    
    def get_countryname_to_iso3_mapping(self):
        code_country = {country.name: country.alpha_3 for country in pycountry.countries}
        return code_country
    
    def scrape_load(self, tdate, country):
        '''
        tdate : datetime
        country : str
        '''
        self.load_data_points = []
        url_api = 'https://transparency.entsoe.eu/api?'
        payload = {'securityToken': '16f9e687-746c-4b6c-ad36-850133d639ec',
                   'documentType': 'A65',
                   'processType': 'A16',
                   'outBiddingZone_Domain': self.country_mapping[country],
                   'PeriodStart': tdate.strftime('%Y%m%d0000'),
                   'PeriodEnd': (tdate + timedelta(days=1)).strftime('%Y%m%d0000')}
        url = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            root = et.fromstring(r.content)
            space_name = root[0].tag[0:-4]
            time_seriess = root.findall(space_name + 'TimeSeries')
            data_points = []
            for time_series in time_seriess:
                period = time_series.find(space_name + 'Period')
                time_interval = period.find(space_name + 'timeInterval')
                start = time_interval.find(space_name + 'start').text
                start_date = datetime.strptime(start,'%Y-%m-%dT%H:%MZ')
                resolution = timedelta(minutes=int(period.find(space_name + 'resolution').text[2:4]))
                data = period.findall(space_name + 'Point')
                for el in data:
                    position = int(el.find(space_name + 'position').text)
                    quantity = int(el.find(space_name + 'quantity').text)
                    tdate_time = start_date + resolution * (position - 1)
                    data_point = {'utc_datetime': tdate_time,
                                  'utc_date': tdate_time.date(),
                                  'demand_mw': quantity,
                                  'Country': self.countryname_to_iso3[country],
                                  'Source': 'Entso-e',
                                  'export_date': self.export_date,
                                  }
                    data_points += [data_point]
            self.load_data_points = data_points
            self.load_all_data_points += self.load_data_points
            logger.info(f'{tdate.date()} {country} demand was scraped')
        else:
            logger.info(f'{tdate.date()} {country} demand data not available')
            self.missing_country_report[country]['demand'] = tdate

    def scrape_loads(self, tdate, countries=[]):
        '''
        tdate : datetime
        countries : list of strings
        '''
        if not countries:
            countries = list(self.country_mapping.keys())
        for country in countries:
            self.scrape_load(tdate, country)

    def scrape_generation(self, tdate, country, fuel_code):
        '''
        tdate : datetime
        country : string
        '''
        url_api = 'https://transparency.entsoe.eu/api?'
        payload = {'securityToken': 'f3486b40-13f6-4558-afcd-03dd94152095',
                   'documentType': 'A75',
                   'processType': 'A16',
                   'in_Domain': self.country_mapping[country],
                   'psrType': fuel_code,  # all fuels accessed on the same page
                   'PeriodStart': tdate.strftime('%Y%m%d0000'),
                   'PeriodEnd': (tdate + timedelta(days=1)).strftime('%Y%m%d0000')}
        url = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            root = et.fromstring(r.content)
            space_name = root[0].tag[0:-4]
            data_points_aggregated = []
            for child in root:
                if child.tag[-10:] == 'TimeSeries':
                    # could use a check on the curve_type: A03 --> outage A01 --> OK
                    period = child.find(space_name + 'Period')
                    time_series_number = int(child[0].text)
                    resolution = timedelta(minutes=int(period.find(space_name + 'resolution').text[2:4]))
                    if time_series_number % 2 == 1:
                        flow_type = 'Actual generation'
                    else:
                        flow_type = 'Actual consumption'
                    # Actual generation output or Actual consumption ('own use').
                    data = period.findall(space_name + 'Point')
                    data_points = []

                    for el in data:
                        position = int(el.find(space_name + 'position').text)
                        quantity = int(el.find(space_name + 'quantity').text)
                        tdate_time = tdate + resolution * (position - 1)
                        data_point = {'utc_datetime': tdate_time,
                                      'production_type': self.fuel_mapping[fuel_code],  # 'B01',
                                      'generation_mw': quantity,
                                      'type': flow_type,
                                      'Country': self.countryname_to_iso3[country]}
                        data_points += [data_point]
                    data_points_aggregated += data_points
            self.generation_all_data_points += data_points_aggregated
            logger.info(f'{tdate.date()} {country} was scraped for generation')
        else:
            logger.info(f'{tdate.date()} {country} {self.fuel_mapping[fuel_code]} '
                         'No generation data for that fuel')

    def scrape_generation_all_fuels(self, tdate, country):
        '''
        tdate : datetime
        country : str
        '''
        url_api = 'https://transparency.entsoe.eu/api?'
        payload = {'securityToken': 'f3486b40-13f6-4558-afcd-03dd94152095',
                   'documentType': 'A75',
                   'processType': 'A16',
                   'in_Domain': self.country_mapping[country],
                   'PeriodStart': tdate.strftime('%Y%m%d0000'),
                   'PeriodEnd': (tdate + timedelta(days=1)).strftime('%Y%m%d0000')}
        url = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            root = et.fromstring(r.content)
            space_name = root[0].tag[0:-4]
            data_points_aggregated = []
            time_series_lst = root.findall(space_name + 'TimeSeries')
            for time_series in time_series_lst:
                data_points = []
                type_of_use = 'Own use' if time_series.find(
                    space_name + 'inBiddingZone_Domain.mRID') is None else 'Generation'
                fuel_code = time_series.find(space_name + 'MktPSRType')[0].text
                fuel = self.fuel_mapping[fuel_code]
                period = time_series.find(space_name + 'Period')
                resolution = timedelta(minutes=int(period.find(space_name + 'resolution').text[2:4]))
                tdate_start = pd.to_datetime(period.find(space_name + 'timeInterval').find(space_name + 'start').text,
                                             format='%Y-%m-%dT%H:%MZ')
                data = period.findall(space_name + 'Point')
                for el in data:
                    position = int(el.find(space_name + 'position').text)
                    quantity = int(el.find(space_name + 'quantity').text)
                    quantity = -quantity if type_of_use == 'Own use' else quantity
                    tdate_time = tdate_start + resolution * (position - 1)
                    data_point = {'utc_datetime': tdate_time,
                                  'utc_date': tdate_time.date(),
                                  'production_type': fuel,
                                  'generation_mw': quantity,
                                  'type_of_use': type_of_use,
                                  'Country': self.countryname_to_iso3[country],
                                  'Source': 'Entso-e',
                                  'export_date': self.export_date,
                                  }
                    data_points += [data_point]
                data_points_aggregated += data_points
            self.generation_all_data_points += data_points_aggregated
            logger.info(f'{tdate.date()} {country} was scraped for generation')
        else:
            logger.info(f'{tdate.date()} {country}. No generation data')
            self.missing_country_report[country]['generation'] = tdate

    def scrape_generations(self, tdate, countries=[]):
        if not countries:
            countries = list(self.country_mapping.keys())
        for country in countries:
            self.scrape_generation_all_fuels(tdate, country)

    def scrape_price(self, tdate, bzn):
        """
        :type tdate:datetime
        :type bzn: str

        """
        url_api = 'https://transparency.entsoe.eu/api?'
        payload = {'securityToken': 'f3486b40-13f6-4558-afcd-03dd94152095',
                   'documentType': 'A44',
                   'processType': 'A01',
                   'in_Domain': self.bidding_zone_mapping[bzn],
                   'out_Domain': self.bidding_zone_mapping[bzn],
                   'PeriodStart': tdate.strftime('%Y%m%d0000'),
                   'PeriodEnd': (tdate + timedelta(days=1)).strftime('%Y%m%d0000')}
        url = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            root = et.fromstring(r.content)
            space_name = root[0].tag[0:-4]
            time_series = root.find(space_name + 'TimeSeries')
            period = time_series.find(space_name + 'Period')
            resolution = timedelta(minutes=int(period.find(space_name + 'resolution').text[2:4]))
            currency = str(time_series.find(space_name + "currency_Unit.name").text)
            data = period.findall(space_name + 'Point')
            data_points = []
            for el in data:
                position = int(el.find(space_name + 'position').text)
                price_amount = float(el.find(space_name + 'price.amount').text)
                tdate_time = tdate + resolution * (position - 1)
                data_point = {'utc_datetime': tdate_time,
                              'utc_date': tdate_time.date(),
                              'price_EUR': price_amount,
                              'bidding_zone': bzn,
                              'Country': self.iso2_to_iso3[bzn[:2]],
                              'Source': 'Entso-e',
                              'export_date': self.export_date,
                              'Flow 1': 'Spot',
                              'Flow 2': currency
                              }
                data_points += [data_point]
            self.prices_data_points = data_points
            self.prices_all_data_points += self.prices_data_points
            logger.info(f'{tdate.date()} Domain {bzn} price was scraped')
        else:
            logger.info(f'{tdate.date()} Domain {bzn} price data not available')
            self.missing_prices_report[bzn]['prices'] = tdate

    def scrape_prices(self, tdate, in_bzn=[]):
        if not in_bzn:
            in_bzn = list(self.bidding_zone_mapping.keys())
        for bzn in in_bzn:
            try: #LC 28-08-21: added a try/except to skip 'empty' bidding zones
                self.scrape_price(tdate, bzn)
            except AttributeError:
                logger.info(f'No matching data found for Data item Day-ahead Prices [12.1.D] - {bzn}')


    def scrape_trade(self, tdate, in_country, out_country):
        url_api = 'https://transparency.entsoe.eu/api?'
        payload = {'securityToken': '16f9e687-746c-4b6c-ad36-850133d639ec',
                   'documentType': 'A11',
                   'processType': 'A16',
                   'in_Domain': self.country_mapping[in_country],
                   'out_Domain': self.country_mapping[out_country],
                   'PeriodStart': tdate.strftime('%Y%m%d0000'),
                   'PeriodEnd': (tdate + timedelta(days=1)).strftime('%Y%m%d0000')}
        url = url_api + '&'.join([param + '=' + value for param, value in payload.items()])
        r = requests.get(url, verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            root = et.fromstring(r.content)
            space_name = root[0].tag[0:-4]
            data_points_aggregated = []
            time_series_lst = root.findall(space_name + 'TimeSeries')
            for time_series in time_series_lst:
                if time_series.find(space_name + 'out_Domain.mRID') is None:
                    continue
                else:
                    period = time_series.find(space_name + 'Period')
                    resolution = timedelta(minutes=int(period.find(space_name + 'resolution').text[2:4]))
                    data = period.findall(space_name + 'Point')
                    data_points = []
                    for el in data:
                        position = int(el.find(space_name + 'position').text)
                        quantity = int(el.find(space_name + 'quantity').text)
                        tdate_time = tdate + resolution * (position - 1)
                        data_point = {'utc_datetime': tdate_time,
                                      'utc_date': tdate_time.date(),
                                      'physical_flow_mw': quantity,
                                      'in_country': in_country,
                                      'out_country': out_country,
                                      'Source': 'Entso-e',
                                      'export_date': self.export_date,
                                      }
                        data_points += [data_point]
                    data_points_aggregated += data_points
                self.trade_data_points = data_points_aggregated
                logger.info(f'{tdate.date()} To {in_country} From {out_country} flow data was scraped')

    def scrape_trades(self, date_start, date_end, in_countries=[], out_countries=[]):
        self.load_all_data_points = []
        if not in_countries and not out_countries:
            in_countries = list(self.country_mapping.keys())
            out_countries = list(self.country_mapping.keys())
        for in_country in in_countries:
            for out_country in out_countries:
                for tdate in pd.date_range(date_start, date_end, freq='1D'):
                    self.scrape_trade(tdate, in_country, out_country)
                    self.trade_all_data_points += self.trade_data_points

    def print_missing_countries_report(self):
        print('***Missing generation***')
        for country in self.missing_country_report:
            if self.missing_country_report[country]['generation']:
                print(country)
        print('***Missing demand***')
        for country in self.missing_country_report:
            if self.missing_country_report[country]['demand']:
                print(country)

    def print_missing_prices_report(self):
        print('***Missing prices***')
        for bzn in self.missing_prices_report:
            if self.missing_prices_report[bzn]['prices']:
                print(bzn)

    @property
    def df_load(self):
        return pd.DataFrame(self.load_all_data_points)

    @property
    def df_generation(self):
        df_generation = pd.DataFrame(self.generation_all_data_points)
        return df_generation

    @property
    def df_prices(self):
        return pd.DataFrame(self.prices_all_data_points)

    @property
    def df_trades(self):
        return pd.DataFrame(self.trade_all_data_points)

    @property
    def df_dw(self):
        df_ivo = pd.concat([self.df_load_ivo, self.df_generation_ivo, self.df_prices_ivo], axis=0)
        return df_ivo

    @property
    def df_load_ivo(self):
        df_load = self.df_load.rename(columns={'demand_mw': 'Value',
                                               'export_date': 'Export Date'})
        df_load['Metric'] = 'Demand'
        df_load['Product'] = 'ELE'
        return df_load

    @property
    def df_generation_ivo(self):
        df_generation = self.df_generation.rename(columns={'production_type': 'Product',
                                                           'type_of_use': 'Flow 2',
                                                           'generation_mw': 'Value',
                                                           'export_date': 'Export Date'})
        df_generation['Metric'] = 'Generation'
        return df_generation

    @property
    def df_prices_ivo(self):  # a revoir
        df_prices = self.df_prices.rename(columns={'price_EUR': 'Value',
                                                   'export_date': 'Export Date',
                                                   'bidding_zone': 'Region'})
        df_prices['Metric'] = 'Prices'
        df_prices['Product'] = 'ELE'
        df_ivo = df_prices
        return df_ivo

    def perform(self, date_start, date_end, folder):
        self.scrape_loads(date_start, date_end)
        self.scrape_generations(date_start, date_end)
        self.scrape_prices(date_start, date_end)
        self.print_missing_countries_report()
        self.print_missing_prices_report()
        self.to_csv(folder)
        
    def run_date(self, tdate):
        self.scrape_loads(tdate)
        self.scrape_generations(tdate)
        self.scrape_prices(tdate)

class EuropeanCountryPowerStatsJob(EuropeanPowerStatsJob):
    '''Scraper for just one country. Generation. Demand. Prices'''
    
    def __init__(self, country):
        super().__init__()
        self.country = country
        self.country_iso2 = pycountry.countries.get(name=self.country).alpha_2
        self.associated_bidding_zones = [zone for zone in self.bidding_zone_mapping
                                         if zone[:2] == self.country_iso2]
    
    def run_date(self, tdate):
        self.scrape_load(tdate, self.country)
        self.scrape_generation_all_fuels(tdate, self.country)
        self.scrape_prices(tdate, in_bzn=self.associated_bidding_zones)
  
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    # entsoe_scraper = EuropeanPowerStatsJob()
    # entsoe_scraper.test_run(folder)
    bg_scraper = EuropeanCountryPowerStatsJob('Austria')
    bg_scraper.run_date(datetime(2020,4,3))
    bg_scraper.plot_all()
