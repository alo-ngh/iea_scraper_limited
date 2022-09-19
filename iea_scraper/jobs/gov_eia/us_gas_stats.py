# %%
"""
Collects data from the eia at three different scraping frequencies:
daily: prices (spot price and 4 futures).
weekly: supply, demand, trade and physical prices infromation; volumes are weekly, prices are daily.
monthly: production, demand, trade and some daily LNG trade flows.
The overall scraper runs everyday, the date of the date determines if weekly and monthly scrapers should run.
 """
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import sys
sys.path.append(r"C:\Repos\iea_scraper")

from iea_scraper.core.job import EdcGasBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH
from iea_scraper.core.utils import get_country_iso3

import logging
from pathlib import Path
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class UsGasStatsJob(EdcGasBulkJob):
    title: str = '"eia.gov - US natural gas data"'

    def __init__(self):
        super().__init__()
        self.api_key = 'b3b22187c8108c7f2bfdc5a7ad1e4641'
        self.underlyings = { #different maturities scraped for prices
            "NG.RNGWHHD.D": ["Average Natural Gas Spot Price", 'Spot'],
            "NG.RNGC1.D": ["Average Henry Hub Natural Gas Futures Price", "1M"],
            "NG.RNGC2.D": ["Average Henry Hub Natural Gas Futures Price", "2M"],
            "NG.RNGC3.D": ["Average Henry Hub Natural Gas Futures Price", "3M"],
            "NG.RNGC4.D": ["Average Henry Hub Natural Gas Futures Price", "4M"]}
        self.sectors = { #breakdown of consumption - some to be removed to prevent double counting
            'Residential Consumption': 'Residential',
            'Commercial Consumption': 'Commercial',
            'Industrial Consumption': 'Industry',
            'Vehicle Fuel Consumption': 'Transport (including pipeline)',
            'Plant Fuel Consumption': 'Own use',
            'Total Consumption': 'IGNORE',
            'Lease Fuel Consumption': 'Own use',
            'Lease and Plant Fuel Consumption': 'IGNORE',
            'Electric Power Consumption': 'Power generation',
            'Pipeline Fuel Consumption': 'Pipeline transportation',
            'Delivered to Consumers': 'Delivered to consumers'}
        self.url_monthly = 'https://api.eia.gov/v2/natural-gas/sum/lsum/data/'
        self.url_weekly = 'https://www.eia.gov/naturalgas/weekly/archivenew_ngwu/'
        self.url_daily = 'https://api.eia.gov/v2/natural-gas/pri/fut/data/'
        self.base_url_lng = "https://www.energy.gov/sites/default/files/"
        self.end_url_lng = "/LNG%20Monthly%20"
        self.df_lng = pd.DataFrame()
        self.df_daily_raw = pd.DataFrame()
        self.df_daily = pd.DataFrame()
        self.df_monthly = pd.DataFrame()
        self.df_monthly_raw = pd.DataFrame()
        self.reference = None
        self.df_weekly = pd.DataFrame()
        self.list_data_prices = []
        self.df_weekly_prices_raw = pd.DataFrame()
        self.list_data_supply = []
        self.df_weekly_supply_raw = pd.DataFrame()
        self.list_data_demand = []
        self.df_weekly_demand_raw = pd.DataFrame()
        self.list_data_trade = []
        self.dict_process_to_metric = {
            'EEX': 'Trade',
            'ENG': 'Trade',
            'ENP': 'Trade',
            'FGC': 'Production',
            'FGG': 'Production',
            'FGO': 'Production',
            'FGS': 'Production',
            'FGW': 'Production',
            'FPD': 'Production',
            'FWA': 'Prices',
            'IM0': 'Trade',
            'IML': 'Trade',
            'IRP': 'Trade',
            'PCS': 'Prices',
            'PDV': 'Prices',
            'PEU': 'Prices',
            'PEX': 'Prices',
            'PG1': 'Prices',
            'PGP': 'Prices',
            'PIN': 'Prices',
            'PM0': 'Prices',
            'PML': 'Prices',
            'PNG': 'Prices',
            'PNP': 'Prices',
            'PRP': 'Prices',
            'PRS': 'Prices',
            'R11': 'ignore',
            'R12': 'ignore',
            'R13': 'ignore',
            'R14': 'ignore',
            'R15': 'ignore',
            'R16': 'ignore',
            'R17': 'ignore',
            '17A': 'ignore',
            'R18': 'ignore',
            'R19': 'ignore',
            'R20': 'ignore',
            'SAB': 'ignore',
            'SAC': 'ignore',
            'SAI': 'ignore',
            'SAL': 'ignore',
            'SAN': 'ignore',
            'SAO': 'ignore',
            'SAT': 'ignore',
            'SAW': 'ignore',
            'SLI': 'ignore',
            'SLW': 'ignore',
            'VC0': 'ignore',
            'VCF': 'Demand',
            'VCL': 'Demand',
            'VCS': 'Demand',
            'VDV': 'Demand',
            'VEU': 'Demand',
            'VG9': 'Demand',
            'VGL': 'Demand',
            'VGM': 'Production',
            'VGP': 'Demand',
            'VGQ': 'Demand',
            'VGT': 'Demand',
            'VGV': 'Demand',
            'VIN': 'Demand',
            'VRN': 'Demand',
            'VRS': 'Demand',
            'XDC': 'ignore',
            'XDG': 'ignore',
            'YGP': 'Demand'
            }
        self.dict_states_3 = {
            'NUS': 'All states', 'SAK': 'Alaska', 'SAL': 'Alabama', 'SAR': 'Arkansas', 'SAZ': 'Arizona', 'SCA': 'California', 'SCO': 'Colorado', 'SCT': 'Connecticut', 'SDC': 'District of Columbia',  'SDE': 'Delaware', 'SFL': 'Florida', 'SGA': 'Georgia', 'SHI': 'Hawaii', 'SIA': 'Iowa', 'SID': 'Idaho', 'SIL': 'Illinois', 'SIN': 'Indiana', 'SKS': 'Kansas', 'SKY': 'Kentucky', 'SLA': 'Louisiana', 'SMA': 'Massachusetts', 'SMD': 'Maryland', 'SME': 'Maine', 'SMI': 'Michigan', 'SMN': 'Minnesota', 'SMO': 'Missouri', 'SMS': 'Mississippi', 'SMT': 'Montana', 'SNC': 'Carolina', 'SND': 'North Carolina', 'SNE': 'Nebraska', 'SNH': 'Hampshire', 'SNJ': 'New Jersey', 'SNM': 'New Mexico', 'SNV': 'Nevada', 'SNY': 'New York', 'SOH': 'Ohio', 'SOK': 'Oklahoma', 'SOR': 'Oregon', 'SPA': 'Pennsylvania', 'SRI': 'Rhode Island', 'SSC': 'South Carolina',  'SSD': 'South Dakota', 'STN': 'Tennessee', 'STX': 'Texas', 'SUT': 'Utah', 'SVA': 'Virginia', 'SVT': 'Vermont', 'SWA': 'Washington', 'SWI': 'Wisconsin', 'SWV': 'West Virginia', 'SWY': 'Wyoming'}
        self.dict_maturities = {
            'PS0': 'Spot',
            'PE1': '1M',
            'PE2': '2M',
            'PE3': '3M',
            'PE4': '4M'}
        self.exports_columns = { #for renaming lng exports
                            'Date of Departure': 'local_date',
                            'Docket Term': 'Flow 5',
                            'Country of Destination': 'Flow 3',
                            'Departure Terminal': 'Flow 2',
                            'Volume (Mcf of Natural Gas)': 'Value'}
        self.imports_columns = { #for renaming lng imports
                'Date of Arrival': 'local_date',
                'Country of Origin': 'Flow 3',
                'Receiving Terminal': 'Flow 2',
                'Volume (Mcf of Natural Gas)': 'Value'}
    
    @property
    def offset_now(self):
        return 2

    @property
    def df_dw(self):
        return pd.concat([self.df_daily, self.df_weekly, self.df_monthly, self.df_lng])

    def triggers(self, tdate):
        """Based on the date, returns 0 or 1 to decide on kicking-off the scrapers.
        - daily: runs daily, no trigger needed.
        - weekly: runs just twice per week on day 4 (Friday) and 5 (Saturday).
        - monthly: runs only during the last week of the month (26th onwards).

            Parameters
            ----------
            - `tdate`:
                day of data collection

            Returns
            -------
            - `trigger_week`:
                True on Fridays and Saturdays
            - `trigger_month`:
                True from the 26th to the end of the month

        """
        trigger_week = True if tdate.weekday() in [4, 5] else False
        trigger_month = True if tdate.day > 25 else False
        return trigger_week, trigger_month

    def run_date(self, tdate: datetime):
        """Calls individually the three processes when appropriate based on the date of the month.
        
            Parameters
            ----------
            - `tdate`: 
                day of data collection

            Calls
            -----
            - `triggers(tdate)`
            - `run_date_daily(tdate)`
            - `run_date_weekly(tdate)`: on Fridays and Saturdays
            - `run_date_monthly(tdate)`: from the 26th to the end of the month
            """
        trig_week, trig_month = self.triggers(tdate)

        try:
            self.run_date_daily(tdate)
        except:
            logger.warning(f"Issue in the daily data scraping!")      
        try:
            self.run_date_weekly(tdate, trig_week)
        except:
            logger.warning(f"Issue in the weekly data scraping!")        
        try:
            self.run_date_monthly(tdate, trig_month)
        except:
            logger.warning(f"(X) Issue in the monthly data scraping!")
        logger.warning(f"---------- run_date over for {tdate.date()}")

    def run_date_daily(self, tdate: datetime):
        """Runs everyday independently of the date.
        Collects spots and futures prices and stores them in:: 

                self.df_daily_raw : DataFrame

            Called by
            ---------
            - `run_date(tdate)`

            Parameters
            ----------
            - `tdate`: 
                day of data collection

            Calls
            -----
            - `format_daily()`
        """
        payload = {
            "frequency": "daily",
            "data[]": "value",
            "start": (tdate - timedelta(days=7)).strftime("%Y-%m-%d"),
            "api_key": self.api_key,
            "length": 100000}

        df = pd.DataFrame(requests.get(self.url_daily, params=payload, verify=SSL_CERTIFICATE_PATH).json()['response']['data'])

        self.df_daily_raw = df
        self.format_daily()
        logger.warning(f"(V) Daily scraping done for {tdate.date()}!")
    
    def format_daily(self):
        """Takes the daily raw data collected from the source and gives it a DW format saved in::

                self.df_daily : DataFrame

            Called by
            ---------
            - `run_date_daily(tdate)`
        """
        df = self.df_daily_raw
        df = df[df['value'] != 0]
        df["Flow 3"] = df["process"].map(self.dict_maturities)        
        df['Product'] = 'Gaseous natural gas'
        df['Country'] = 'USA'
        df['Type'] = 'Observed'
        df['Source'] = 'U.S. Energy Information Administration - eia'
        df['Flow 1'] = 'Henry Hub'
        df['Metric'] = 'Prices'
        df['Flow 4'] = 'USD'
        df['units'] = '$/MMBTU'
        df = df.dropna().rename(columns={'period': 'local_date',
                                        'process-name': 'Flow 2',
                                        'units': 'Unit',
                                        'value': 'Value'}).drop(columns=['duoarea', 
                                                                        'area-name', 
                                                                        'product', 
                                                                        'process', 
                                                                        'series-description',
                                                                        'product-name', 
                                                                        'series'])

        self.df_daily = pd.concat([df, self.df_daily])
        self.df_daily['data_frequency'] = 'daily'
        self.df_daily['source_frequency'] = 'daily'
        
    def run_date_weekly(self, tdate: datetime, trigger: bool):     
        """Runs only on Fridays and Saturdays.
        Collects the raw information and saves it raw in several DataFrames::
                
                self.df_weekly_prices_raw : DataFrame
                self.df_weekly_supply_raw : DataFrame
                self.df_weekly_demand_raw : DataFrame

        Parameters
        ----------
        - `tdate`: 
            day of data collection
        - `trigger`:
            the scraper only runs if True      

        Calls
        -----
        - `extract_weekly_data()`: extracts the relevant information from the raw data
        - `format_weekly()`: formats the extracted data to meet the DW's constraints
        """
        if trigger:
            url = self.find_weekly_url(tdate)
            tables = pd.read_html(url)

            # weekly prices
            self.df_weekly_prices_raw = tables[0] 
            self.df_weekly_supply_raw = tables[1] 
            self.df_weekly_demand_raw = tables[2]
            #note: HDD and CDD remain available in tables[3] for future collection

            self.extract_weekly_data()
            self.format_weekly()
            logger.warning(f"(V) Weekly scraping done for {tdate.date()}!")
        else:
            logger.warning(f"(X) Weekly scraping was not triggered on {tdate.date()}!")

    def extract_weekly_data(self):
        """
        Extracts the relevant information from the raw data and saves it in lists::

                self.list_data_prices : list
                self.list_data_supply : list
                self.list_data_trade : list
                self.list_data_demand : list
        
        Called by
        ---------
        - `run_date_weekly(tdate)`
        """
        #spot and futures prices
        if not self.df_weekly_prices_raw.empty:
            df_prices = self.df_weekly_prices_raw
            self.df_weekly_prices_raw = pd.DataFrame()

            # TODO: Careful, this looks incorrect we'd rather use dynamically the name of the titles
            new_columns = ["Flow 1",
                            self.reference + timedelta(days=-7),    
                            self.reference + timedelta(days=-6),    
                            self.reference + timedelta(days=-3),    
                            self.reference + timedelta(days=-2),    
                            self.reference + timedelta(days=-1)]
            new_columns = ['Flow 1'] + [datetime.strptime(col, "%W, %-d-%m/Futures($/MMBtu")
                                        for col in df_prices.columns[1:]] #something like that: shorter and more precise ;)

            df_prices.columns = new_columns
            df_prices['Flow 2'] = 'Average Natural Gas Spot Price'
            df_prices['Flow 2'][4:6] = 'Average Natural Gas Futures Price'
            df_prices['Flow 2'][3] = "Avg. of NGI's reported prices for: Malin, PG&E Citygate, and Southern California Border Avg."
            df_prices['Flow 3'] = 'Spot'
            df_prices['Flow 3'][4] = '1M'
            df_prices['Flow 3'][5] = '2M'
            df_prices = pd.melt(df_prices[0:6],
                                id_vars=["Flow 1", "Flow 2", "Flow 3"],
                                var_name="local_date",
                                value_name="Value")
            df_prices = df_prices[df_prices['Value'] != 'Holiday']
            df_prices = df_prices[df_prices['Value'] != 'Expired']
            df_prices['Flow 4'] = 'USD'
            df_prices['Metric'] = 'Prices'
            df_prices['Unit'] = '$/MMBtu'
            df_prices['Source'] = 'Natural Gas Intelligence and CME Group as compiled by Bloomberg, L.P.'
            df_prices['Product'] = 'Total natural gas'
            
            self.list_data_prices += df_prices.values.tolist()

        #production
        if not self.df_weekly_supply_raw.empty:
            df_supply = self.df_weekly_supply_raw
            self.df_weekly_supply_raw = pd.DataFrame()
            # TODO: put directly in a df to avoid many variables (pd.DataFrame(...))
            # * Get rid of first 2 lines of DF,
            # * then convert column names to make them dates
            # * melt
            # * rename where needed
            self.list_data_supply += [{'Flow 3': 'Marketed production',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_supply.iloc[2][1]},
                                        {'Flow 3': 'Marketed production',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_supply.iloc[2][2]},
                                        {'Flow 3': 'Marketed production',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_supply.iloc[2][3]}]

            #imports
            self.list_data_trade += [{'Flow 1': 'Entry',
                                        'Flow 3': 'CAN',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_supply.iloc[4][1],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'CAN',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_supply.iloc[4][2],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'CAN',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_supply.iloc[4][3],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_supply.iloc[5][1],
                                        'Product': 'Liquefied natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_supply.iloc[5][2],
                                        'Product': 'Liquefied natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_supply.iloc[5][3],
                                        'Product': 'Liquefied natural gas'}]

        #sectorial consumption
        if not self.df_weekly_demand_raw.empty:
            df_demand = self.df_weekly_demand_raw

            self. list_data_demand += [{'Flow 1': 'Power and heat generation',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[3][1]},
                                        {'Flow 1': 'Power and heat generation',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[3][2]},
                                        {'Flow 1': 'Power and heat generation',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[3][3]},
                                        {'Flow 1': 'Industry',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[4][1]},
                                        {'Flow 1': 'Industry',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[4][2]},
                                        {'Flow 1': 'Industry',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[4][3]},
                                        {'Flow 1': 'Residential and commercial',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[5][1]},
                                        {'Flow 1': 'Residential and commercial',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[5][2]},
                                        {'Flow 1': 'Residential and commercial',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[5][3]},
                                        {'Flow 1': 'Transport (including pipeline)',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[7][1]},
                                        {'Flow 1': 'Transport (including pipeline)',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[7][2]},
                                        {'Flow 1': 'Transport (including pipeline)',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[7][3]}]

            #exports
            self.list_data_trade += [{'Flow 1': 'Exit',
                                        'Flow 3': 'MEX',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[6][1],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'MEX',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[6][2],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'MEX',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[6][3],
                                        'Product': 'Pipeline natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=7),
                                        'Value': df_demand.iloc[8][1],
                                        'Product': 'Liquefied natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=14),
                                        'Value': df_demand.iloc[8][2],
                                        'Product': 'Liquefied natural gas'},
                                        {'Flow 1': 'Entry',
                                        'Flow 3': 'LNG Partner',
                                        'local_date': self.reference - timedelta(days=7) - relativedelta(years=1),
                                        'Value': df_demand.iloc[8][3],
                                        'Product': 'Liquefied natural gas'}]            

    def format_weekly(self):
        """
        Takes data contained in the lists and converts into DataFrames.
        Formats to meet the proper DW shape and saves in::

            self.df_weekly : DataFrame

        Called by
        ---------
        - `run_date_weekly(tdate)`
        """
        df_prices = pd.DataFrame(self.list_data_prices)
        df_prices.columns = ['Flow 1', 'Flow 2', 'Flow 3', 'local_date', 'Value', 'Flow 4', 'Metric', 'Unit', 'Source', 'Product']
        df_prices['data_frequency'] = 'daily'
        df_prices['source_frequency'] = 'weekly'

        df_supply = pd.DataFrame(self.list_data_supply)
        df_supply['Metric'] = 'Production'
        df_supply['Unit'] = 'BCF'
        df_supply['Source'] = 'PointLogic'
        df_supply['Product'] = 'Total natural gas'
        df_supply['data_frequency'] = 'weekly'
        df_supply['source_frequency'] = 'weekly'

        df_demand = pd.DataFrame(self.list_data_demand)
        df_demand['Metric'] = 'Demand'
        df_demand['Unit'] = 'BCF'
        df_demand['Source'] = 'PointLogic'
        df_demand['Product'] = 'Total natural gas'
        df_demand['data_frequency'] = 'weekly'
        df_demand['source_frequency'] = 'weekly'

        df_trade = pd.DataFrame(self.list_data_trade)
        df_trade['Unit'] = 'BCF'
        df_trade['Source'] = 'PointLogic'
        df_trade['Metric'] = 'Trade'
        df_trade['data_frequency'] = 'weekly'
        df_trade['source_frequency'] = 'weekly'

        self.df_weekly = pd.concat([self.df_weekly,
                                    df_prices,
                                    df_supply,
                                    df_trade,
                                    df_demand])

        self.df_weekly['Country'] = 'USA'
        self.df_weekly['Type'] = 'Observed'

    def find_weekly_url(self, tdate: datetime):
        """
        Points at the correct Thursday (current week of the one of the week before).

        Called by
        ---------
        - `run_date_weekly()`

        Parameters
        ----------
        - `tdate`: 
            day of data collection

        Returns
        -------
        - `str`: 
            the completed url with the base and extension.
        """
        delta = (tdate.weekday() + 4) % 7
        self.reference = tdate - timedelta(days=delta)
        return self.url_weekly + self.reference.strftime("%Y/%m_%d/")

    def run_date_monthly(self, tdate: datetime, trigger: bool):
        """Runs only between the 26th and the end of the month.
        Collects the raw information starting in January the year before to date, and saves it raw in::
                
                self.df_monthly_raw : DataFrame

        Called by
        ---------
        - `run_date(tdate)`

        Parameters
        ----------
        - `tdate`: 
            day of data collection
        - `trigger`:
            the scraper only runs if True

        Calls
        -----
        - `format_monthly()`: formats the extracted data to meet the DW's constraints
        """
        if trigger:
        
            payload = {"frequency": "monthly",
                    "data[]": "value",
                    "start": str(tdate.year - 1) + "-01",
                    "api_key": self.api_key,
                    "length": 100000}

            df = pd.DataFrame(requests.get(self.url_monthly, 
                                            params=payload, 
                                            verify=SSL_CERTIFICATE_PATH).json()['response']['data'])

            self.df_monthly_raw = df
            self.format_monthly()
            self.get_monthly_lng(tdate)
            logger.warning(f"(V) Monthly scraping done for {tdate.date()}!")
        else:
            logger.warning(f"(X) Monthly scraping was not triggered on {tdate.date()}!")

    def format_monthly(self):
        """
        Accesses the monthly data stored and formats it to fit the DW, saves in::

            self.df_monthly: DataFrame

        Called by
        ---------
        - `run_date_monthly(tdate)`
        """
        df = self.df_monthly_raw
        self.df_monthly_raw = pd.DataFrame()
        df["Region"] = df["duoarea"].map(self.dict_states_3)
        df["Metric"] = df["process"].map(self.dict_process_to_metric)
        df = df[df["Metric"] != 'ignore']
        df['Product'] = 'Gaseous natural gas' #update into LNG where relevant
        df['Country'] =  'USA'
        df['Type'] =  'Observed'
        df['Source'] =  'U.S. Energy Information Administration - eia'
        df = df.rename(columns={'period': 'local_date',
                                'process-name': 'Flow 1',
                                'units': 'Unit',
                                'value': 'Value'})
        df = df[['local_date', 'Region', 'Metric', 'Product', 'Flow 1', 'Unit', 'Country', 'Source', 'Type', 'Value']]
        df.replace({'MMCF': 'BCF', '$/MCF': '$/MMCF'}, inplace=True)
        df['Value'] = df['Value'] / 1000
        df = df[df['Value'] != 0]
        df = df[df['Value'].notna()]
        df_demand = df[df['Metric'] == 'Demand'] #already formatted
        df_demand['Flow 1'] = df_demand['Flow 1'].map(self.sectors)
        df_demand= df_demand[df_demand['Flow 1'] != 'IGNORE']

        df_production = df[df['Metric'] == 'Production'] #filter Flow 1 on Marketed production
        df_prices = df[df['Metric'] == 'Prices']#F1: hub, F2: underlying, F3: mat, F4: ccy

        df_production = df_production[df_production['Flow 1'] == 'Marketed Production'].drop(columns=['Flow 1'])

        df_prices['Flow 2'] = df_prices['Flow 1']
        df_prices['Flow 1'] = df_prices['Region']
        df_prices['Flow 3'] = 'Spot'
        df_prices['Flow 4'] = 'USD'

        self.df_monthly = pd.concat([self.df_monthly,
                                    df_demand,
                                    df_production,
                                    df_prices])
        self.df_monthly['data_frequency'] = 'monthly'
        self.df_monthly['source_frequency'] = 'monthly'

    def get_url_monthly_lng(self, tdate):
        """
        Provides a dynimix url that depends on the tdate::

        Called by
        ---------
        - `run_date_monthly(tdate)`

        Returnds
        ---------
        - `str`
        """
        tdate = tdate - timedelta(days=31)
        month_url = tdate.month - 3
        next_year = (month_url + 3) // 12
        year_url = tdate.year + next_year
        return self.base_url_lng + str(year_url) + tdate.strftime("-%m") + self.end_url_lng + (tdate - timedelta(days=92)).strftime("%B") + '%20' + str(year_url) + '.xlsx'

    def get_monthly_lng(self, tdate):
        """
        Gets the latest monthly spreadsheet with daily LNG flows and formats it before saving in::

            self.df_lng: DataFrame

        Called by
        ---------
        - `run_date_monthly(tdate)`

        Calls
        ---------
        - `get_url_monthly_lng(tdate)`
        """
        url = self.get_url_monthly_lng(tdate)

        #Exports
        df_lng_exports = pd.read_excel(url, sheet_name='LNG Exports - Repository', skiprows=9)
        df_lng_exports = df_lng_exports.dropna(thresh=5)
        df_lng_exports = df_lng_exports[list([key for key in self.exports_columns.keys()])]
        df_lng_exports.rename(columns = self.exports_columns, inplace=True)
        df_lng_exports['Region'] = df_lng_exports['Flow 2'].apply(lambda x: x.split(',')[1])
        df_lng_exports['Flow 2'] = df_lng_exports['Flow 2'].apply(lambda x: x.split(',')[0])
        df_lng_exports['Flow 1'] = 'Exit'

        #Imports
        df_lng_imports = pd.read_excel(url, sheet_name='LNG Imports', skiprows=9)
        df_lng_imports = df_lng_imports.dropna(thresh=5).reset_index(drop=True)
        line = df_lng_imports['Date of Arrival'].str.find('Date of Arrival').dropna()
        df_lng_imports = df_lng_imports[list([key for key in self.imports_columns.keys()])]

        #the spreadsheet contains two tables, we do the split, allocate the sales basis and concatenate:
        df_lng_imports_st = df_lng_imports.iloc[0:line.index[0],:]
        df_lng_imports_st['Flow 5'] = 'Short-Term'
        df_lng_imports_st['Region'] = 'Massachusetts'

        df_lng_imports_lt = df_lng_imports.iloc[line.index[0] +1 : -1,:]
        df_lng_imports_lt['Flow 5'] = 'Long-Term'
        df_lng_imports_lt['Region'] = df_lng_imports_lt['Receiving Terminal'].apply(lambda x: x.split(',')[1])
        df_lng_imports_lt['Receiving Terminal'] = df_lng_imports_lt['Receiving Terminal'].apply(lambda x: x.split(',')[0])
        
        df_lng_imports= pd.concat([df_lng_imports_st, df_lng_imports_lt]).reset_index(drop=True)

        df_lng_imports.rename(columns = self.imports_columns, inplace=True)
        df_lng_imports['Flow 1'] = 'Entry'

        df_lng = pd.concat([df_lng_exports, df_lng_imports])

        df_lng['Unit'] = 'MMCF'
        df_lng['local_date'] = df_lng['local_date'].apply(lambda x : x.date())
        df_lng['Metric'] = 'Trade'
        df_lng['Product'] = 'Liquefied natural gas'
        df_lng['Country'] = 'USA'
        df_lng['Source'] = 'U.S. Department of Energy'
        df_lng['Type'] = 'Observed'
        # for some reason, pycountry doesn't do its work for two specific countries:
        df_lng['Flow 3'] = df_lng['Flow 3'].str.replace('South Korea', 'Korea, Republic of')
        df_lng['Flow 3'] = df_lng['Flow 3'].apply(lambda x: get_country_iso3(x))
        df_lng['Flow 3'] = df_lng['Flow 3'].str.replace('Trinidad', 'TTO')

        df_lng['Value'] = df_lng['Value'] / 1000
        df_lng = df_lng[df_lng['Value'] != 0]
        df_lng = df_lng[df_lng['Value'].notna()]

              
        self.df_lng = df_lng
        self.df_lng['data_frequency'] = 'daily'
        self.df_lng['source_frequency'] = 'monthly'  
        logger.warning(f"(V) Monthly LNG scraping done for {tdate.date()}!")

if __name__ == '__main__':
    folder = Path().resolve().parent.parent.parent / 'csvs'
    scraper = UsGasStatsJob()
    scraper.test_run(folder)
