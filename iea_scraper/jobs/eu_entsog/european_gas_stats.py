"""
European Gas Trade Flows, from ENTSO-G Transparency Platform
- data is in MWh/d
- queries the data per specific type of flow (we selected a list of relevant ones)
    -> this should be changed to ultimately check for the relevant list dynamically
- Note: for now Partner Location (in the gas table) isn't used in the scraper.
    At some point we should have a mapping that connects:
        ENTSO-G Location <<<>>> Pair of Locations, so that to have both
        - Location AND - Partner Location
        So that to enable reconciling imports and exports when analysing the data.
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import pycountry
import sys
sys.path.append(r"C:\Repos\iea_scraper")
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from iea_scraper.core.job import EdcGasBulkJob
from iea_scraper.settings import SSL_CERTIFICATE_PATH


class EuropeanGasStatsJob(EdcGasBulkJob):
    title: str = 'Entsog.eu - European Gas Statistics'

    def __init__(self):

        super().__init__()
        self.url_data = 'https://transparency.entsog.eu/api/v1/operationaldata.csv'
        self.columns = [ #used to request only specific columns from Entso-g's outpout
            'periodFrom', 
            'operatorKey', 
            'operatorLabel', 
            'pointKey', 
            'pointLabel', 
            'directionKey',
            'unit',
            'value',
            'pointType',
            'idPointType']
        self.id_info = { #those are considered relevant, rest was either empty, or double-counting (or storage etc.)
            0: ['Cross-Border Transmission IP within EU',
                'Pipeline natural gas'],
            1: ['Cross-Border Transmission IP between EU and Non-EU (import)',
                'Pipeline natural gas'],
            2: ['LNG Entry point',
                'Liquefied natural gas'],
            7: ['Cross-Border Transmission IP Non-EU',
                'Pipeline natural gas'],
            14: ['Cross-Border Transmission IP between EU and Non-EU',
                'Pipeline natural gas']}
        self.data = pd.DataFrame()
        self.data_points_directions = pd.DataFrame()
        self.df_merged = pd.DataFrame()
        self.df_final_info = pd.DataFrame() #can be ignored, in case we need additional info

    @property
    def offset_now(self):
        return 1

    def run_date(self, tdate):
        """
        Called by: EdcGasBulkJob.pre_run() 

        Input:
            tdate [datetime]: date for which we want data

        Output(s):
            data [dataframe]: dataframe that contains the data queried

        Description:
            - retrieves info for all the types of flows per id of flow type
            - retrieves info for all the TSOs
            - merges the info to obtain a final DataFrame that connects value, location and country

        Calls:
            get_data_all()
            get_data_points_direction()
        """

        self.get_data_all(tdate)
        self.get_data_points_direction(tdate)

    @property
    def df_dw(self):
        """
        Called by: run_date()

        Output:
            df_dw [dataframe]: dataframe that contains the data formatted to fit the DW

        Description:
            - from the merged info of border points and TSOs, keeps only data to be saved
            - formats the data to pass the processing checks in core.job
        """

        df = self.data.merge(self.data_points_directions, how='left', 
                             on=['pointKey', 'operatorKey'])

        df = df.rename(columns= {
            'periodFrom': 'local_date',
            'directionKey': 'Flow 1',
            'unit': 'Unit',
            'value': 'Value',
            'adjacentCountry': 'Flow 3',
            'tpTsoCountry': 'Country',
            'operatorKey_x': 'operatorKey',
            'pointLabel_x': 'pointLabel'
        }).drop(columns=['pointLabel_y'])

        df['local_date'] = pd.to_datetime(df['local_date']).dt.date
        df['Flow 2'] = df['pointLabel'] + ' ' + df['pointKey'] #location is built upon the name and the id used in Entso-g
        df['Product'] = df['idPointType'].apply(lambda x: self.id_info[x][1])
        df = df.dropna(subset=['Country', 'Flow 3'])
        df['Country'] = df['Country'].replace('UK', 'GB').apply(lambda x: pycountry.countries.get(alpha_2=x))
        df['Country'] = df['Country'].apply(lambda x: x.alpha_3)
        df['Flow 3'] = df['Flow 3'].replace('UK', 'GB').apply(lambda x: pycountry.countries.get(alpha_2=x))
        df['Flow 3'] = df['Flow 3'].apply(lambda x: x.alpha_3)
        df['Source'] = 'ENTSO-G'
        df['Metric'] = 'Trade'
        df['Unit'] = 'GWh/d' #different from what Entso-g reports (either KWh/h or KW/d) but needed to pass our treshold check
        df['Type'] = 'Observed'

        mask_LNG = df['Product'] == 'Liquefied natural gas'
        df.loc[mask_LNG, 'Flow 3'] = 'LNG Partner'
        df['Flow 1'].replace('exit', 'Exit', inplace=True)
        df['Flow 1'].replace('entry', 'Entry', inplace=True)

        #there is still some valuable indormation, the line below is to potentially keep it
        #self.df_final_info = self.df_final_info.append(df)

        df = df[['local_date', 'Source', 'Metric', 'Unit', 'Product','Country',
                    'Flow 1', 'Flow 2', 'Flow 3', 'Value', 'Type']]
        return df


    def get_data_points_direction(self, tdate):
        """
        Called by: run_date()

        Input:
            tdate [datetime]: date for which we want data

        Output:
            data_points_directions [dataframe]: dataframe containing the data extracted

        Calls:
            get_url_points_direction()

        """

        data = pd.read_csv(self.get_url_points_direction(tdate))
        self.data_points_directions = data[[
            'pointKey',
            'pointLabel',
            'operatorKey',
            'tpTsoCountry', 
            'adjacentCountry']].drop_duplicates()
        
    def get_url_points_direction(self, tdate):
        """
        Called by: get_data_ponts_direction()

        Input:
            tdate [datetime]: date for which we want data
        
        Output:
            url_to_query [string]: url used in the request to query the data
        """

        self.get_points_direction_url = {
            'base': 'https://transparency.entsog.eu/api/v1/operatorpointdirections.csv?forceDownload=true&delimiter=comma&from=',
            'date_from': (tdate - timedelta(days=1)).strftime("%Y-%m-%d"),
            'to_str': '&to=',
            'date_to': tdate.strftime("%Y-%m-%d"),
            'end': r'&indicator=Physical%20Flow&periodType=hour&timezone=CET&periodize=1&limit=-1'
        }
        url_to_query = ''.join([self.get_points_direction_url[element] for element in self.get_points_direction_url])
        return url_to_query

    def get_url_data(self, tdate, id_type):
        """
        Called by: get_data_type()

        Input:
            tdate [datetime]: date for which we want data
            id_type [int]: number corresponding to the type of flows to be queried (from list of selected flows)
        
        Output:
            url_to_query [string]: url used in the request to query the data
        """

        self.get_data_url = {
            'base': 'https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&delimiter=comma&idPointType=',
            'id_number': str(id_type),
            'from_str': '&from=',
            'date_from': (tdate - timedelta(days=1)).strftime("%Y-%m-%d"),
            'to_str': '&to=',
            'date_to': tdate.strftime("%Y-%m-%d"),
            'end': r'&indicator=Physical%20Flow&periodType=day&timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1'
        }

        url_to_query = ''.join([self.get_data_url[element] for element in self.get_data_url])
        return url_to_query
        
    def get_data_all(self, tdate):
        """
        Called by: run_date()

        Input:
            tdate [datetime]: date for which we want data

        Output: 
            data [dataframe]: dataframe where all the data has been aggregated

        Description:
            - loops over all the types of flows that have been selected
            - queries the data for each type
            - appends all the outputs in one dataframe

        Calls:
            get_data_type()
        """

        for id_type in self.id_info.keys():
            data = self.get_data_type(tdate, id_type)
            self.data = pd.concat([self.data,data])
            logger.info(f'Data collected for {self.id_info[id_type][0]} on {tdate.date()}')
        pass

    def get_data_type(self, tdate, id_type):
        """
        Called by: get_data_all()

        Input(s):
            tdate [datetime]: date for which we want data
            id_type [int]: number corresponding to the type of flows to be queried (from list of selected flows)

        Output(s):
            data [dataframe]: dataframe that contains the data queried

        Description:
            - retrieves the url to query
            - queries the url
            - does a quick formatting 
            - returns the dataframe

        Calls:
            get_url_data()
        """

        r = requests.get(self.get_url_data(tdate, id_type), verify=SSL_CERTIFICATE_PATH)
        if r.status_code == 200:
            data = pd.read_csv(self.get_url_data(tdate, id_type),
                                index_col=False, 
                                usecols=[i for i in self.columns])
            data = data.loc[data['value'] != 0].dropna()
            data['value'] = data['value'] / (10 ** 6)
        else:
            data = pd.DataFrame()

        return data

if __name__ == '__main__':
    folder = r'C:\Repos\iea_scraper\iea_scraper\csvs'
    scraper = EuropeanGasStatsJob()

    scraper.test_run(folder, historical=False)

# %%
