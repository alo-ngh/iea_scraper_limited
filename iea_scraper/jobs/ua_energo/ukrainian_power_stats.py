"""
Created on Mon Oct 18, 2021
``
@author: CHAMBEAU_L

Ukrainian power data:
- Power Generation per fuel source
- Power Demand

"""
import requests
import pandas as pd
from datetime import timedelta
import logging
import sys
from datetime import datetime

from iea_scraper.core.exceptions import EdcJobError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.core import factory
from iea_scraper.settings import SSL_CERTIFICATE_PATH, REQUESTS_HEADERS


class UkrainianPowerStatsJob(EdcBulkJob):
    title: str = 'UKRENERGO - Ukraine Power Generation/Consumption statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.url = 'https://ua.energy/wp-admin/admin-ajax.php'
        self.dict_renaming = {
            'aes': 'Nuclear',
            'tec': 'Natural Gas',
            'tes': 'Coal',
            'vde': 'Other Renewables',
            'biomass': 'Biomass',
            'gesgaes': 'Hydro',
            'solar': 'Solar',
            'wind': 'Wind',
            'oil': 'Oil',
            'geothermal': 'Geothermal',
            'consumptiongaespump': 'Hydro Pumped Storage',
            'consumption': 'ELE'
        }
        self.output_df = pd.DataFrame()

    @property
    def offset_now(self):
        """
        Description:
            Latest available data is the day before today
        """
        return 1

    @property
    def df_dw(self):
        """
        Called by run_date()

        Input(s):
            None

        Output(s):
            df_dw [DataFrame]: DataFrame to be loaded in the DW

        Description:
            Updates columns with relevant information for generation and demand
        """
        return self.output_df

    def run_date(self, tdate):
        """
        Called by EdcBulkJob.pre_run()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            df_gen [DataFrame]: Dataframe with generation data.
            df_cons [DataFrame]: Dataframe with consumption data.

        Description:
            - Executes the job for one day without all the primary checks and without loading into db on the current date
            - Requests the data for both generation and demand at the same time
            - Parses the data in json and formats the appropriate DataFrames

        Calls:
            request_data()
            transform_to_df()
        """

        data_json = self.request_data(tdate)
        if len(data_json):
            self.transform_to_df(tdate, data_json)
            logger.info(f'UKREnergo: data collected for {tdate.date()}')
        else:
            logger.warning(f'UKR ENERGO: no data available on {tdate.date()}')

    def request_data(self, tdate):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected

        Output(s):
            data_json [Dict]: Dictionary with data collected.

        Description:
            - Sends a query to the website with relevant arguments (date etc)
            - Requests the data for both generation and demand at the same time
        """

        self.postdata = {
            'action': 'get_data_oes',
            'report_date': tdate.strftime("%d.%m.%Y"),
            'type': 'day'
        }

        response = requests.post(
            self.url,
            self.postdata,
            headers=REQUESTS_HEADERS,
            verify=SSL_CERTIFICATE_PATH)
        if response.status_code != 200:
            raise EdcJobError(f'UKR ENERGO: request data not available on {tdate.date()}')
        else:
            data_json = response.json()
            return data_json

    def transform_to_df(self, tdate, data_json):
        """
        Called by run_date()

        Input(s):
            tdate [datetime]: date to be collected
            data_json [Dict]: dictionary with data collected from the request

        Output(s):
            df_gen [DataFrame]: Dataframe with generation data.
            df_cons [DataFrame] Dataframe with consumption data.

        Description:
            - Reads the dictionary for hour per hour to build a dictionary for the data of the day
            - Creates two distinc dataframes for "&dh metric
            - Formats the dataframes
        """

        df_data = pd.DataFrame(data_json).melt(
            id_vars=['hour'],
            var_name='Product',
            value_name='Value'
        )
        df_data = df_data.loc[df_data['hour'] != 24].copy()
        df_data['hour'] = df_data['hour'].apply(lambda x: int(x[:2]))
        df_data['local_datetime'] = df_data['hour'].apply(lambda x: tdate.replace(hour=x))
        df_data = df_data.drop(columns='hour')
        df_data['Product'] = df_data['Product'].map(self.dict_renaming)
        df_data = df_data.loc[~df_data['Product'].isna()]
        df_data['Metric'] = df_data.apply(lambda x: "Demand" if x["Product"] == "ELE" else "Generation", axis=1)
        df_data['Country'] = 'Ukraine'
        df_data['Source'] = 'UKRENERGO'
        self.output_df = pd.concat([self.output_df, df_data])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    scraper_ukraine = UkrainianPowerStatsJob()
    scraper_ukraine.test_run(folder, historical=False)
    # scraper_ukraine = factory.get_scraper_job('ua_energo', 'ukrainian_power_stats')
