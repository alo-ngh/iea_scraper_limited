from abc import abstractmethod
import logging
import sys
import pandas as pd
from datetime import datetime

from iea_scraper.core.exceptions import EdcJobError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import EDC_DAILY_JAPAN_JOBS
from iea_scraper.core import factory
import numpy as np


class EdcJapanJob(EdcBulkJob):
    """
    This classes ensures historical data is extracted together with latest available data for Japanese scrapers.
    """
    title: str = "EdcJapanJob class (on child class, define class variable title for metadata)."

    def __init__(self):
        """"
        Constructor.
        @param full_load: True for full-load.
                          False signals the scraper to load only most recent data.
        @param **kwargs: forward following parameters to super()
        """

        EdcBulkJob.__init__(self)
        self.metric = None
        self.region = None
        self.source = None
        self.output_df = pd.DataFrame()
        self.conversion_mw = 10
        self.year_df_generation = {year: None for year in range(2016, datetime.now().year + 1)}
        self.year_df_demand = {year: None for year in range(2016, datetime.now().year + 1)}
        self.fuel_mapping = {}
        self.df_data = {'Demand': pd.DataFrame(),
                        'Generation': pd.DataFrame()}

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = self.output_df
        return df_dw

    @property
    def day_lags(self):
        """
        type : list
        Enables to bypass the 3 weeks logic to scrape the
        last 3 months
        """
        return list(range(90))

    @abstractmethod
    def get_data(self, tdate):
        """
        This method extract from url and stores it in a DataFrame depending on parameter date
        :param date: datetime
        :return: DataFrame
        """
        pass

    @abstractmethod
    def format_df(self, tdate):
        pass

    def format_generation_for_all_scrapers(self, df):
        """
        format data for all generation scrapers
        @param df: DataFrame
        @return: DataFrame
        """
        df = pd.melt(df, id_vars=['local_datetime', 'local_date'], value_name='Value', var_name='Product')
        df['Product'] = df['Product'].map(self.fuel_mapping)
        df['Value'] = df['Value'].replace(regex=[',', '－'], value=0)
        df['Value'] = df['Value'].apply(lambda x: float(x))
        df = df.groupby(['local_datetime', 'local_date', 'Product'])['Value'].sum().reset_index()
        for product in set(df['Product']):
            if all(value == 0 for value in df.loc[df['Product'] == product, 'Value']):
                df = df.loc[df['Product'] != product]
        df = df[['local_datetime', 'local_date', 'Value', 'Product']]
        return df

    def format_demand_for_all_scrapers(self, df):
        """
        format data for all demand scrapers
        @param df: DataFrame
        @return: DataFrame
        """
        df = df[['local_datetime', 'local_date', 'Value']].copy()
        df = df.loc[(df['Value'] != 0) & (~df['Value'].isna())]
        df['Value'] = df['Value'].apply(lambda x: float(x) * self.conversion_mw)
        df['Product'] = 'ELE'
        return df

    def format_all_df(self):
        """
        formats data for all srapers
        @return:
        """
        df = self.output_df
        df['Country'] = 'Japan'
        df['Source'] = self.source
        df['Region'] = self.region
        df['Metric'] = self.metric
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize('Asia/Tokyo').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        self.output_df = df

    def check_japan_attributes(self):
        """
        checks specific characteristics of each scraper.
         if metric, region  or source then return EdcJobError
        """
        for value in set(self.df_dw['Metric']):
            if value not in ['Demand', 'Generation']:
                raise EdcJobError("Metric is mandatory and must be in ['Demand','Generation']")
        for value in set(self.df_dw['Region']):
            if value is None:
                raise EdcJobError("Region cannot be None, region must be in [Chubu, Chugoku, Hokkaido, Kansai, Kyushu,"
                                  "Okinawa, Hokuriku, Tokyo, Tokohu, Shikoku]")
        for value in set(self.df_dw['Source']):
            if value is None:
                raise EdcJobError("Source cannot be None, please check Source")

    def find_first_row_of_data(self, df):
        """
        loops over csv to find header row and filter output dataframe with relevant data
        @return: dataframe
        """
        first_row_found = False
        i = 0
        while not first_row_found and i < len(df):
            cell_value = df.iloc[i][0]
            if 'DATE' in cell_value:
                first_row_found = True
                df.columns = df.iloc[i]
                df = df.iloc[i + 1:i + 25]
            else:
                i += 1
        return df

    def run_date(self, tdate):
        self.get_data(tdate)
        self.format_df(tdate)
        if not self.output_df.empty:
            self.format_all_df()
            self.check_japan_attributes()

    def pre_run(self, historical=True):
        EdcBulkJob.pre_run(self, historical=historical, max_errors=60)


class AllJapanStatsJob(EdcBulkJob):
    title: str = 'All Japan Power Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.japan_jobs = {job_name: factory.get_scraper_job(**job_params)
                           for job_name, job_params in 
                           EDC_DAILY_JAPAN_JOBS.items()}
                
    @property
    def df_dw(self):
        return pd.concat([job.df_dw for job in self.japan_jobs.values()])
    
    def run_date(self, tdate):
        for name in self.japan_jobs:
            try:
                self.japan_jobs[name].run_date(tdate)
            except:
                logger.warning(f'{name} did not work for {tdate.date()}')
            
    def plot_metric(self, metric, by_region=False):
        x_axis = 'local_datetime'
        df_plot = self.df_dw_processed.loc[self.df_dw_processed['Metric'] == metric].copy()
        df_plot = df_plot.pivot_table(index=[x_axis], values='Value', columns='Source',
                                      aggfunc=np.sum).reset_index()
        df_plot.plot.line(x=x_axis, title=metric)

    @property
    def day_lags(self):
        return list(range(90))

    @property
    def offset_now(self):
        return 1


if __name__ == '__main__':
    all_jp = AllJapanStatsJob()
    all_jp.test_run(historical=True)
