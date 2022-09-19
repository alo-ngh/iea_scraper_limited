import collections
import locale
import logging
import os
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime, timedelta
from typing import NoReturn
from copy import copy

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.utils import batch_upload, parallelize, \
    calc_checksum_download, get_db_source_dict, timeit, timeout, get_country_iso3, load_config
from iea_scraper.settings import FILE_STORE_PATH, API_END_POINT, PROXY_DICT, \
    EDC_TIMEOUT, EXT_DB_STR, SSL_CERTIFICATE_PATH

MAX_WORKER = 15
BATCH_SIZE = 20000
BATCH_SIZE_DIM = 1000
logger = logging.getLogger(__name__)


class BaseJob(ABC):
    """
    Base class for all types of scraper jobs.
    This class does not impose or offer any implementation but requests
    children classes to implement the run() method.

    If the subclass overrides the constructor, it must make sure to invoke the base class constructor
    (BaseSource.__init__()) before doing anything else to the thread.
    """
    title: str = "BaseJob class (on child class, define class variable title for metadata)."

    def __init__(self, full_load=None, **kwargs):
        """
        Constructor.

        :param full_load: 

        - True for full-load.
        - False signals the scraper to load only most recent data.
        """
        super().__init__(**kwargs)
        self.full_load = full_load

    @abstractmethod
    def run(self, download=True):
        """
        Execute the job.

        - download: if False, bypass source files download.
        """


class EdcJob(BaseJob):
    """
    The EDC scraper job is meant to send data to the datawarehouse with an 
    enforced schema that fits EDC's guidelines
    """
    title: str = "EdcJob class (on child class, define class variable title for metadata)."

    def __init__(self, full_load=None, config='electricity', **kwargs):
        """
        Constructor.

        :param full_load: 

                - True for full-load.
                - False signals the scraper to load only most recent data.

        """
        super().__init__(**kwargs)
        locale.setlocale(locale.LC_ALL, '')
        self.driver = None
        self.full_load = full_load
        self.export_datetime = datetime.now()
        self._df_dw_processed = None
        conf = load_config(config)
        for key, val in conf.items():
            setattr(self, key, val)
            
        self._df_dw_processed = None
      
    @property
    @abstractmethod
    def df_dw(self):
        """Builds the dataframe that will be sent to the DW"""
        pass

    @property
    def df_dw_processed(self):
        """Final dataframe with modifications to make it fit for DW"""
        self._df_dw_processed = self.process_df_dw()
        return self._df_dw_processed

    def process_df_dw(self, max_limit=1000000, min_limit=-30000):
        """
        Performs some actions on df_dw:
            - Removes nulls
            - Removes values between min and max (1M for now)
            - Converts Value into float type
            - Convert date fields into dates
            - Adds local_date or utc_date when local_datetime or utc_datetime are there
            - Removes utc_datetime, local_datetime timezone info
            - Drops duplicates
            - Groups by everything but Value
            - Adds export date
            - Converts country name into ISO3

        :param max_limit: max limit for data to be rejected in check
        :param min_limit: min limit for data to be rejected in check
        """
        df_dw_p = self.df_dw.copy()
        if df_dw_p.empty:
            raise EdcJobError('df_dw is empty')
        row_number = len(df_dw_p)
        df_dw_p = df_dw_p[~df_dw_p['Value'].isnull()]
        number_null = row_number - len(df_dw_p[~df_dw_p['Value'].isnull()])        
        df_dw_p['Value'] = df_dw_p['Value'].astype(float)
        df_dw_p = df_dw_p[df_dw_p['Value'] < max_limit]
        df_dw_p = df_dw_p[df_dw_p['Value'] > min_limit]
        number_above = row_number - len(df_dw_p[df_dw_p['Value'] < max_limit])
        number_below = row_number - len(df_dw_p[df_dw_p['Value'] > min_limit])
        if number_null > 0:
            logger.warning(f'Input data has {number_null} rows with nulls in Value')
        if number_above > 0:
            logger.warning(f'Input data has {number_above} rows above {max_limit} and {number_below} rows below {min_limit} in Value')
            
        date_columns = [col for col in df_dw_p.columns if col in self.date_columns]
        for col in date_columns:
            try:
                df_dw_p[col] = pd.to_datetime(df_dw_p[col])
            except:
                raise EdcJobError(f"{[col]} has to be a date/datetime")
        if 'local_datetime' in df_dw_p.columns and not(df_dw_p.loc[~df_dw_p['local_datetime'].isnull()].empty):
            df_dw_p.loc[~df_dw_p['local_datetime'].isnull(),'local_date'] = df_dw_p.loc[
                ~df_dw_p['local_datetime'].isnull(),'local_datetime'].apply(lambda x: x.date())
            df_dw_p['local_datetime'] = df_dw_p['local_datetime'].apply(lambda x: x.replace(tzinfo=None))
        if 'utc_datetime' in df_dw_p.columns and not(df_dw_p.loc[~df_dw_p['utc_datetime'].isnull()].empty):
            df_dw_p.loc[~df_dw_p['utc_datetime'].isnull(),'utc_date'] = df_dw_p.loc[
                ~df_dw_p['utc_datetime'].isnull(),'utc_datetime'].apply(lambda x: x.date())
            df_dw_p['utc_datetime'] = df_dw_p['utc_datetime'].apply(lambda x: x.replace(tzinfo=None))
        df_dw_p = df_dw_p.drop_duplicates()
        attribute_cols = [col for col in df_dw_p.columns if col != 'Value']
        df_dw_p = df_dw_p.groupby(attribute_cols, dropna=False).sum().reset_index()
        df_dw_p['Export Date'] = self.export_datetime
        df_dw_p['Country'] = df_dw_p['Country'].apply(get_country_iso3)

        not_tolerated_countries = [col for col in set(df_dw_p['Country'])
                                   if col not in self.tolerated_countries]
        if not_tolerated_countries:
            raise EdcJobError(f"{str(not_tolerated_countries)} not tolerated as country. "
                              f"Countries must be ISO3")
        return df_dw_p

    @abstractmethod
    def pre_run(self):
        """
        Executes the job without all the primary checks and without
        loading into db
        """

    def run(self, db_str=EXT_DB_STR, download=True):
        """
        Execute the job.
        :param download: if False, bypass source files download.
        """
        self.pre_run()
        self.check_df_dw()
        if db_str is not None:
            self.to_sql(db_str)
        if self.driver is not None:
            self.driver.close()

    @timeit
    def test_run(self, folder=None, historical=None, plot=True):
        """
        historical : None -> EdcJob runs pre_run
                     True -> [Only available for EdcBulkJob] All dates set in day_lags will be scraped
                     False -> [Only available for EdcBulkJob] Only latest available 2 days will be scraped
                        
        """
        if historical is not None:
            self.pre_run(historical)
        else:
            self.pre_run()
        self.check_df_dw()
        if plot:
            self.plot_all()
        if folder is not None:
            self.to_csv(folder)
        else:
            _ = self.df_dw_processed

    def check_df_dw(self):
        """
        Checks include:
            - not empty
            - not tolerated columns
            - mandatory columns that were not used
            - not tolerated products
            - not tolerated countries
            - not tolerated flow 1 depending on metric
            - not tolerated flow 2 depending on metric
            - mandatory flows per metric
        """
        df_dw = self.df_dw
        if df_dw.empty:
            raise EdcJobError('Output DataFrame is empty')
        not_tolerated_columns = [col for col in df_dw.columns
                                 if col not in self.tolerated_columns]
        if not_tolerated_columns:
            raise EdcJobError(f"[{str(not_tolerated_columns)}] not tolerated as a column name(s). "
                              f"Accepted columns are [{str(self.tolerated_columns)}]")
        lost_mandatory_columns = [col for col in self.mandatory_columns
                                  if col not in df_dw.columns]
        if lost_mandatory_columns:
            raise EdcJobError(f"[{str(lost_mandatory_columns)}] is/are "
                              f"mandatory in df_dw")
        not_tolerated_metrics = [col for col in set(self.df_dw['Metric'])
                                 if col not in self.tolerated_metrics]
        if not_tolerated_metrics:
            raise EdcJobError(f"{str(not_tolerated_metrics)} not tolerated as metric(s). "
                              f"Accepted metrics are {str(self.tolerated_metrics)}")

        for metric in set(df_dw['Metric']):
            for flow in self.tolerated_flows:
                tolerated_flows = self.tolerated_flows[flow][metric]
                if flow in df_dw.columns:
                    df_metric = df_dw.loc[df_dw['Metric'] == metric]
                    not_tolerated_flows = list(df_metric.loc[~df_metric[flow].isin(tolerated_flows), flow].unique())
                    not_tolerated_flows = [flow for flow in not_tolerated_flows if str(flow) != 'nan']
                    if 'ALL' in tolerated_flows:
                        not_tolerated_flows = []
                    if not_tolerated_flows:
                        raise EdcJobError(f"{not_tolerated_flows} not tolerated as {flow}. "
                                          f"Accepted {flow} for metric {metric} are {tolerated_flows}")
                else:
                    if np.nan not in tolerated_flows:
                        raise EdcJobError(f"[{flow}] is mandatory in df_dw for {metric}")
        df_no_product = df_dw.loc[df_dw['Product'].isnull()]
        if not df_no_product.empty:
            raise EdcJobError(f"Product cannot be NULL")

        logging.info('Dataframe checked')

    def to_csv(self, folder):
        '''Sends df_dw in a csv file in folder'''
        file_name = os.path.join(folder, self.export_date.strftime(f'%Y_%m_%d_{self.name}.csv'))
        self.df_dw_processed.to_csv(file_name, index=False)

    def to_sql(self, db_str):
        '''Sends the data in the DW'''
        engine = create_engine(db_str, fast_executemany=True)
        self.df_dw_processed.to_sql(self.table_name,
                                    con=engine,
                                    schema='edc',
                                    if_exists='append',
                                    index=False)
        logging.info('Data succesfully added to DB')

    def plot_all(self):
        '''Plots all metrics in df_dw'''
        for metric in self.available_metrics:
            if self.metric_regions[metric] and metric != 'Generation':
                self.plot_metric(metric=metric, by_region=True)
            else:
                self.plot_metric(metric=metric, by_region=False)

    def plot_metric(self, metric, by_region=False):
        '''
        Plots df_dw with only the chosen metric
        metric: str 'Generation' or 'Demand' or 'Prices'
        by_region: boolean saying if we want to plot by region
        '''
        if metric not in self.available_metrics:
            raise EdcJobError(f'{metric} metric is not available. Available metrics are {self.available_metrics}')
        df_plot = self.df_dw_processed.loc[self.df_dw_processed['Metric'] == metric].copy()
        if self.metric_regions[metric] == []:
            by_region = False
        for date_col in self.used_date_columns:
            if not df_plot[date_col].isnull().all():
                x_axis = date_col
                break
        if metric == 'Generation':
            #remove negative values
            df_plot = df_plot.loc[df_plot['Value'] > 0]
            df_plot = df_plot.pivot_table(index=[x_axis], values='Value', columns='Product',
                                          aggfunc=np.sum).reset_index()
            df_plot.plot.area(x=x_axis, title=metric)
        elif metric == 'Trade':
            #remove negative values
            df_plot = df_plot.loc[df_plot['Value'] > 0]
            df_plot = df_plot.pivot_table(index=[x_axis], values='Value', columns='Product',
                                          aggfunc=np.sum).reset_index()
            df_plot.plot.line(x=x_axis, title=metric)
        elif metric in ('Prices', 'Total Generation', 'Demand'):
            if by_region:
                df_plot = df_plot.pivot_table(index=[x_axis], values='Value', columns='Region',
                                          aggfunc=np.sum).reset_index()
                df_plot.plot(x=x_axis, title=metric)
            else:
                df_plot = df_plot.groupby(x_axis)['Value'].sum().reset_index()
                df_plot.plot(x=x_axis, y='Value', title=metric)
                
    @property
    def name(self):
        '''Name of the scraper'''
        return self.__class__.__name__

    def __repr__(self):
        return self.name

    @property
    def job_type(self):
        '''Class it inherits from'''
        return self.__class__.__bases__[0].__name__

    @property
    def bulk(self):
        '''True if the job is a bulk job'''
        bulk_condition = "Bulk" in self.job_type
        return bulk_condition

    @property
    def export_date(self):
        '''Date of the extraction'''
        return datetime.combine(self.export_datetime, datetime.min.time())

    @property
    def available_metrics(self):
        if 'Metric' not in self.df_dw.columns:
            raise EdcJobError('Metric is not in df_dw columns')
        else:
            return set(self.df_dw['Metric'])

    @property
    def used_date_columns(self):
        used_date_columns = [col for col in self.date_columns if col in self.df_dw.columns]
        return used_date_columns

    @property
    def time_since_start(self):
        return datetime.now() - self.export_datetime
    
    @property
    def metric_regions(self):
        '''Dictionary metric -> list of regions'''
        if 'Region' in self.df_dw:
            metric_regions = {metric: list(self.df_dw.loc[(self.df_dw['Metric']==metric)&(~self.df_dw['Region'].isnull()), 'Region'].drop_duplicates())
                for metric in self.available_metrics}
        else:
            metric_regions = {metric:[] for metric in self.available_metrics}
        return metric_regions

    def __del__(self):
        '''Closes selenium before deleting the object'''
        if self.driver is not None:
            try:
                self.driver.close()
            except:
                pass


class EdcBulkJob(EdcJob):
    """
    This classes ensures historical data is extracted together with 
    latest available data.
    """
    title: str = "EdcJobBulk class (on child class, define class variable title for metadata)."
    def __init__(self, full_load: object = None, **kwargs: object) -> object:
        """
        Constructor.
        :param full_load: 
            
        - True for full-load.
        - False signals the scraper to load only most recent data.
        
        :param **kwargs: forward following parameters to super()
        """
        super().__init__(**kwargs)
        if not isinstance(self.offset_now, int):
            raise EdcJobError('offset_now must be an integer. Set as N if latest day available is D-N.')
        self.earliest_available_date = self.last_available_date

    @property
    @abstractmethod
    def offset_now(self):
        """
        integer counting the number of days from today to the latest available and complete data
        """

    @property
    def day_lags(self):
        """
        type : list
        Days to scrape before today:
        - D-n,..., D-n-6 where D-n is the latest day available
        - D-30,..., D-36
        - D-90,...,D-96
        """
        first_week = list(range(self.offset_now, self.offset_now + 7))
        first_month = list(range(30, 37))
        third_month = list(range(90, 97))
        return first_week + first_month + third_month

    @property
    def last_available_date(self):
        return self.export_date - timedelta(days=self.offset_now)

    @abstractmethod
    def run_date(self, tdate):
        """
        Executes the job for one day without all the primary checks and without
        loading into db on the current date
        """

    @timeout(EDC_TIMEOUT)
    def _run_date(self, tdate):
        """
        Executes run_date with decorators
        """
        self.run_date(tdate)

    def bulk_run(self, start_date=None, end_date=None, error_tolerance=7,
                 db_str=None):
        """
        Executes the job for:
            - All days between start_date and end_date if they are not None
            - Otherwise all days between from the latest available date to the earliest date
            
        @parameters
            start_date: None -> will get all days until the earliest available
            end_date: None -> will start with the latest available day
            error_tolerance -> Number of consecutive errors until it stops
        """
        start_date = datetime(1900, 1, 1) if start_date is None else start_date
        end_date = self.last_available_date if end_date is None or end_date > self.last_available_date else end_date
        errors = 0
        date_to_scrape = end_date
        while errors <= error_tolerance and date_to_scrape >= start_date:
            try:
                self._run_date(date_to_scrape)
                errors = 0
                self.earliest_available_date = date_to_scrape
            except:
                errors += 1
                logger.warning(f'{date_to_scrape} did not work')
            date_to_scrape -= timedelta(days=1)
        self.check_df_dw()
        if db_str is not None:
            self.to_sql(db_str)
        else:
            _ = self.df_dw_processed

    def bulk_run_by_batches(self, batch_days, start_date=None, end_date=None,
                            error_tolerance=7, db_str=None):
        """
        Same parameters as bulk_run except batch_days that gives the number of days 
        in each batch loaded into db
        """

        # not possible at the moment, missing reset df_dw
        pass

    def pre_run(self, historical=True, max_errors=21):
        """
        Executes the job for all required days without all the primary checks 
        and without loading into db on the current date
        :param historical: 
        
        - True -> all dates set in day_lags will be scraped
        - False -> only latest available 2 days will be scraped
        """
        if historical:
            count_errors = 0
            for day in self.day_lags:
                try:
                    self._run_date(self.export_date - timedelta(days=day))
                except Exception as e:
                    logger.warning(f'{self.export_date - timedelta(days=day)} could not be scraped')
                    logger.exception(e)
                    last_error = e
                    count_errors += 1
            logger.info(f'{count_errors} days could not be scraped')
            if count_errors >= max_errors:
                raise EdcJobError(f'{self.name} did not work. Last error {last_error}')
        else:
            self._run_date(self.export_date - timedelta(days=self.offset_now))
            self._run_date(self.export_date - timedelta(days=self.offset_now + 1))

    @timeit
    def test_run(self, folder=None, historical=False, **kwargs):
        EdcJob.test_run(self, folder, historical, **kwargs)

    def reset(self):
        for attribute in self.__dict__:
            setattr(self, attribute, None)
        self.__init__()

    def run_last_date(self):
        '''Runs the latest date available'''
        self.run_date(self.export_date - timedelta(days=self.offset_now))


#fuel classes
class EdcElectricityJob(EdcJob):
    def __init__(self, **kwargs):
        super().__init__(config='electricity', **kwargs)


class EdcElectricityBulkJob(EdcBulkJob):
    def __init__(self, **kwargs):
        super().__init__(config='electricity', **kwargs)


class EdcGasJob(EdcJob):
    def __init__(self, **kwargs):
        super().__init__(config='gas', **kwargs)


class EdcGasBulkJob(EdcBulkJob):
    def __init__(self, **kwargs):
        super().__init__(config='gas', **kwargs)


class ExtDbApiJob(BaseJob):
    """"
    Generic class for a scraper job for loading into IEA External DB through its API.
    """
    title: str = "ExtDbApiJob class (on child class, define class variable title for metadata)."

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dynamic_dim = collections.defaultdict(list)
        self.data = None
        self.sources = []
        self.source_complements = []

    def run(self, download=True, parallel_download=True):
        """
        Execute the job.
        :param download: if False, bypass source files download.
        :param parallel_download: if False, download source files sequentially.
        """
        self.get_sources()
        self.download_and_get_checksum(download, parallel_download)
        self.rm_sources_up_to_date()
        self.transform()
        self.insert_new_dynamic_dim()
        self.upsert()
        self.update_sources_metadata()

    @abstractmethod
    def get_sources(self):
        """Should create a list of mutable object source in self.sources with at least
        3 attributes: 'url', 'code', 'path'. You can setup a list of complementary 
        files for download in  self.source_complements"""

    @abstractmethod
    def transform(self):
        """
        Should create:
        *  In self.dynamic_dim_dfs a dictionary of the element of the dynamic
           dimensions to be inserted with the API (the key being the name of the dim)
           ex: {'source': [{'code': 'code1', 'url': 'url1', ...},
                           {'code': 'code2', ... }, ...],
                'entity': [{'code': 'code1', 'category': 'category1', ...},
                           {'code': 'code2', ... }, ...],
                ...}
        *  In self.data the data to be uploaded to upserted with the API
        """

    @timeit
    def download_and_get_checksum(self, download=True, parallel_download=True):
        """
        This function downloads all files listed in self.sources.

        :param download: Flag determining whether the file should be downloaded or not. Default is True.
        :param parallel_download: Flag determining whether download should occur in parallel. Default is True.
        :return NoReturn:
        """
        if download:
            logger.debug(f"download: {download}, parallel download: {parallel_download}")
            file_for_download = self.sources + self.source_complements
            if parallel_download:
                parallelize(self.download_source, file_for_download, MAX_WORKER)
            else:
                for f in file_for_download:
                    self.download_source(f)
        parallelize(calc_checksum_download, self.sources, MAX_WORKER)

    @timeit
    def rm_sources_up_to_date(self):
        for source in reversed(self.sources):
            logger.debug(f"rm_sources_up_to_date: processing {source.code}")
            try:
                db_source = get_db_source_dict(source.code)
                if 'checksum' in db_source and type(db_source['checksum']) == str \
                        and db_source['checksum'].strip() == source.checksum:
                    logger.debug(f"rm_sources_up_to_date: removing {source.code} from self.sources")
                    self.sources.remove(source)
            except ValueError as e:
                logger.debug(f"Source code {source.code} not found in dimension.source: it's a new source.")

    @timeit
    def insert_new_dynamic_dim(self):
        logger.debug(f"Running insert_new_dynamic_dim(): {len(self.dynamic_dim)} items")
        for dimension, data in self.dynamic_dim.items():
            logger.debug(f"Processing {dimension}: size {len(data)}")
            if len(data) > 0:
                logger.debug(f"{dimension}: loading {len(data)} rows")
                endpoint = f"{API_END_POINT}/dimension/{dimension}"
                batch_upload(data, endpoint, BATCH_SIZE_DIM)
        return None

    @timeit
    def upsert(self):
        endpoint = f"{API_END_POINT}/main/datapoint"
        if self.data is not None:
            batch_upload(self.data, endpoint, BATCH_SIZE)
        return None

    @timeit
    def update_sources_metadata(self):
        endpoint = f"{API_END_POINT}/dimension/source"
        r = None
        for source in self.sources:
            _id = get_db_source_dict(source.code)['id']
            data = {}
            data["last_update"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            data["checksum"] = source.checksum
            try:
                data["last_download"] = source.last_download
            except Exception as e:
                logger.warning(f"Attribute source.last_download not available. Running with download=False?")

            r = requests.put(f"{endpoint}/{_id}", json=data)
        return r

    def get_source_from_code(self, code):
        source = [s for s in self.sources if s.code == code]
        try:
            return source[0]
        except IndexError:
            return None

    @staticmethod
    def download_source(source, http_headers=None):
        """
        Download one given file.
        :param source: BaseSource object describing the file to download.
        :param http_headers: optional headers to pass in the request. Default is None.
        Defined as a static method to allow overloading
        """
        try:
            path, url = source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        r = requests.get(url,
                         proxies=PROXY_DICT,
                         headers=http_headers,
                         verify=SSL_CERTIFICATE_PATH)

        r.raise_for_status()

        file_path = FILE_STORE_PATH / path
        file_path.write_bytes(r.content)
        logger.debug(f'{len(r.content)} bytes written to {file_path.name}.')

        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def remove_existing_dynamic_dim(self, dimension, filters=None):
        """
        Remove items from self.dynamic_dim already existing in the database.
        :param dimension: the dimension name.
        :param filters: a dictionary containing the filter for the
                        get query.
        :return: None
        """
        query = f"{API_END_POINT}/dimension/{dimension}"
        if filters is not None:
            query += '?'
            for k, v in filters.items():
                query += f"{k}={v}&"
            query = query[:-1]

        logger.debug(f'remove_existing_dynamic_dim: query - {query}')
        r = requests.get(query)
        data = r.json()
        codes = [x['code'] for x in data]
        logger.debug(f"self.dynamic_dim['{dimension}'] size before: {len(self.dynamic_dim[dimension])}")
        self.dynamic_dim[dimension] = [x for x in self.dynamic_dim[dimension]
                                       if x['code'] not in codes]
        logger.debug(f"self.dynamic_dim['{dimension}'] size after: {len(self.dynamic_dim[dimension])}")


class ExtDbApiJobV2(BaseJob):
    """"
    Generic class for a scraper job for loading into IEA External DB through its API.
    This new version brings the following improvements:
    - enforce declaration of provider properties (code, long_name, url)
    - automatically transforms the provider (add it to self.dynamic_dim['provider'])
    - automatically loads sources as dynamic dimension (add them to self.dynamic_dim['source'])
    Scrapers derived from ExtDbApiJob can be progressively migrated to V2 with no big-bang.
    """

    @property
    @abstractmethod
    def title(self):
        """
        title should contain description of the scraper that will appear on the daily load report.
        This declaration enforces that subclasses define title, but python will not check the type is str.
        :return: NoReturn
        """
        pass

    @property
    @abstractmethod
    def provider_code(self):
        """
        provider_code should contain the provider code.
        This declaration enforces that subclasses define provider_code, but python will not check the type is str.
        :return: NoReturn
        """
        pass

    @property
    @abstractmethod
    def provider_long_name(self):
        """
        provider_long_name should contain the provider code.
        This declaration enforces that subclasses define provider_long_name, but python will not check the type is str.
        :return: NoReturn
        """
        pass

    @property
    @abstractmethod
    def provider_url(self):
        """
        provider_url should contain the provider code.
        This declaration enforces that subclasses define provider_url, but python will not check the type is str.
        :return: NoReturn
        """
        pass

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dynamic_dim = collections.defaultdict(list)
        self.data = None
        self.sources = []
        self.source_complements = []

    def run(self, download=True, parallel_download=True):
        """
        Execute the job.
        :param download: if False, bypass source files download.
        :param parallel_download: if False, download source files sequentially.
        """
        self.get_sources()
        self.download_and_get_checksum(download, parallel_download)
        self.rm_sources_up_to_date()
        self.add_sources_to_dynamic_dim()
        self.transform_provider()
        self.transform()
        self.insert_new_dynamic_dim()
        self.upsert()
        self.update_sources_metadata()

    @abstractmethod
    def get_sources(self):
        """Should create a list of mutable object source in self.sources with at least
        3 attributes: 'url', 'code', 'path'. You can setup a list of complementary
        files for download in  self.source_complements"""

    def add_sources_to_dynamic_dim(self):
        """
        Adds sources in self.sources into self.dynamic_dim['source'], so they dimension.LU_source is fed
        as a dynamic dimension.
        :return: NoReturn
        """
        logger.debug("Adding sources to dynamic_dim['source']...")
        sources = [vars(copy(source)) for source in self.sources]
        self.dynamic_dim['source'] += sources
        logger.debug(f"{len(sources)} sources added to self.dynamic_dim['source']")
        self.remove_existing_dynamic_dim('source')

    def transform_provider(self):
        """
        Loads the provider dimension.
        :return: NoReturn
        """
        logger.debug("Transforming provider ...")
        logger.debug(f"Adding provider to dynamic_dim: {self.provider_code}")
        self.dynamic_dim['provider'] = [{
                    'code': self.provider_code,
                    'long_name': self.provider_long_name,
                    'url': self.provider_url
                    }]
        self.remove_existing_dynamic_dim('provider')

    @abstractmethod
    def transform(self):
        """
        Should create:
        *  In self.dynamic_dim_dfs a dictionary of the element of the dynamic
           dimensions to be inserted with the API (the key being the name of the dim)
           ex: {'source': [{'code': 'code1', 'url': 'url1', ...},
                           {'code': 'code2', ... }, ...],
                'entity': [{'code': 'code1', 'category': 'category1', ...},
                           {'code': 'code2', ... }, ...],
                ...}
        *  In self.data the data to be uploaded to upserted with the API
        """

    @timeit
    def download_and_get_checksum(self, download=True, parallel_download=True):
        """
        This function downloads all files listed in self.sources.

        :param download: Flag determining whether the file should be downloaded or not. Default is True.
        :param parallel_download: Flag determining whether download should occur in parallel. Default is True.
        :return NoReturn:
        """
        if download:
            logger.debug(f"download: {download}, parallel download: {parallel_download}")
            file_for_download = self.sources + self.source_complements
            if parallel_download:
                parallelize(self.download_source, file_for_download, MAX_WORKER)
            else:
                for f in file_for_download:
                    self.download_source(f)
        parallelize(calc_checksum_download, self.sources, MAX_WORKER)

    @timeit
    def rm_sources_up_to_date(self):
        for source in reversed(self.sources):
            logger.debug(f"rm_sources_up_to_date: processing {source.code}")
            try:
                db_source = get_db_source_dict(source.code)
                if 'checksum' in db_source and type(db_source['checksum']) == str \
                        and db_source['checksum'].strip() == source.checksum:
                    logger.debug(f"rm_sources_up_to_date: removing {source.code} from self.sources")
                    self.sources.remove(source)
            except ValueError as e:
                logger.debug(f"Source code {source.code} not found in dimension.source: it's a new source.")

    @timeit
    def insert_new_dynamic_dim(self):
        logger.debug(f"Running insert_new_dynamic_dim(): {len(self.dynamic_dim)} items")
        for dimension, data in self.dynamic_dim.items():
            logger.debug(f"Processing {dimension}: size {len(data)}")
            if len(data) > 0:
                logger.debug(f"{dimension}: loading {len(data)} rows")
                endpoint = f"{API_END_POINT}/dimension/{dimension}"
                batch_upload(data, endpoint, BATCH_SIZE_DIM)
        return None

    @timeit
    def upsert(self):
        endpoint = f"{API_END_POINT}/main/datapoint"
        if self.data is not None:
            batch_upload(self.data, endpoint, BATCH_SIZE)
        return None

    @timeit
    def update_sources_metadata(self):
        endpoint = f"{API_END_POINT}/dimension/source"
        r = None
        for source in self.sources:
            _id = get_db_source_dict(source.code)['id']
            data = {}
            data["last_update"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            data["checksum"] = source.checksum
            try:
                data["last_download"] = source.last_download
            except Exception:
                logger.warning(f"Attribute source.last_download not available. Running with download=False?")

            r = requests.put(f"{endpoint}/{_id}", json=data)
        return r

    def get_source_from_code(self, code):
        source = [s for s in self.sources if s.code == code]
        try:
            return source[0]
        except IndexError:
            return None

    @staticmethod
    def download_source(source, http_headers=None):
        """
        Download one given file.
        :param source: BaseSource object describing the file to download.
        :param http_headers: optional headers to pass in the request. Default is None.
        Defined as a static method to allow overloading
        """
        try:
            path, url = source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        r = requests.get(url,
                         proxies=PROXY_DICT,
                         headers=http_headers,
                         verify=SSL_CERTIFICATE_PATH)

        r.raise_for_status()

        file_path = FILE_STORE_PATH / path
        file_path.write_bytes(r.content)
        logger.debug(f'{len(r.content)} bytes written to {file_path.name}.')

        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def remove_existing_dynamic_dim(self, dimension, filters=None):
        """
        Remove items from self.dynamic_dim already existing in the database.
        :param dimension: the dimension name.
        :param filters: a dictionary containing the filter for the
                        get query.
        :return: None
        """
        query = f"{API_END_POINT}/dimension/{dimension}"
        if filters is not None:
            query += '?'
            for k, v in filters.items():
                query += f"{k}={v}&"
            query = query[:-1]

        logger.debug(f'remove_existing_dynamic_dim: query - {query}')
        r = requests.get(query)
        data = r.json()
        codes = [x['code'] for x in data]
        logger.debug(f"self.dynamic_dim['{dimension}'] size before: {len(self.dynamic_dim[dimension])}")
        self.dynamic_dim[dimension] = [x for x in self.dynamic_dim[dimension]
                                       if x['code'] not in codes]
        logger.debug(f"self.dynamic_dim['{dimension}'] size after: {len(self.dynamic_dim[dimension])}")


class ExtDbApiDedicatedTableJob(ExtDbApiJobV2, metaclass=ABCMeta):
    """
    A new (abstract) class built over ExtDbApiJob to offer the possibility to load the data
    into a separate, dedicated fact table.

    Everything functions as in ExtDbApiJob: you define get_sources() and transform() methods, and this class will do
    the rest when you call run() method.

    In addition to it, we also define the following variables:

    - self.key_columns - list of columns being part of the key
    - self.db_schema - the database SQL Server schema where the table is in.
    - self.db_table_prefix - prefix used to build the following 2 attributes:
    - self.db_temp_table - the SQL Server temporary table name. It starts with '#' and finishes with '_temp'.
    - self.db_final_table - the SQL server name for the final table. As a convention, we finish it with '_data'.

    Notice that not defining these attributes will raise a TypeError exception when instantiating the class.

    Another difference: we expect self.data to have a dataframe with the data to be loaded into the final table.

    The upsert() method is overridden and has the following behaviour:

    1. In case self.full_load=True, it truncates the final table and load the dataframe in self.data into it.
    2. In case self.full_load=False, it loads self.data into a temporary table, and then merges it's table with the
    existing final table, adding date_created for inserted rows, and date_modified for updated rows.

    The new_data_query() method returns a query to serve as an extension to the new data alert query.

    So, how to use it? Simple as just running job.run() and all will be done.
    """

    def __init__(self, chunk_size=10000, **kwargs):
        """
        Constructor.
        :param chunk_size: int: chunk_size for pd.to_sql(). Defaults to 10000.
        :param kwargs:
        """
        super().__init__(**kwargs)
        # moving from local temporary table (named as #...) to global temporary table (named ##...)
        # to avoid error:
        #  (pyodbc.ProgrammingError) ('String data, right truncation: length 616 buffer 510', 'HY000')
        self.db_temp_table = f'##{self.db_table_prefix}_temp'
        self.db_final_table = f'{self.db_table_prefix}_data'
        logger.info(f'Temporary table name: {self.db_temp_table}, final table name: {self.db_final_table}')
        self.chunk_size = chunk_size

    @property
    @abstractmethod
    def key_columns(self):
        """
        key_columns should contain a list of column names.
        This declaration enforces that subclasses define key_columns, but python will not check the type is a List[str].
        :return: NoReturn
        """
        pass

    @property
    @abstractmethod
    def db_schema(self):
        """
        Defines the SQL Server database schema where the table is in.
        This declaration enforces that subclasses define db_schema by python will not check if type is str.
        :return: NoReturn.
        """
        pass

    @property
    @abstractmethod
    def db_table_prefix(self):
        """
        Defines the table prefix, used to define db_temp_table and db_final_table variables.
        This declaration enforces that subclasses define db_schema by python will not check if type is str.
        :return: NoReturn
        """
        pass

    def build_merge_query(self):
        """
        Build the merge query.
        :return: str with the merge query.
        """
        # columns to compare (ignore key columns)
        all_columns = list(self.data.columns)
        cols_to_compare = [col for col in all_columns if col not in self.key_columns]
        # columns to insert in the table
        cols_to_insert = all_columns
        # expression for matching keys
        merge_on = [f'target.[{key}] = source.[{key}]'
                    for key in self.key_columns]
        # expression for finding differences in non-key columns
        merge_difference = [f'target.[{col}] <> source.[{col}]'
                            for col in cols_to_compare]
        # update expression (on non-key columns)
        merge_update = [f'target.[{col}] = source.[{col}]'
                        for col in cols_to_compare]
        # insert expression
        merge_insert_cols = [f'[{col}]' for col in cols_to_insert]
        merge_insert_values = [f'source.[{col}]' for col in cols_to_insert]
        # final query
        query = (f'MERGE {self.db_schema}.{self.db_final_table} target \n'
                 f'USING {self.db_schema}.{self.db_temp_table} as source \n'
                 f"ON ({' AND '.join(merge_on)}) \n"
                 f"WHEN MATCHED AND ({' OR '.join(merge_difference)})\n"
                 f"THEN UPDATE SET {', '.join(merge_update)}, target.[date_modified] = GETDATE()\n"
                 f'WHEN NOT MATCHED \n'
                 f"THEN INSERT ({', '.join(merge_insert_cols)}, [date_created]) \n"
                 f"VALUES ({', '.join(merge_insert_values)}, GETDATE());")

        logger.debug(f'Merge query: {query}')
        return query

    def upsert(self) -> NoReturn:
        """
        Overrides parent method to bypass API and write results directly to specific table.
        if self.full_load = True, it truncates final table and loads the content of self.data into it,
        It loads self.data into a temporary table and runs a merge with final table otherwise.
        :return: NoReturn
        """
        logger.info("Writing to database.")
        if self.data is None or len(self.data) == 0:
            logger.info("No data to load into the database.")
            return

        logger.debug(f'Database: {EXT_DB_STR}')
        engine = create_engine(
            EXT_DB_STR,
            fast_executemany=True)

        if self.full_load:
            # instead of truncating, send a create schema + to_sql(if_exists='replace')
            logger.debug('Sending truncate table statement')
            try:
                logger.info(f'Creating schema {self.db_schema}')
                engine.execute(f"CREATE SCHEMA {self.db_schema}")
            except Exception:
                # Pernicious thing: impossible to catch pyodbc.ProgrammingError ...
                logger.info(f'Schema {self.db_schema} already exists.')
                pass

            logger.info(f'Loading {len(self.data)} rows to database.')
            self.data['date_created'] = datetime.now()
            self.data['date_modified'] = pd.to_datetime(np.nan)
            logger.debug(f'Creating table {self.db_final_table} with these columns: {self.data.columns}')
            self.data.to_sql(self.db_final_table,
                             con=engine,
                             schema=self.db_schema,
                             index=False,
                             if_exists='replace',
                             chunksize=self.chunk_size)

            try:
                engine.execute(f'GRANT SELECT ON {self.db_schema}.{self.db_final_table} TO [IEA_EXTERNAL-DB_READ]')
            except Exception:
                logger.warning(f'Could not grant select to [IEA_EXTERNAL-DB_READ] on '
                               f'{self.db_schema}.{self.db_final_table}')
                pass
        else:
            # engine.begin() starts a transaction
            with engine.begin() as con:
                # loads data into temporary table and then merge with final table.
                logger.info(f'Loading {len(self.data)} rows to temporary table {self.db_temp_table}')
                self.data.to_sql(self.db_temp_table,
                                 con=con,
                                 schema=self.db_schema,
                                 index=False,
                                 if_exists='append',
                                 chunksize=self.chunk_size)

                logger.info(f'Merging data from {self.db_temp_table} into {self.db_final_table}')
                merge_query = self.build_merge_query()
                rs = con.execute(merge_query)
                logger.info(f'Merge from {self.db_temp_table} into {self.db_final_table} finished successfully.')

    def build_new_data_query(self, from_date: str) -> str:
        """
        The query needed to generate the new data alert.
        @param: from_date: str: date from which start searching new data.
        :return: str: the query that calculates the new data since yesterday.
        """
        return (f"select '{self.title}' as [source],"
                "       FORMAT([date], 'yyyyMM') as [period],"
                "       COUNT(1) as [affected_rows]"
                f"from [main].[{self.db_final_table}]"
                f"where (date_created > Cast('{from_date}' AS date)"
                f"or date_modified > Cast('{from_date}' AS date))"
                f"group by FORMAT([date], 'yyyyMM')"
                f"order by 2 desc")
