# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 17:24:15 2021
@author: NGHIEM_A, ALSAIDI_A, ROBINSON_M
"""

from pytrends.request import TrendReq
from datetime import datetime, timedelta
import pandas as pd
import math
import sqlalchemy
from iea_scraper.jobs.com_google_trends.core.config import COUNTRIES, KEYWORDS, DICT_KEYWORD_TO_TOPIC_IDS, DICT_KEYWORD_TO_CATEGORY, START_DATE, FOLDER
from iea_scraper.settings import SSL_CERTIFICATE_PATH
from iea_scraper.jobs.com_google_trends.core.utils import wait_and_retry
from iea_scraper.settings import EXT_DB_STR
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO)

class GoogleTrendsQuerier:
    start_run = datetime.now()
    def __init__(self, keywords=KEYWORDS, countries=COUNTRIES, 
                 start_date=START_DATE, folder=FOLDER):
        self.keywords = keywords
        self.countries = countries
        self.start_date = start_date
        self.end_date = datetime.now()
        self.folder = folder
        self.df_interest_all_techs = None
        self.df_all_word_pairs = None
        self.start_run = datetime.now()
        self.time_step1 = datetime.now()
        self.time_step2 = datetime.now()
    
    @property
    def minutes_to_start(self):
        return (datetime.now() - self.start_run).seconds//60
    
    def get_action_time(self):
        self.time_step1 = self.time_step2
        self.time_step2 = datetime.now()
        action_time = self.time_step2 - self.time_step1
        logger.info(f'Iteration took {action_time.seconds} seconds')
                
    @property
    def previous_month(self):
        previous_month = self.end_date
        previous_month = previous_month.replace(day=1)
        previous_month -= timedelta(days=1)
        previous_month = previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return previous_month
    
    @property
    def keyword_pairs(self):
        """All combinations of two keywords from self.keywords"""
        list_word_pairs = [(self.keywords[i], self.keywords[j]) for i in range(len(self.keywords)-1) 
                           for j in range(i+1, len(self.keywords))]
        return list_word_pairs
    
    @wait_and_retry(retry=30)
    def search_with_start_end_date(self, keywords, start_date, end_date, 
                                   countries=None):
        '''
        Used for word pairs. Doesn't use the categories
        
        Parameters
        ----------
        keywords : List
        start_date : datetime
        end_date : datetime
        countries: list of countries, None if every country 
    
        Returns
        -------
        Dataframe with columns geoCode, technology1, technology2, technology3...
        The purpose of this dataframe is to compare techs popularity search in every country
        from start date to end date
        This function converts keywords into topic IDs
        '''
        requests_args = {'verify': SSL_CERTIFICATE_PATH}
        tr = TrendReq(hl='en-US', tz=360, requests_args=requests_args)
        topic_id = [DICT_KEYWORD_TO_TOPIC_IDS[x] for x in keywords]
        tr.build_payload(topic_id, cat=0, timeframe = start_date.strftime("%Y-%m-%d") +" "+ end_date.strftime("%Y-%m-%d"), gprop='')
        df_interest = tr.interest_by_region(resolution='country', inc_low_vol=False, inc_geo_code=True)
        if countries is not None:
            df_interest = df_interest.loc[df_interest['geoCode'].isin(self.countries)]
        df_interest = df_interest.loc[df_interest[topic_id].sum(axis=1) > 0]    
        logger.info(f'Succeeded to extract data for popularity of {keywords} per country between {start_date.date()} and {end_date.date()}')
        self.get_action_time()
        return df_interest
    
    @wait_and_retry(retry=30)
    def search_with_reference_keyword(self, reference_keyword, country, start_date, end_date):
        '''
        Used for interest by tech
        
        Parameters
        ----------
        reference_keyword : string
        country : string
        start_date : datetime
        end_date : datetime
    
        Returns
        -------
        Dataframe with columns date, technology, country, value 
        The purpose is to see the evolution for one tech one country over time
        '''
        requests_args = {'verify': SSL_CERTIFICATE_PATH}
        tr = TrendReq(hl='en-US', tz=360, requests_args=requests_args)  
        topic_id = DICT_KEYWORD_TO_TOPIC_IDS[reference_keyword]
        category = DICT_KEYWORD_TO_CATEGORY[reference_keyword]
        country_query = '' if country == 'GLOBAL' else country
        tr.build_payload([topic_id], cat=category, 
                               timeframe=start_date.strftime('%Y-%m-%d') +' '+ \
                               end_date.strftime('%Y-%m-%d'),
                               gprop='', geo=country_query)    
        df_interest_over_time = tr.interest_over_time()
        if df_interest_over_time.empty:
            logger.info(f'No data for {reference_keyword} in {country} between {start_date.date()} and {end_date.date()}')
        else:
            df_interest_over_time = df_interest_over_time.reset_index()
            df_interest_over_time = df_interest_over_time.drop(columns= 'isPartial')  
            logger.info(f'Succeeded to extract data for evolution of {reference_keyword} in {country} between {start_date.date()} and {end_date.date()}')
            self.get_action_time()
        return df_interest_over_time
        
    
    def search_with_ref_keyword_all_countries(self, reference_keyword, countries, start_date, end_date):
        '''
        Parameters
        ----------
        reference_keyword : string
        countries : list of strings
        start_date : datetime
        end_date : datetime
    
        Returns
        -------
        Dataframe with columns date, technology, country, value 
        The purpose is to see the evolution for one tech for all countries over time
    
        '''
        list_df_1country = []
        df_interest_over_time_all_countries = pd.DataFrame()
        for country in countries:
           df_interest_over_time_1country = self.search_with_reference_keyword(reference_keyword, country, start_date, end_date)
           if not df_interest_over_time_1country.empty:
               df_interest_over_time_1country['iso2'] = country 
               df_interest_over_time_1country['technology'] = reference_keyword
               df_interest_over_time_1country['value'] = df_interest_over_time_1country[DICT_KEYWORD_TO_TOPIC_IDS[reference_keyword]]
               df_interest_over_time_1country['version_date'] = self.end_date
               df_interest_over_time_1country = df_interest_over_time_1country.drop(columns=DICT_KEYWORD_TO_TOPIC_IDS[reference_keyword])
               list_df_1country += [df_interest_over_time_1country]
               df_interest_over_time_all_countries = pd.concat(list_df_1country)
        if df_interest_over_time_all_countries.empty:
            logger.warning(f'{reference_keyword} returns no interest data over time')
        return df_interest_over_time_all_countries
    
    def search_with_multiple_keyword_all_countries(self, keywords, countries, start_date, end_date):  
        '''
        Parameters
        ----------
        keywords : list of strings
        countries : list of strings
        start_date : datetime
        end_date : datetime
    
        Returns
        -------
        Dataframe with columns date, technology, country, value 
        The purpose is to see the evolution for multiple technologies for all countries over time 
    
        '''
        list_df_1tech = []
        for keyword in keywords:
            df_interest_by_tech = self.search_with_ref_keyword_all_countries(keyword, countries, start_date, end_date)
            list_df_1tech += [df_interest_by_tech]
        df_interest_all_techs = pd.concat(list_df_1tech)
        self.df_interest_all_techs = df_interest_all_techs
    
    def search_all_by_word_pairs(self, keywords, start_date, end_date, batch=False):
        '''
        Parameters
        ----------
        keywords : list of strings
        start_date : datetime
        end_date : datetime
        batch : boolean - if True, runs in weekly batches 
        
        Returns
        -------
        Runs the search with reference date with all pairs for all countries
        for all pairs of key words (e.g. for 3 keywords: keyword1-keyword2, keyword2-keyword3 and keyword1-keyword3)
        '''
        list_word_pairs = [(keywords[i], keywords[j]) for i in range(len(keywords)-1) for j in range(i+1, len(keywords))]
        if batch: 
            batch_size = math.ceil(len(list_word_pairs)/7)
            week_day = self.start_run.weekday()
            if week_day < 7:
                list_word_pairs = list_word_pairs[batch_size*week_day:batch_size*(week_day+1)]
            else:
                list_word_pairs = list_word_pairs[batch_size*week_day:]
                
        list_df_1word_pair = []
        for word_pair in list_word_pairs:
            df_pair = self.search_with_start_end_date(word_pair, start_date, end_date, self.countries)
            df_pair['keyword_01'] = word_pair[0]
            df_pair['keyword_02'] = word_pair[1]
            df_pair['value_01'] = df_pair[DICT_KEYWORD_TO_TOPIC_IDS[word_pair[0]]]
            df_pair['value_02'] = df_pair[DICT_KEYWORD_TO_TOPIC_IDS[word_pair[1]]]
            df_pair['iso2'] = df_pair['geoCode']
            df_pair['date'] = start_date
            df_pair['version_date'] = self.end_date
            df_pair = df_pair.reset_index()
            df_pair = df_pair[['date', 'version_date', 'iso2', 'keyword_01', 
                               'keyword_02', 'value_01', 'value_02']]
            df_pair = df_pair.loc[(df_pair['value_01']>0) & (df_pair['value_02']>0)]
            list_df_1word_pair += [df_pair]
        df_all_word_pairs = pd.concat(list_df_1word_pair)
        return df_all_word_pairs
    
    def search_all_by_word_pairs_by_month(self, start_date, end_date, keywords,
                                          batch=False):
        '''
        Runs search_all_by_word_pairs for all dates in timeseries by month
    
        Parameters
        ----------
        start_date : datetime
        end_date : datetime
        keywords : list of strings
    
        Returns
        -------
        Dataframe with columns date (by month), keyword, country, value
        '''
        list_pairs_1month = []
        first_days_of_month = pd.date_range(start=start_date, end=end_date, freq='MS') 
        for first_day_of_month in first_days_of_month:
            last_day_of_month = (first_day_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            df_month = self.search_all_by_word_pairs(keywords, first_day_of_month, 
                                                     last_day_of_month, batch)
            list_pairs_1month += [df_month]
        self.df_all_word_pairs = pd.concat(list_pairs_1month)
            
    def to_sql(self, db_str=EXT_DB_STR):
        ''' Sends self.df_interest_all_techs and self.df_all_word_pairs to DB'''
        engine = sqlalchemy.create_engine(db_str, fast_executemany=True)
        if self.df_all_word_pairs is not None:
            self.df_all_word_pairs.to_sql(name='google_trends_keyword_comparison', con=engine, schema='edc', if_exists='append', index=False)
            self.df_all_word_pairs = None
        if self.df_interest_all_techs is not None:
            self.df_interest_all_techs.to_sql(name='google_trends_interest_by_tech', con=engine, schema='edc', if_exists='append', index=False)
            self.df_interest_all_techs = None
        
    def to_csv(self, folder):
        ''' Saves self.df_interest_all_techs and self.df_all_word_pairs to 2
        csv files in folder
        '''
        str_date = datetime.now().strftime('%Y%m%d')
        if self.df_all_word_pairs is not None:
            keyword_list_1 = self.df_all_word_pairs['keyword_01'].unique()
            keyword_list_2 = self.df_all_word_pairs['keyword_02'].unique()
            if len(keyword_list_1) == 1 and len(keyword_list_2) == 1:
                self.df_all_word_pairs.to_csv(f'{str(folder)}//{str_date}_{keyword_list_1[0]}_{keyword_list_2[0]}_wordpairs.csv', index=False)
            else:
                self.df_all_word_pairs.to_csv(f'{str(folder)}//{str_date}_all_wordpairs.csv', index=False)
        if self.df_interest_all_techs is not None:
            keyword_list = self.df_interest_all_techs['technology'].unique()
            if len(keyword_list) == 1:
                self.df_interest_all_techs.to_csv(f'{str(folder)}//{str_date}_interest_{keyword_list[0]}.csv', index=False)
            else:
                self.df_interest_all_techs.to_csv(f'{str(folder)}//{str_date}_interest_all_techs.csv', index=False)
        
    def bulk_run(self, start_date, end_date, write_sql=True, write_csv=False,
                 query='word_pair'):
        ''' generates the 2 dataframes for and sends them to the DW if write_sql = True
        and saves 2 csv files if write_csv=True from start_date to end_date
                Parameters
        ----------
        start_date : datetime
        end_date : datetime
        write_sql : boolean
        write_csv : boolean
        query : string - can be all, tech or word_pair
        '''
        if query not in ['all','tech','word_pair']:
            raise KeyError("Query needs to be 'all', 'tech' or 'word_pair'")
        if query in ['all', 'tech']:
            for keyword in self.keywords:
                self.search_with_multiple_keyword_all_countries([keyword], self.countries, 
                                                        self.start_date, self.end_date)
                self.store(write_sql=write_sql, write_csv=write_csv)
        if query in ['all', 'word_pair']:
            for month_start in pd.date_range(start_date, end_date, freq='MS'):
                for keyword_pair in self.keyword_pairs:
                    self.search_all_by_word_pairs_by_month(month_start, 
                                              month_start, keyword_pair,
                                              batch=False) 
                    self.store(write_sql=write_sql, write_csv=write_csv)
                    logger.info(f'Interest for {keyword} stored')
         
        end_run = datetime.now()
        run_time = end_run - self.start_run
        logger.info(run_time)
        
    def store(self, write_sql=False, write_csv=True): 
        '''writes data to database and/or csv 
        '''
        if write_sql:
            self.to_sql()
        if write_csv:
            self.to_csv(self.folder)
        
    def run_wout_schedule(self, write_sql=True, write_csv=False, query='all', batch=True):
        ''' generates the 2 dataframes  and sends them to the DW if write_sql = True
        and saves 2 csv files if write_csv=True for production
            
        Parameters
        ----------
        write_sql : boolean
        write_csv : boolean
        query : string - can be all, tech or word_pair
        '''
        if query not in ['all','tech','word_pair']:
            raise KeyError("Query needs to be 'all', 'tech' or 'word_pair'")
        if query in ['all', 'tech']:
            for keyword in self.keywords:
                self.search_with_multiple_keyword_all_countries([keyword], self.countries, 
                                                        self.start_date, self.end_date)
                self.store(write_sql=write_sql, write_csv=write_csv)
                logger.info(f'Interest for {keyword} stored from '
                            '{self.start_date.date()} to {self.end_date.date()}'
                            ' stored after {self.minutes_to_start}')
        if query in ['all', 'word_pair']:
            for keyword_pair in self.keyword_pairs:
                self.search_all_by_word_pairs_by_month(self.previous_month, 
                                              self.previous_month, keyword_pair,
                                              batch) 
                self.store(write_sql=write_sql, write_csv=write_csv)
                logger.info(f'Keyword comparison between {keyword_pair[0]} and '
                            '{keyword_pair[1]} for {self.previous_month.date()} '
                            'stored after {self.minutes_to_start}')
     
        end_run = datetime.now()
        run_time = end_run - self.start_run
        logger.info(run_time)
        
    def run(self, write_sql=True, write_csv=False):
        ''' generates the 2 dataframes  and sends them to the DW if write_sql = True
        and saves 2 csv files if write_csv=True for production
        schedule is a 2 week cycle, where first week schedule_numbers are 0-6 
        and second week are 7-13. (e.g. Tuesday week 2 schedule_number=8)
        for even schedule_days:
            keywords in google_trends_interest_by_tech 
            will be loaded to DW in two batches (each batch every 4th day)
        for odd schedule_days:
            word pairs in google_trends_keyword_comparison in 7 batches
            
        Parameters
        ----------
        write_sql : boolean
        write_csv : boolean
        query : string - can be all, tech or word_pair
        '''
        week_number = self.start_run.isocalendar()[1]
        week_day = self.start_run.weekday()
        schedule_number = week_number % 2 * 7 + week_day
        if schedule_number in [i*2 for i in range(0,7)]:
            batch_size = math.ceil(len(self.keywords)/2)
            batch_number = schedule_number // 2
            if batch_number in [i*2 for i in range(0,4)]:
                keywords_batch = self.keywords[:batch_size]
            else:
                keywords_batch = self.keywords[batch_size:]
            for keyword in keywords_batch:
                self.search_with_multiple_keyword_all_countries([keyword], self.countries, 
                                                self.start_date, self.end_date)
                self.store(write_sql=write_sql, write_csv=write_csv)
        else:
            list_word_pairs = self.keyword_pairs
            batch_size = math.ceil(len(list_word_pairs)/7)
            batch_number = schedule_number // 2
            if batch_number < 6:
                list_word_pairs = list_word_pairs[batch_size*batch_number:batch_size*(batch_number+1)]
            else:
                list_word_pairs = list_word_pairs[batch_size*batch_number:]
            for word_pair in list_word_pairs:
                self.search_all_by_word_pairs_by_month(self.previous_month, 
                                          self.previous_month, word_pair) 
                self.store(write_sql=write_sql, write_csv=write_csv)
     
        end_run = datetime.now()
        run_time = end_run - self.start_run
        print(run_time)

if __name__ =='__main__':
    countries = COUNTRIES
    keywords = KEYWORDS
    querier = GoogleTrendsQuerier(keywords=keywords, countries=countries)
    querier.run(write_sql = False, write_csv = True)
    
