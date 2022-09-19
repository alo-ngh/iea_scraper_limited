# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 17:54:59 2021

@author: NGHIEM_A
This file enables a full completion of all scrapers on all dates available

"""
import sys
import os
sys.path.append('C:\Repos\scraper')
from iea_scraper.settings import EDC_ALL_DAILY_JOBS, LOGGING_EDC_POPULATE_DB, EDC_DAILY_JOBS_BATCH_ELEC, \
    EDC_DAILY_JOBS_BATCH_GAS_OTHERS
from iea_scraper.core.utils import config_logging
from iea_scraper.core import factory
from iea_scraper.instance import EXT_DB_STR
from iea_scraper.core.exceptions import EdcJobError
import logging
from datetime import datetime, timedelta
from multiprocessing import Process
import multiprocessing_logging



config_logging(LOGGING_EDC_POPULATE_DB)
logger = logging.getLogger()

def populate_db(start_date=None, end_date=None, db_str=EXT_DB_STR, 
                jobs=EDC_ALL_DAILY_JOBS.keys()):
    job_params_lst = [EDC_ALL_DAILY_JOBS[job] for job in jobs]
    for job_params in job_params_lst:
        job = factory.get_scraper_job(**job_params)
        logger.info(f'Launching {job} at {datetime.now()}')
        # try:
        job.bulk_run(db_str=db_str, start_date=start_date, end_date=end_date)
        logger.warning(f'{job} was successful at {datetime.now()}')
        # except:
        #     logger.warning(f'{job} did not work')
            
def populate_db_by_date(start_date=None, end_date=None, db_str=None, folder=None,
                         error_tolerance=20, jobs=EDC_ALL_DAILY_JOBS.values()):
    '''
    Parameters
    ----------
    start_date : datetime, optional
        Earliest day to scrape to populate DB. If None, will try to go as far as possible.
        The default is None.
    end_date : datetime, optional
        Latest day to scrape to populate DB. If None, will start from the latest available day.
        The default is None.
    db_str : str, optional
        If None, no db insert. The default is None.
    error_tolerance : int, optional
        Number of consecutive errors before the scraper stops. The default is 7.

    '''
    start_date = datetime(1900,1,1) if start_date is None else start_date
    end_date = datetime(2500,1,1) if end_date is None else end_date
    
    for job_params in jobs:
            errors=0
            job = factory.get_scraper_job(**job_params)
            logger.warning(f'Launching {job} at {datetime.now()}')
            current_date = min(job.last_available_date, end_date)
            while current_date >= start_date and errors < error_tolerance:
                try:
                    job = factory.get_scraper_job(**job_params)
                    if job.bulk:
                        job.run_date(current_date) 
                        if db_str is not None:
                            job.check_df_dw()
                            job.to_sql(db_str)
                        if folder is not None:
                            job.df_dw_processed.to_csv(os.path.join(folder, f'{job.name}_{start_date.date()}_{end_date.date()}.csv'))
                    logger.warning(f'{job} completed for {current_date}')
                    errors = 0
                except Exception as e:
                    logger.warning(f'{job} failed on {current_date}. Error: {e}')
                    errors += 1
                current_date -= timedelta(days=1)
                if errors == error_tolerance:
                    logger.warning(f'{job} stopped after {error_tolerance} consecutive errors')

def populate_db_by_batch(start_date=None, end_date=None, db_str=None, folder=None,
                         batch_days=20, error_tolerance=7, jobs=EDC_ALL_DAILY_JOBS.values()):
    '''
    Parameters
    ----------
    start_date : datetime, optional
        Earliest day to scrape to populate DB. If None, will try to go as far as possible.
        The default is None.
    end_date : datetime, optional
        Latest day to scrape to populate DB. If None, will start from the latest available day.
        The default is None.
    db_str : str, optional
        If None, no db insert. The default is None.
    batch_days : int, optional
        Number of days per batch. The default is 20.
    error_tolerance : int, optional
        Number of consecutive errors before the scraper stops. The default is 7.

    '''
    start_date = datetime(1900,1,1) if start_date is None else start_date
    end_date = datetime(2500,1,1) if end_date is None else end_date
    
    for job_params in jobs:
        try:
            errors=0
            job = factory.get_scraper_job(**job_params)
            logger.warning(f'Launching {job} at {datetime.now()}')
            end_date_batch = min(job.last_available_date, end_date)
            start_date_batch = end_date_batch - timedelta(days=batch_days - 1)
            while end_date_batch >= start_date and errors < error_tolerance:
                job = factory.get_scraper_job(**job_params)
                if job.bulk:
                    job.bulk_run(start_date=start_date_batch, end_date=end_date_batch, 
                                 db_str=db_str, error_tolerance=error_tolerance)
                    if folder is not None:
                        job.df_dw_processed.to_csv(os.path.join(folder, f'{job.name}_{start_date.date()}_{end_date.date()}.csv'))
                    errors = (job.earliest_available_date - start_date_batch).days
                    logger.warning(f'{job} completed batch between {start_date_batch.date()} and {end_date_batch.date()} with {errors} errors')
                    end_date_batch = start_date_batch - timedelta(days=1)
                    start_date_batch = end_date_batch - timedelta(days=batch_days - 1)
                    logger.warning(f'{job} completed with earliest date {job.earliest_available_date.date()}')
        except Exception as e:
            logger.warning(f'{job} failed with earliest scraped date {job.earliest_available_date}. Error: {e}')

def run_many_years_and_countries(start_end_countries, db_str=EXT_DB_STR, 
                                 folder=None, parallelise=None, job_type=None):
    '''
    Runs batches for all the years and countries in countries
    Parameters
    ----------
    start_end_countries : list of tuples (start_date, end_date, list of countries)
    db_str: str
    folder: str
    parallelise: full --> Every start, end, country task is sent on a new thread
                 country --> Every country is sent on a new thread
                 None --> No parallelisation at all.
    job_type: None --> run all jobs
                'electricity' --> run electricity jobs
                'other' --> run other jobs (and gas)
    '''
    job_dict = {}
    if job_type is None:
        job_dict = EDC_ALL_DAILY_JOBS
    elif job_type == 'electricity':
        job_dict = EDC_DAILY_JOBS_BATCH_ELEC
    elif job_type == 'other':
        job_dict = EDC_DAILY_JOBS_BATCH_GAS_OTHERS
    else:
        raise EdcJobError("job_type must be in [None, 'electricity','other']")

    if parallelise not in ['full', 'country', None]:
        raise ValueError("parallelise argument has to be either 'full', "\
                         "'country' or None")
    processes = []
    if parallelise == 'country':
        start_end_countries_by_country = {country: [] for _,_,countries in start_end_countries
                                          for country in countries}
        for start, end, countries in start_end_countries:
            for country in countries:
                start_end_countries_by_country[country].append((start, end, [country]))
        for country, start_end_countries_c in start_end_countries_by_country.items():
            kwargs = {
                'start_end_countries':start_end_countries_c, 
                'db_str': db_str, 
                'folder': None, 
                'parallelise': None}
            p = Process(target=run_many_years_and_countries, kwargs=kwargs)
            processes.append(p)
    else:
        for start, end, countries in start_end_countries:
            end_date = end if isinstance(end, datetime) else datetime(end, 12, 31)
            end_date = min(end_date, datetime.now())
            start_date = start if isinstance(start, datetime) else datetime(start, 1, 1)
            for country in countries:
                kwargs = {
                    'start_date':start_date, 
                    'end_date': end_date,
                    'db_str': db_str, 
                    'folder': folder, 
                    'error_tolerance': 20,
                    'jobs': [job_dict[country]]
                          }
                if parallelise is not None:
                    p = Process(target=populate_db_by_date, kwargs=kwargs)
                    processes.append(p)
                else:
                    populate_db_by_date(**kwargs)
    if parallelise is not None:
        for p in processes:
            p.start()
        for p in processes:
            p.join()
            
def check_start_end_countries(start_end_countries, job_type=None):
    '''
    Parameters
    ----------
    start_end_countries : list of tuples {start_date, end_date, list of countries}
    '''
    job_dict = {}
    if job_type is None:
        job_dict = EDC_ALL_DAILY_JOBS
    elif job_type == 'electricity':
        job_dict = EDC_DAILY_JOBS_BATCH_ELEC
    elif job_type == 'other':
        job_dict = EDC_DAILY_JOBS_BATCH_GAS_OTHERS
    else:
        raise EdcJobError("job_type must be in [None, 'electricity','other']")
    countries_to_scrape = set(sum([tpl[2] for tpl in start_end_countries], []))
    fake_countries = []
    for country in countries_to_scrape:
        try:
            factory.get_scraper_job(**job_dict[country])
            print(f'{country} ok')
        except:
            print(f'{country} failed')
            fake_countries += [country]
    all_jobs = list(job_dict.keys())
    all_jobs.sort()
    if fake_countries:
        raise EdcJobError(f'{fake_countries} must be in {all_jobs}')
    
    
def populate_european_db(country_list, start, end, db_str=EXT_DB_STR):
    '''
    Scrapes only European countries embedded in 
    ----------
    country_list : list
        list of European countries to scrape
    start : datetime
    end : datetime
    '''
    from iea_scraper.jobs.eu_entsoe.european_power_stats import EuropeanCountryPowerStatsJob
    scrapers = [EuropeanCountryPowerStatsJob(country) for country in country_list]
    for scraper in scrapers:
        scraper.bulk_run(start_date=start, end_date=end, 
                                  db_str=db_str)

if __name__ == "__main__":
    country_list = ['Ukraine']
    #populate_european_db(country_list, datetime(2021, 1, 1), datetime(2021, 12, 1), db_str=None)

    start_end_countries = [(datetime(2015,1,1), datetime(2021,1,27),['Europe'])]

    check_start_end_countries(start_end_countries)
    run_many_years_and_countries(start_end_countries, folder=None, db_str=EXT_DB_STR,
       parallelise=None, job_type='electricity')
    # jobs = ['google_trends']
    # populate_db(start_date=datetime(2010,1,1), end_date=datetime(2021,9,1), jobs=jobs)
