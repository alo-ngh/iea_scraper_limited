# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 16:01:55 2021

@author: NGHIEM_A AL-SAIDI_A
"""

from iea_scraper.core.job import BaseJob
from iea_scraper.jobs.com_google_trends.google_trends_querier import GoogleTrendsQuerier
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class GoogleTrendsJob(BaseJob):
    '''
    This scraper gets data from google trends
    '''
    title: str = "Google Trends"
    def __init__(self):
        self.google_trends_querier = GoogleTrendsQuerier()
        
    def run(self):
        '''
        Scrapes data from google trends, sends two dataframes to DW,
        one with with reference date with all pairs for all countries, 
        and the other with evolution for multiple technologies for all 
        countries over time
        '''
        self.google_trends_querier.run()

    def bulk_run(self, start_date, end_date, db_str):
        '''
        Scrapes data from google trends, sends two dataframes to DW,
        one with with reference date with all pairs for all countries, 
        and the other with evolution for multiple technologies for all 
        countries over time
        '''
        self.google_trends_querier.bulk_run(start_date=start_date, 
                                            end_date=end_date, write_sql=True,
                                            write_csv=False)
    
    def test_run(self):
        google_trends_querier_test =  GoogleTrendsQuerier()
        google_trends_querier_test.keywords = self.google_trends_querier.keywords[:3]
        google_trends_querier_test.countries = ['FR','UK']
        google_trends_querier_test.start_date = self.google_trends_querier.previous_month
        google_trends_querier_test.run(write_sql=False, write_csv=True, batch=False)
        
if __name__ == '__main__':
    google_trends_scraper = GoogleTrendsJob()
    google_trends_scraper.test_run()
    