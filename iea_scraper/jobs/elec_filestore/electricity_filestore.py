# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: NGHIEM_A
"""
import pandas as pd
import logging
import sys
from pathlib import Path
from iea_scraper.settings import FILE_STORE_PATH, EXT_DB_STR
from iea_scraper.core.exceptions import EdcJobError
import ntpath

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob

class ElectricityFilestoreJob(EdcJob):
    
    title: str = 'Electricity filestore scraper'
    '''Scrapes csv  files with prefix electricity_filestore
    located in the filestore'''
    
    def __init__(self):
        EdcJob.__init__(self)
        self.key_word = 'electricity_scraper_filestore'
        self.filestore_files = None
        self.df_all_data = None
        self.temporary_df_dw = None
        self.final_df_dw = pd.DataFrame()
       
    @property
    def df_dw(self):
        return self.temporary_df_dw if self.temporary_df_dw is not None else self.final_df_dw
    
    def find_files_in_filestore(self):
        csv_files = Path(FILE_STORE_PATH).glob('*.csv')
        elec_files = [file for file in csv_files 
                      if self.key_word in str(file)]
        if elec_files == []:
            raise EdcJobError('There are no files to load')
        return elec_files
        
    def concatenate_files(self):
        self.df_all_data = {ntpath.basename(file): pd.read_csv(file)
            for file in self.filestore_files}
        
    def delete_files(self):
        for file in self.filestore_files:
            Path.unlink(file) 
        
    def pre_run(self):
        '''gets each file from the filestore and tries to load it in the ele
        table
        '''
        self.filestore_files = self.find_files_in_filestore()
        self.concatenate_files()
        errors = []
        for filename, df in self.df_all_data.items():
            self.temporary_df_dw = df
            try:
                self.check_df_dw()
                self.final_df_dw = pd.concat([self.final_df_dw, self.df_dw_processed])
            except Exception as e:
                logger.warning(f'Could not concatenate  file: {filename}: {e}')
                errors += [f'{filename}: {e}']
        if self.final_df_dw.empty:
            raise EdcJobError('the following errors happened:'+ "\n".join(errors))
        self.delete_files()
        self.temporary_df_dw = None
        logger.info('Pre run completed')
        
        
if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    ef_scraper = ElectricityFilestoreJob()
    ef_scraper.test_run(folder)