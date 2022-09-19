# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 13:56:30 2022

@author: NGHIEM_A
This scripts update the edc electricity metadata table with the new data
"""

import sqlalchemy as sa
from ieacore.connectors.datawarehouse_connector import merge_metadata as gmerge_metadata
from iea_scraper.settings import ROOT_PATH
from pathlib import Path

import pandas as pd
import xarray as xr
import numpy as np

table ="V_IEAGENERIC_LU_ELEDATARIGHTS"
engine = sa.create_engine("mssql+pyodbc://vimars/Division_EDC?driver=SQL Server?Trusted_Connection=yes",fast_executemany=True)

df = pd.read_sql(f"""SELECT   * 
                       FROM [Gandalf].[{table}]
                    """, con=engine)

df.loc[1,["Main Source"]] = 'yyy'

data_rights_path = Path(ROOT_PATH).joinpath('iea_scraper\core\metadata\edc_electricity_data_rights.csv')
df = pd.read_csv(data_rights_path)
gmerge_metadata(df, table, engine, 'FULL') #Replaces all content of the table in the db with this dataframe. In this case just updates one record. But it could Delete, Insert or Update