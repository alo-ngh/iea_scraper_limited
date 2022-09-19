# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 15:32:04 2021

@author: NGHIEM_A
"""

from datetime import datetime
import pandas as pd
from iea_scraper.settings import ROOT_PATH 

GOOGLE_TRENDS_ROOT_PATH = ROOT_PATH /'iea_scraper' / 'jobs' / 'com_google_trends'

COUNTRIES_DF = pd.read_csv(GOOGLE_TRENDS_ROOT_PATH / 'core' / 'countries.csv')
COUNTRIES = list(COUNTRIES_DF['ISO2'])
KEYWORDS_ENERGY_EFFICIENCY = []
KEYWORDS_ELECTRICITY = ['Wind', 'Solar', 'Coal', 'Electric Vehicle', 'Hydrogen',
                        'Hydro']

df_from_csv = pd.read_csv(GOOGLE_TRENDS_ROOT_PATH / 'topics' / 'topics_with_topics_id.csv')
df_from_csv_idea_topic_id = df_from_csv[['Idea','topic_id']]
df_from_csv_idea_category = df_from_csv[['Idea','Category']]
list_from_csv_idea_topic_id = df_from_csv_idea_topic_id.to_dict(orient='records')
list_from_csv_idea_category = df_from_csv_idea_category.to_dict(orient='records')
DICT_KEYWORD_TO_TOPIC_IDS = {datapoint['Idea']:datapoint['topic_id'] for datapoint in list_from_csv_idea_topic_id}
DICT_KEYWORD_TO_CATEGORY = {datapoint['Idea']:datapoint['Category'] for datapoint in list_from_csv_idea_category}

KEYWORDS = list(DICT_KEYWORD_TO_TOPIC_IDS.keys())

START_DATE = datetime(2010, 1, 1)
FOLDER = GOOGLE_TRENDS_ROOT_PATH / 'csv'
DB_LOCATION = r'G:\EDC - Data team\4. Data\google_trends\google_trends.db'

