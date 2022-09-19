# -*- coding: utf-8 -*-
"""
Created on Thu Sep  9 11:13:38 2021

@author: NGHIEM_A
"""

from pytrends.request import TrendReq
import pandas as pd
from iea_scraper.settings import SSL_CERTIFICATE_PATH


def get_topic_id_from_keyword(keyword, topic_type='Topic'):
    '''
    Converts keyword into topic id

    Parameters
    ----------
    keyword : string
    topic_type : string. Can be Topic, chemical element, kitchenware, etc

    Returns
    -------
    topic_id : string

    '''
    pytrends = TrendReq(hl='en-US', tz=360, requests_args={'verify':SSL_CERTIFICATE_PATH})
    print(keyword)
    suggs = pytrends.suggestions(keyword) 
    print(suggs)
    topic_id = [el['mid'] for el in  suggs if (el['title'].lower() == keyword.lower() and el['type']==topic_type)][0]
    return topic_id

def get_topic_list():
    df = pd.read_csv(r'iea_scraper\jobs\com_google_trends\topics\topics.csv')
    df = df.loc[~df['Topic'].isnull()]
    df = df.loc[df['Used'] == 'Y']
    df = df.set_index('Topic')
    dict_topics = df['Topic type'].to_dict()
    return dict_topics

def get_topic_ids_to_topics_dict():
    dict_topics = get_topic_list()
    dict_topic_topic_id = {}
    for keyword, topic_type in dict_topics.items():
        topic_id = get_topic_id_from_keyword(keyword, topic_type)
        dict_topic_topic_id[keyword] = topic_id
    return dict_topic_topic_id

def add_topic_ids_to_csv():
    df = pd.read_csv(r'iea_scraper\jobs\com_google_trends\topics\topics.csv')
    df = df.loc[~df['Topic'].isnull()]
    df = df.loc[df['Used'] == 'Y']
    dict_topic_topic_id = get_topic_ids_to_topics_dict()
    df['topic_id'] = df['Topic'].apply(lambda x: dict_topic_topic_id[x])
    df.to_csv(r'iea_scraper\jobs\com_google_trends\topics\topics_with_topics_id.csv', index=False)


if __name__ == '__main__':
    add_topic_ids_to_csv()
    keyword = 'wind power'
    topic = get_topic_id_from_keyword(keyword)
