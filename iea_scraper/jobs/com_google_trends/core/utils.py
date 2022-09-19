# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 16:20:27 2021

@author: NGHIEM_A
"""
import time
from pytrends.exceptions import ResponseError
from requests.exceptions import ReadTimeout
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO)

def wait_and_retry(wait=4, retry=30, nb_retries=20):
    """wait and retry are in seconds"""
    def outer(fun):
        def inner(*args, **kwargs):
            n_retry = 0
            while n_retry<nb_retries:
                try:
                    time.sleep(wait)
                    n_retry=0
                    return fun(*args, **kwargs)
                except (ResponseError, ReadTimeout) as e:
                    logger.warning(f'{e} -> sleeps for ({retry} sec)')
                    time.sleep(retry)
                    n_retry += 1
                    return inner(*args, **kwargs)
        return inner
    return outer
            
