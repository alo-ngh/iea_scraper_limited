from concurrent.futures import ThreadPoolExecutor
from email.message import EmailMessage
import functools
from functools import wraps, lru_cache
import hashlib
from itertools import islice
import logging
import logging.config
import requests
import smtplib
import time
import types
import threading
import _thread
import sys
import pycountry
import os
import yaml
import numpy as np

from iea_scraper import settings
from iea_scraper.settings import API_END_POINT, FILE_STORE_PATH, ROOT_PATH, \
    EDC_TOLERATED_LISTS
    

logger = logging.getLogger(__name__)


def config_logging(conf):
    logging.config.dictConfig(conf)


@functools.lru_cache(maxsize=4)
def get_dimension_db_data(dimension, query_string=None):
    """
    Gets dimension data from the API.
    :param dimension: the dimension to get data from.
    :param query_string: A query string to add at the end of the request
    :return: a json with the results
    """
    query = f"{API_END_POINT}/dimension/{dimension}"
    if query_string is not None:
        query += f"?{query_string}"
    r = requests.get(query)
    try:
        return r.json()
    except Exception as e:
        raise EnvironmentError(f"Get result: {r.text}, \n url:{r.url} \n \n {e}")


def batch_upload(data, api_endpoint, batch):
    """
    Performs a batch load into the given API endpoint.
    :param data: a array of dictionaries to load (one dictionary per record).
    :param api_endpoint: the target API endpoint.
    :param batch: number of records per batch.
    :return: None
    """
    i = 0
    total_processed_rows = 0
    while True:
        if isinstance(data, types.GeneratorType):
            i = 0
        batch_data = list(islice(data, i * batch, (i + 1) * batch))
        if len(batch_data) > 0:
            i += 1
            try:
                r = requests.post(url=api_endpoint, json=batch_data)
                batch_rows = len(batch_data)
                logging.debug(f"Sending data to IEA External DB - Batch[{i}]: {batch_rows} rows")
                total_processed_rows += batch_rows
            except ConnectionError:
                r = requests.post(url=api_endpoint, json=batch_data)
            if r.status_code != 201:
                raise IOError(f"Issue for loop {i + 1}: \n {r._content}")
        else:
            break

    logging.info(f"{total_processed_rows} items sent to IEA External DB API instance at: {api_endpoint}")
    return None


def parallelize(function, param_list, max_workers=5):
    """
    Parallelizes the execution of the given function.
    :param function: the function to run in parallel.
    :param param_list: the list of parameters for each execution of the function.
    :param max_workers: maximum number of workers (it defaults to 5).
    :return: a list with the results of each execution.
    """
    logger.info(f'Executing function {function.__name__}() over {len(param_list)} items '
                f'with a maximum of {max_workers} parallel workers.')

    with ThreadPoolExecutor(max_workers) as executor:
        return [result for result in executor.map(function, param_list)]


def send_message(subject: str, message=None, mail_to=settings.MAIL_RECIPIENT,
                 mail_from=settings.MAIL_DEFAULT_SENDER, html_content=None):
    """
    Send a e-mail to default recipients.
    If html_content provided, send as HTML e-mail, plain-text with message otherwise.
    :param subject: the e-mail subject.
    :param message: the message.
    :param html_content: an HTML message.
    :param mail_from: sender email.
    :param mail_to: the recipient of the e-mail. Defaults to settings.MAIL_RECIPIENT.
    :return: None.
    """

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = mail_to

    if html_content:
        msg.add_header('Content-Type', 'text/html')
        msg.set_payload(html_content)
    else:
        msg.set_content(message)

    s = smtplib.SMTP(settings.MAIL_SERVER)
    s.starttls()
    s.send_message(msg)
    s.quit()
    return None


def get_db_source_dict(source_code):
    """
    Get meta-data for a specific source from database.
    :param source_code: the code identifying the source.
    :return: a JSON with source meta-data from database.
    """
    endpoint = f"{API_END_POINT}/dimension/source"
    r = requests.get(f"{endpoint}?code={source_code}")
    if r.status_code == 404:
        raise ValueError(f"No code: {source_code} in source!")
    source = r.json()[0]
    return source


def calc_checksum_download(source):
    """
    Calculates the checksum based on file's content.
    This is important not to overload as will be changed with file store
    :param source: BaseSource class describing the file. Uses the path parameter
    to locate the file.
    """
    file_path = FILE_STORE_PATH / source.path
    logger.info(f"Calculating checksum of file {file_path.name}")
    content = file_path.read_bytes()
    logger.debug(f'{len(content)} bytes read from {file_path.name}')
    checksum = hashlib.md5(content).hexdigest()
    setattr(source, 'checksum', checksum)
    logger.info(f"Checksum calculated and appended to source with code '{source.code}'")


def timeit(method):
    """
    Calculates the execution time of a function
    :param method: function whose execution time is measured.
    :return: the execution time of the function
    """

    @wraps(method)
    def timed(*args, **kwargs):
        ts = time.time()
        res = method(*args, **kwargs)
        te = time.time()
        logger.info(f"{method.__name__}: {(te - ts) * 1000} ms")
        return res

    return timed


def timeout(s):
    """
    @param s: int: number of seconds to wait.
    use as decorator to exit process if 
    function takes longer than s seconds
    """
    def outer(fn):
        def quit_function(fn_name):
            # print to stderr, unbuffered in Python 2.
            sys.stderr.flush() # Python 3 stderr is likely buffered.
            _thread.interrupt_main() # raises KeyboardInterrupt

        def new_func(*args, **kwargs):
            timer = threading.Timer(s, quit_function, args=[fn.__name__])
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result

        def inner(*args, **kwargs):
            try:
                new_func(*args, **kwargs)
            except KeyboardInterrupt:
                raise TimeoutError(f'{fn.__name__} took more than {s} seconds')
        return inner
    return outer


@lru_cache(maxsize=1)
def get_country_dict():
    country_list = list(pycountry.countries)
    country_name_dict = {country.name: country.alpha_3 for country in country_list}
    country_name_begin_dict = {country.name.split(',')[0]: country.alpha_3 for country in country_list}
    country_alpha_2_dict = {country.alpha_2: country.alpha_3 for country in country_list}
    country_official_name_dict = {}

    # Build official name dictionary in try except, in order to skip countries with no official name
    for country in country_list:
        if 'official_name' in country.__dict__['_fields']:
            country_official_name_dict[country.official_name] = country.alpha_3

    country_dict = {'name': country_name_dict,
                    'alpha_2': country_alpha_2_dict,
                    'official_name': country_official_name_dict,
                    'name_begin': country_name_begin_dict
                    }
    return country_dict


def get_country_iso3(country_field):
    for country_mapping in get_country_dict().values():
        if country_field in country_mapping:        
            return country_mapping[country_field]
    return country_field

def load_config(config):
    """
    Will read the config.yml and create a config_dict
    Dynamic config elements are listed in dict_settings_exceptions
    """
    if type(config) == str:
        filepath = os.path.join(ROOT_PATH / "iea_scraper" / "core" / "edc_config" / f"{config}.yml")
        with open(filepath) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
    elif type(config) == dict:
        conf = config
    dict_settings_exceptions = EDC_TOLERATED_LISTS
    for key, val in conf.items():
        if type(val) == str:
            if val in dict_settings_exceptions:
                conf[key] = dict_settings_exceptions[val]
        elif type(val) == list:
            if 'nan' in val:
                conf[key].append(np.nan)
        elif type(val) == dict:
            new_val = load_config(val)
            conf[key] = new_val
    return conf
