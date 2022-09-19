import os
import re
import glob
import datetime
import requests

from bs4 import BeautifulSoup
from pathlib import Path
from copy import copy
import pandas as pd

from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.settings import FILE_STORE_PATH, PROXY_DICT

import logging

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()
PROVIDER_LONG_NAME = "Information-Analytical Centre of Oil and Gas - JSC"

FILE_PREFIX = "daily"
SOURCE_URL = "http://www.iacng.kz/en/"
TITLE = "Daily indicators of oil and gas treatment of the RK"
DATE_REGEX = "([0-9]{2}\.[0-9]{2}\.[0-9]{4})"

ENCODING = 'utf-8'

TON_TO_BBL_RATIO = 7.65

ROW_DETAILS = {
    # added by LMR on 22-10-2019 (text changed on the website, let's keep previous as well)
    'Oil and gas condensate, thousand tons':
        {'flow': 'SUPPLY',
         'unit': 'KT',
         'product': 'CRUDEOIL'},
    # old text
    'Oil and gas condensate, outdoors. ton':
        {'flow': 'SUPPLY',
         'unit': 'KT',
         'product': 'CRUDEOIL'},
    # added by LMR on 22-10-2019 (text changed on the website, let's keep previous as well)
    'Gas production, million cubic meters':
        {'flow': 'SUPPLY',
         'unit': 'MCM',
         'product': 'NATGAS'
         },
    # old text
    'Gas production, million cubic meters. m':
        {'flow': 'SUPPLY',
         'unit': 'MCM',
         'product': 'NATGAS'
         },
    'Processing of oil at the refinery of the RK thousand tons':
        {'flow': 'REFINOBS',
         'unit': 'KT',
         'product': 'CRUDEOIL'}
}

AREA = "KAZAKHSTAN"
FREQUENCY = "Daily"
ORIGINAL = True


class KzDailyOilProdJob(job.ExtDbApiJob):
    """
    Job for extracting daily oil production from Kazakhstan.
    """
    title: str = "Kazakhstan - Daily Oil Production"

    def __init__(self, **kwargs):
        """
        Constructor. It goes to website and extract the necessary part.
        :param kwargs: pass-through parameters to super.__init__()
        """
        super().__init__(**kwargs)

        page = requests.get(SOURCE_URL, proxies=PROXY_DICT)

        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
            self.current_raw_data = soup.find_all('div', class_='bl-areas')[0]
        else:
            raise ValueError(f"Website {SOURCE_URL} not available.")

    def get_sources(self):
        """
        Define the sources to load.
        if not full_load, it loads the date from the current website content.
        if full_load, loads the full existing history (and ignores the download).
        """
        current_date = _get_date(self.current_raw_data)
        ref_file = f"{JOB_CODE}_{FILE_PREFIX}_{current_date}.html"

        self.sources.append(BaseSource(url=SOURCE_URL,
                                       code=ref_file.split(".")[0],
                                       path=ref_file,
                                       long_name=f"{AREA} {PROVIDER} "
                                       f"Oil&Condensate, Gas amd Refinery Daily Production - {current_date}",
                                       meta_data={'content': self.current_raw_data}
                                       ))

        if self.full_load:
            # Load the history... based on existing files in the filestore
            file_list = glob.glob(os.path.join(FILE_STORE_PATH, f"{JOB_CODE}_{FILE_PREFIX}_*"))

            for file in file_list:
                file_basename = os.path.basename(file)
                # check if it is not the current date file
                if file_basename == self.sources[0].path:
                    logger.debug(f"Full load: skipping existing current date file to avoid duplicates: {file_basename}")
                    break

                file_date = file_basename.split(".")[0].split('_')[-1]
                self.sources.append(
                    BaseSource(url=SOURCE_URL,
                               code=file_basename.split(".")[0],
                               path=file_basename,
                               long_name=f"{AREA} {PROVIDER} "
                               f"Oil&Condensate, Gas amd Refinery Daily Production - {file_date}"
                               ))

        # add dictionary to dynamic dims
        for source in self.sources:
            copied_source = copy(source)

            # exclude metadata as there is no need to write it to the database
            if hasattr(copied_source, 'meta_data'):
                delattr(copied_source, 'meta_data')

            dicto = vars(copied_source)
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    @staticmethod
    def download_source(source):
        """
        As we 'download' the content at the constructor, we have to override this method
        to get the content from the source meta_data dictionary ('content' entry).
        As there are no history online, it will process only the current date source.
        """
        if hasattr(source, 'meta_data') and 'content' in source.meta_data:
            try:
                try:
                    meta_data, path, url = source.meta_data, source.path, source.url
                except AttributeError as e:
                    raise AttributeError(f"Missing an essential source attribute: {e}")

                content = str(meta_data['content']).encode(ENCODING)
                with open(os.path.join(FILE_STORE_PATH, path), "wb") as fp:
                    fp.write(content)
                    setattr(source, 'last_download', datetime.datetime.now().strftime("%x %X"))
            except Exception as e:
                del source
                logger.exception(f"Issue: {e}")

    @staticmethod
    def __get_data_from_source(source):
        """
        Transform each downloaded source file.
        :param source: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """
        logger.debug(f'Getting data from {source.path}')
        full_path = os.path.join(FILE_STORE_PATH, source.path)
        final_df = []

        try:
            results = dict()
            with open(full_path, "r") as f:
                content = f.read()
                soup = BeautifulSoup(content, 'html.parser')

                # get date
                ref_date = _get_date(soup)

                # get values
                for data in soup.find_all('li'):
                    title = data.find('div', class_='bl-areas-text').get_text()
                    value = data.find('div', class_='bl-areas-value').get_text()
                    results[title] = value

                logger.debug(f'Data read from page: {results}')

            # generate data frames
            for product, value in results.items():
                df = pd.DataFrame([ROW_DETAILS[product]])
                df['period'] = ref_date
                df['value'] = float(value)
                df['source'] = source.code
                final_df.append(df)

        except ValueError as e:
            if str(e) == 'No data found':
                logger.warn(f"Error while reading source {source.path}: {str(e)}. Ignoring this source.")
                df = None
            else:
                raise e
        return pd.concat(final_df)

    def transform(self):
        """
        This method must:
        - transform provider
        - get data from sources
        - transform data (3 rows per file)
        :return: data frame containing the source rows
        """
        logger.debug("Transforming data ...")
        self.__transform_provider()

        self.data = []
        logger.debug("Reading data from files in parallel ...")
        dfs = parallelize(self.__get_data_from_source, self.sources, job.MAX_WORKER)
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                self.__transform_data(df)
            except ValueError as e:
                logger.warn(f"Error while concatenating data frames, not transforming data: {e}")
        return None

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Transforming provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = PROVIDER_LONG_NAME
        provider["url"] = SOURCE_URL

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    def __transform_data(self, df):
        """
        Transform the data frame and calculate entity dimension.
        :param df: data frame containing data from all files
        :return: None
        """
        logger.debug("Transforming data frame")

        # convert to KBBL
        df = df.apply(convert_df_units, axis=1)

        # remove duplicates
        df.sort_values(by='period', ascending=False, inplace=True)
        df.drop_duplicates(subset=['flow', 'product', 'unit', 'period', 'value'], keep='first', inplace=True)

        # add static columns
        df = df.assign(provider=PROVIDER,
                       area=AREA,
                       frequency=FREQUENCY,
                       original=ORIGINAL)

        # load results into self.data
        self.data.extend(df.to_dict('records'))


def _get_date(content):
    """
    Auxiliary method to get date from current website content.
    :return: string with the current date in 'yyyy-mm-dd' format.
    """
    title = content.find('div', class_='bl-areas-title').get_text()

    if TITLE not in title:
        raise ValueError(f"Title with date not found in the website {SOURCE_URL} : {title}")

    # extract date
    date = re.search(DATE_REGEX, title).group()

    if len(date) != 10:
        raise ValueError(f"Error parsing date from website {SOURCE_URL}")

    # converts from 'dd.mm.yyyy' to 'yyyy-m-dd'
    return '-'.join(date.split('.')[::-1])


def convert_ton_to_barrel(x):
    """
    Converts a measure in tons to barrels using Kazakhstan oil density.
    :param x: amount in tons
    :return: amount in barrels
    """
    return x * TON_TO_BBL_RATIO


def convert_df_units(x):
    """
    Converts data frame units and values from T and KT to KBBL.
    :param x: data frame row to convert
    :return: modified data frame row
    """

    if x['unit'] == 'KT':
        x['value'] = convert_ton_to_barrel(x['value'])
        x['unit'] = 'KBBL'

    return x
