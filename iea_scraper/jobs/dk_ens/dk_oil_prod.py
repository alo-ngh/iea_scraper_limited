import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict
from dateutil.relativedelta import relativedelta

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
import requests
from bs4 import BeautifulSoup

# libraries needed to change locale
import locale
import threading
from contextlib import contextmanager

from iea_scraper.core.job import ExtDbApiJobV2, MAX_WORKER
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.jobs.utils import convert_bbl_to_kbd
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)

LOCALE_LOCK = threading.Lock()


@contextmanager
def setlocale(name):
    with LOCALE_LOCK:
        saved = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL, name)
        finally:
            locale.setlocale(locale.LC_ALL, saved)


class DkOilProdJob(ExtDbApiJobV2):
    """
    Danish monthly oil production.
    The web page contains also gas and water production, but this process extract only oil data.
    As for now, available history online goes back to Jan 2018.
    """
    title: str = "Denmark - Monthly Oil Production"

    job_code = Path(__file__).parent.parts[-1]
    provider_code: str = job_code.upper()
    provider_long_name: str = "Danish Energy Agency"
    provider_url: str = "https://ens.dk/en"

    online_sources_url: str = "https://ens.dk/en/our-services/oil-and-gas-related-data/monthly-and-yearly-production"
    base_url: str = "https://ens.dk"

    sources_pattern = "ofu.htm"
    period_pattern = "%b %Y"

    encoding = 'ISO-8859-1'

    dirt_hist_start = date(2016, 1, 1)
    dirt_hist_end = date(2017, 2, 1)
    period_decimal_sep_comma = date(2020, 1, 1)

    # First period with data in history
    start_period = date(2011, 6, 1)
    # First period available online (this is updated by the process later on)
    start_download = date(2018, 1, 1)
    # Number of periods to load if full_load = False
    nb_periods = 3

    area = "DENMARK"
    unit = 'KBD'
    flow = 'SUPPLY'
    product = 'CRUDEOIL'
    frequency = 'Monthly'
    original = True

    locale_dansk = 'dansk'
    
    @classmethod
    def _get_online_sources(cls) -> List[Dict[str, object]]:
        """
        Obtains the list of source files available online from
        https://ens.dk/en/our-services/oil-and-gas-related-data/monthly-and-yearly-production.
        We consider only sources in oil field units.
        :return: array of dictionaries with 2 values: period, url sorted by period in descending order
        """
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        logger.info(f'Getting list of online sources from {cls.online_sources_url}')
        response = session.get(cls.online_sources_url)
        # if response not OK, raises exception
        response.raise_for_status()

        logger.debug(f'Parsing online sources (anchors with href containing {cls.sources_pattern})')
        bf = BeautifulSoup(response.content, 'lxml')
        source_list = bf.find_all('a', href=re.compile(cls.sources_pattern))
        formatted_list = [{'period': cls.__parse_date(s.text),
                           'url': s['href']} for s in source_list]
        formatted_list.sort(key=lambda x: x['period'], reverse=True)
        logger.info(f'{len(formatted_list)} online sources found in the website.')
        return formatted_list

    @classmethod
    def __parse_date(cls, date_text: str) -> date:
        """
        Auxiliary function to parse the date from text.
        Date is expected to be like 'January 2020', but we are cutting to 'Jan 2020'
        to be more tolerant to misspelling errors in month names in the site.
        :return: a date
        """
        month, year = date_text.split()
        str_to_parse = f'{month[:3]} {year}'
        parsed_date = None
        try:
            parsed_date = datetime.strptime(f"{str_to_parse}", cls.period_pattern).date()
        except ValueError as e:
            logger.warning(f'Error trying to parse date with current locale. Trying dansk locale...')
            with setlocale(cls.locale_dansk):
                parsed_date = datetime.strptime(f"{str_to_parse}", cls.period_pattern).date()
        return parsed_date

    def get_sources(self):
        """
        Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        :return:
        """
        logger.debug("Generating sources...")
        # get most recent period available online in the website (for now, ignore the URL)
        online_sources: List[Dict[str, object]] = self._get_online_sources()

        sources = []

        if not self.full_load:
            sources = online_sources[:self.nb_periods]
            periods_to_load = [s['period'] for s in sources]
            logger.info(f"Periods to load: {periods_to_load}")

        else:
            logger.info(f"Loading all online periods plus those available offline")
            first_source_online = min(online_sources, key=lambda p: p['period'])
            start_download = first_source_online['period']
            logger.debug(f'First period online: {start_download}')
            last_offline_period = start_download + relativedelta(months=-1)
            logger.debug(f'Last period offline: {last_offline_period}')

            offline_sources = [{'period': p.date(),
                                'url': '(local)'}
                               for p in reversed(pd.date_range(self.start_period, last_offline_period, freq='MS'))]
            sources = online_sources
            sources.extend(offline_sources)

        for source in sources:
            str_period = source['period'].strftime('%Y%m')
            code = f"{self.job_code}_{self.product.lower()}_{str_period}"
            url = f"{self.base_url}{source['url']}"
            path = f"{code}.html"
            long_name = f"{self.area} {self.provider_code} Oil Monthly Production by Field - {str_period}"
            source = BaseSource(url=url,
                                code=code,
                                path=path,
                                long_name=long_name)
            self.sources.append(source)

    @classmethod
    def download_source(cls, source):
        """
        Overrrides super method to ensure periods before 200801 are not downloaded.
        This had to become a class method to be able to call parent's download_source() (which is static method).

        :param source: the source object describing the object
        :return:
        """
        period = source.code.split('_')[-1]
        date_period = date(int(period[0:4]), int(period[-2:]), 1)

        if date_period >= cls.start_download:
            logger.debug("calling super(cls, cls).download_source(source)")
            super(cls, cls).download_source(source)
        else:
            logger.debug(f"Source date before {cls.start_download}: skipping download.")

    @classmethod
    def __get_data_from_source(cls, source):
        """
        Transform each downloaded source file.
        :param source: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """
        logger.debug(f'Getting data from {source.path}')
        table = None
        full_path = FILE_STORE_PATH / source.path

        period = source.code.split('_')[-1]
        dt_period = date(int(period[0:4]), int(period[-2:]), 1)
        logger.debug(f"calculated period for source {source.code}: {dt_period}")
        try:
            # consider only the first HTML table fund (so the index '[0]')
            if not full_path.exists():
                logger.warning(f"Source {source.url} or file {str(full_path)} not available.")
                source.checksum = None
                return None
            # beware of dirty history between jan 2016 and feb 2017
            # dirty history is loaded without pandas.read_html options
            pandas_params = {'io': str(full_path)}

            if dt_period < cls.dirt_hist_start or cls.dirt_hist_end < dt_period <= cls.period_decimal_sep_comma:
                # before jan 2016 and between mar 2017 and jan 2020, uses this notation
                pandas_params['thousands'] = '.'
                pandas_params['decimal'] = ','
            elif dt_period > cls.period_decimal_sep_comma:
                # from feb 2020, use this notation and encoding
                pandas_params['thousands'] = ' '
                pandas_params['decimal'] = '.'
                pandas_params['encoding'] = cls.encoding

            logger.debug(f"Period: {dt_period}, pandas_html params: {pandas_params}")
            df = pd.read_html(**pandas_params)[0]
            logger.debug(f'Number of rows read for {dt_period}: {len(df)}')
            # some data cleansing
            df.replace(to_replace='I alt', value='Total', inplace=True)
            df.replace(to_replace='Ialt', value='Total', inplace=True)
            df.replace(to_replace='Tyra  SE', value='Tyra SE', inplace=True)
            df.replace(to_replace='Syd  Arne', value='Syd Arne', inplace=True)

            # oil data is the first table
            field_text_indexes = df[df.apply(lambda r: r.str.contains('Field', case=True).any(), axis=1)].index
            if len(field_text_indexes) == 0:
                raise Exception(f"Column name 'Field' not found for period {dt_period}")
            oil_table_start = field_text_indexes[0]
            # find breaks: all columns null is a break between tables
            table = df[oil_table_start:]
            break_indexes = table[df.apply(lambda r: r.str.contains('Total', case=True).any(), axis=1)].index
            if len(break_indexes) == 0:
                logger.debug(f"Column name 'Total' not found in period {dt_period}, estimating end of table.")
                break_indexes = [len(table) + 1]

            # subtract 1 from break_indexes[0] to exclude Total (if needed)
            df.columns = df.loc[oil_table_start].values
            oil_table_end = break_indexes[0]
            table = df[oil_table_start + 1:oil_table_end]
            logger.debug(f'Number of rows in the oil table for period {dt_period}  s: {oil_table_start} '
                         f'e: {oil_table_end}: {len(table)}')
            if len(table) == 0:
                logger.debug(f"Zero table: {df}")
            table = table[['Field', 'Monthly']]
            table['Monthly'].replace('-', "0", inplace=True)
            # rename columns and add transformations (barrels to kbd)
            table.rename(columns={'Monthly': 'value'}, inplace=True)
            table['value'] = table['value'].astype(float)
            table = table.assign(source=source.code)
            table['period'] = dt_period.strftime("%b%Y").upper()
            # auxiliary columns for kbd calculations, it will be deleted later on
            table['month'] = dt_period.month
            table['year'] = dt_period.year
        except ValueError as e:
            if str(e) == 'No tables found':
                logger.warning(f"Error while reading source {source.path}: {str(e)}. Ignoring this source.")
                table = None
            else:
                raise e
        return table

    def __transform_data(self, df):
        """
        Transform the data frame and calculate entity dimension.
        :param df: data frame containing data from all files
        :return: None
        """
        logger.debug("Transforming data frame")
        df['entity'] = df.apply(lambda x: f"{self.provider_code}_{x['Field'].upper()}", axis=1)
        self.__transform_entity(df)
        # convert measures from kbbl to kbd
        df['value'] = df.apply(lambda x: convert_bbl_to_kbd(x['value'] * 1000, x['year'], x['month']), axis=1)
        df = (df.drop(columns=['Field', 'year', 'month']).
              assign(area=self.area).
              assign(frequency=self.frequency).
              assign(provider=self.provider_code).
              assign(product=self.product).
              assign(unit=self.unit).
              assign(flow=self.flow).
              assign(original=self.original))
        logger.debug(f"Number of rows transformed: {len(df)}")
        self.data = df.to_dict('records')

    def __transform_entity(self, df):
        """
        Deduplicate entity, exclude existing and then load them into self.dynamic_dim.
        :param df: the data frame with the data.
        :return: None.
        """
        entity = df[['entity', 'Field']].drop_duplicates() \
            .rename(columns={'entity': 'code'})
        entity['long_name'] = entity.apply(lambda x: f"{self.provider_code} {x['Field'].upper()}", axis=1)
        entity['category'] = 'field'
        entity.drop(columns=['Field'])

        #   export entity to dictionary
        entity_dict = entity.to_dict('records')
        #   add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity_dict
        # load it!
        self.remove_existing_dynamic_dim('entity')

    def transform(self):
        """
         Should create:
         *  In self.dynamic_dim_dfs a dictionary of the element of the dynamic
            dimensions to be inserted with the API (the key being the name of the dim)
            ex: {'source': [{'code': 'code1', 'url': 'url1', ...},
                            {'code': 'code2', ... }, ...],
                 'entity': [{'code': 'code1', 'category': 'category1', ...},
                            {'code': 'code2', ... }, ...],
                 ...}
         *  In self.data the data to be uploaded to upserted with the API
         """
        logger.debug("Transforming data ...")

        self.data = []
        logger.debug("Reading data from files in parallel ...")
        dfs = parallelize(self.__get_data_from_source, self.sources, MAX_WORKER)

        # ignore Nones
        dfs = [df for df in dfs if df is not None]

        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                self.__transform_data(df)
            except ValueError as e:
                logger.warn(f"Error while concatenating data frames, not transforming data: {e}")
        return None
