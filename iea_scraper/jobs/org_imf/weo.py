import json
import time
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Tuple, Dict

import pandas as pd

import logging

from iea_scraper.core.job import ExtDbApiJobV2, BATCH_SIZE_DIM
from iea_scraper.core.source import BaseSource
from iea_scraper.core.ts import auto_mapping
from iea_scraper.core.utils import get_dimension_db_data, batch_upload
from iea_scraper.jobs.utils import to_detail_format, get_driver
from iea_scraper.settings import API_END_POINT, FILE_STORE_PATH

# creates a logger with id 'scraper.jobs.org_imf.job.weo'
logger = logging.getLogger(__name__)


class WeoJob(ExtDbApiJobV2):
    """
    This class tries to download the most recent data series from IMF's World Economic Outlook.
    There are 2 publications per year: in April and in October.

    The URL to files changes sometimes, so in this version, we get them with selenium.

    The file prefix also changes accordingly:

    - April: WEOApr<year>
    - October: WEOOct<year>
    """
    title: str = "IMF - World Economic Outlook"

    job_code = Path(__file__).parent.parts[-1]

    mapped_dim = ('product', 'flow', 'sector')

    provider_code = 'IMF'
    provider_long_name = 'International Monetary Fund'
    provider_url = 'https://www.imf.org/external/index.htm'

    # The follow URL lists all IMF's WEO publications
    url = r'https://www.imf.org/en/Publications/SPROLLs/world-economic-outlook-databases#sort=%40imfdate%20descending'

    # All results are enclosed by class below
    publication_class_name = 'CoveoResult'

    # possible separators used to split publication name and publication period
    publication_separators = [':', ',', 'Database']

    # period pattern: Month Year
    period_pattern = '%B %Y'
    # period short pattern: MonYear
    period_short_pattern = '%b%Y'

    # Link text for page with Entire dataset to download
    entire_dataset_link_text = 'Entire Dataset'

    # Labels of links to files we want to download
    file_link_labels = ['By Countries', 'SDMX Data Structure Definition']

    # Delay between clicks
    wait_click_delay = 10

    # Unit mappings
    unit_map = {
        'National currency': 'NC',
        'Percent change': 'PC',
        'U.S. dollars': 'USD',
        'Purchasing power parity; international dollars': 'PPP',
        'Index': 'IND',
        'Purchasing power parity; 2011 international dollar': 'PPP',
        'Percent': 'PERC',
        'National currency per current international dollar': 'NC',
        'Percent of GDP': 'PERC',
        'Percent of total labor force': 'PERC',
        'Persons': 'PERS',
        'Percent of potential GDP': 'PERC'
    }

    # file encoding
    encoding = 'ISO-8859-1'
    new_encoding = 'utf-16-le'

    # file separator
    file_separator = '\t'

    # star schema mappings
    original = True
    frequency = 'Annual'
    json_category = "IMF_WEO"

    # source complement information
    source_complement_sheet = 'CONCEPT'
    source_complement_rows_to_skip = 7

    # detail code prefix
    detail_code_prefix = "IMF_SDMX_"

    # excel engine
    excel_engine = 'openpyxl'

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.driver = get_driver()

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def get_file_links(self) -> Tuple[datetime, Dict[str, str]]:
        """
        Uses selenium to get the list of links to the files to download.
        :return: Tuple[datetime, Dict[str, str]]: period of publication as a datetime
                 and dictionary with the links to the files to download.
        """
        logger.info('Getting the links to the files to download...')
        logger.debug('Starting selenium...')
        logger.debug(f'Initial URL: {self.url}')
        self.driver.get(self.url)
        logger.debug(f'Finding publication list (tag: {self.publication_class_name})')
        latest_publication = self.driver.find_element_by_class_name(self.publication_class_name)
        a = latest_publication.find_element_by_tag_name('a')
        publication_title = a.text
        logger.debug(f'Publication title: {publication_title}')
        # extract period from string of pattern: 'publication title, Month YYYY'
        successful: bool = False
        publication_period = None
        for publication_separator in self.publication_separators:
            try:
                logger.debug(f"Extracting date from publication title with '{publication_separator}' as separator.")
                publication_period = publication_title.strip().split(publication_separator)[1].strip()
                logger.debug(f"Period successfully extracted: {publication_period}")
                successful = True
                break
            except IndexError:
                logger.debug(f'Extracting date from table with {publication_separator} failed.')
                continue
        if not successful:
            raise ValueError(f"Unable to extract publication date from '{publication_title}'.")
        publication_period_as_date = datetime.strptime(publication_period, self.period_pattern)
        a.click()
        self.wait_for_click()

        logger.debug(f'URL of the latest publication: {self.driver.current_url}')
        entire_dataset = self.driver.find_element_by_link_text(self.entire_dataset_link_text)
        entire_dataset.click()
        self.wait_for_click()

        logger.debug(f'URL of the entire dataset: {self.driver.current_url}')
        file_links = {label: self.driver.find_element_by_link_text(label).get_attribute('href')
                      for label in self.file_link_labels}
        logger.debug(f'Links retrieved from website: {file_links}')
        return publication_period_as_date, file_links

    def get_sources(self):
        """
        List the sources to process (in this case, just one).
        :param self:
        :return: NoReturn
        """
        logger.debug("Defining sources...")
        # retrieve dynamic links to files from latest publication
        publication_period, file_links = self.get_file_links()
        # There is an (intentional) error on the IMF website for the file extension
        for _, file_link in file_links.items():
            # split file stem from URL
            file_stem = file_link.split('/')[-1].split('.')[0]
            logger.debug(f'file stem from URL: {file_stem}')

            if 'all' in file_stem:
                source = BaseSource(code=f"{self.job_code}_WEO_{file_stem}",
                                    long_name=f"{self.job_code} - {file_stem}",
                                    url=file_link,
                                    path=f"{self.job_code}_WEO_{file_stem}.txt")
                logger.debug(f"Source: {vars(source)}")
                self.sources.append(source)
            else:
                sdmx = BaseSource(code=f"{self.job_code}_WEO_{file_stem}",
                                  long_name=f"{self.job_code} - {file_stem}",
                                  url=file_link,
                                  path=f"{self.job_code}_WEO_{file_stem}.xlsx")
                logger.debug(f"Source complements: {vars(sdmx)}")
                self.source_complements.append(sdmx)
        return None

    def transform(self):
        """
        Processes all the data from the source file.
        :return: NoReturn
        """
        logger.info("Starting transform()")
        self.update_details()

        for source in self.sources:
            df = self._get_data_df(source).pipe(self._scale)

            df['Units'] = df['Units'].map(self.unit_map)
            df['period'] = df['period'].astype(str)
            df = self._map_country_iso(df)
            df.rename(columns={"WEO Subject Code": "detail", "Units": "unit"},
                      inplace=True)
            df = df[["detail", "unit", "period", "value", "area"]]
            df['detail'] = df['detail'].map(self.detail_code_mapping)
            mapping_df = self.get_mapping_df(df)
            logger.debug(f'mapping_df returned columns: {mapping_df.columns}')
            df = pd.merge(df, mapping_df, how='left', left_on='detail', right_on='code')
            del df['code']
            df.fillna('None', inplace=True)
            df = (df.assign(provider=self.provider_code).
                  assign(frequency=self.frequency).
                  assign(original=self.original).
                  assign(source=source.code))
            self.data = df.to_dict('records')
        return None

    def update_details(self):
        """
        Load information from complimentary source into detail dimension.
        :return: NoReturn
        """
        logger.info('Starting update_details()')
        df = self._get_df_details()
        df['mapping'] = df['description'].map(partial(auto_mapping, self.mapped_dim))
        data = to_detail_format(df)
        self.dynamic_dim['detail'] = data
        # exceptionally, we need to update the detail dimension early because
        # we use it later on the scraper
        self.remove_existing_dynamic_dim('detail')
        endpoint = f"{API_END_POINT}/dimension/detail"
        try:
            batch_upload(self.dynamic_dim['detail'], endpoint, BATCH_SIZE_DIM)
        except OSError:
            logger.exception('Exception while updating detail dimension.')

    def _get_data_df(self, source):
        """
        This function reads the file and returns a pd.DataFrame object with its content.
        :param source: BaseSource: the source to read.
        :return: pd.DataFrame object with the file content.
        """
        logger.info("Starting _get_data_df()")
        path = FILE_STORE_PATH / source.path
        logger.debug(f'Reading {path}. Trying with encoding {self.encoding}')
        df = pd.read_csv(path, sep=self.file_separator, encoding=self.encoding)

        if df.isna().values.all():
            logger.debug(f"Unable to read it with encoding as {self.encoding}. Trying with {self.new_encoding}")
            df = pd.read_csv(path, sep=self.file_separator, encoding=self.new_encoding)

        logger.info(f'{len(df)} rows read from {path}')
        num_cols = [x for x in df.columns if x.isnumeric()]
        other_cols = [x for x in df.columns if not x.isnumeric()]
        df = df.melt(id_vars=other_cols,
                     value_vars=num_cols, var_name='period')
        logger.debug(f'After melting by numeric cols: {df.head()}')
        df['value'] = df['value'].map(lambda x: str(x).replace(',', ''))
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df.dropna(subset=['value'], inplace=True)
        return df

    def _get_df_details(self) -> pd.DataFrame:
        """
        This gets source details from complimentary donwloaded file.
        :return: pd.DataFrame containing the content of 'CONCEPT' sheet.
        """
        path = FILE_STORE_PATH / self.source_complements[0].path
        df = pd.read_excel(path,
                           sheet_name=self.source_complement_sheet,
                           usecols=[0, 1],
                           skiprows=self.source_complement_rows_to_skip,
                           engine=self.excel_engine)
        logger.info(f'{len(df)} rows read from {path.name}')
        df.columns = [x.strip().lower() for x in df.columns]
        df = df[['code', 'description']]
        df['code'] = df['code'].map(self.detail_code_mapping)
        df['category'] = self.json_category
        # Reads only the first part of the list, stops with the blanks
        df = df[df.index < 500]
        df.dropna(inplace=True)
        logger.info(f'{len(df)} rows after removing NAs.')
        return df

    @staticmethod
    def _scale(df):
        """
        Static method to convert scales.
        :param df: pd.DataFrame: data frame to convert scales.
        :return: pd.DataFrame: data frame with converted scales.
        """
        logger.info(f'Start _scale()')
        mapping_scale = {'Billions': 1000000000, 'Millions': 1000000, 'Units': 1}
        df.loc[df['Scale'].isnull(), 'Scale'] = 'Units'
        df['Scale'] = df['Scale'].map(mapping_scale)
        df['value'] = df['value'] * df['Scale']
        del df['Scale']
        return df

    @classmethod
    def detail_code_mapping(cls, x):
        """
        Adds a standard prefix to detail codes.
        :param x: the original detail code
        :return: the original code with a prefix.
        """
        return cls.detail_code_prefix + str(x)

    @classmethod
    def get_mapping_df(cls, df):
        """
        Gets the contents of detail dimension for the category defined by cls.json_category.
        :param df: the content of detail dimension for category equal to cls.json_category.
        :return: pd.
        """
        cols = df.columns
        details = pd.unique(df['detail'])
        logger.debug(f"Running get_dimension_db_data('detail', 'category={cls.json_category}')")
        db_details = get_dimension_db_data('detail', f'category={cls.json_category}')
        logger.debug(f'{len(db_details)} rows returned by get_dimension_db_data()')

        # mapping existing codes to the ones in the dimension detail
        db_details = [(x['code'], cls.map_json(x['json'])) for x in db_details
                      if x['code'] in details]
        db_details = [{'code': x[0], **x[1]['mapping']} for x in db_details]

        mapping_df = pd.DataFrame(db_details)
        for col in mapping_df.columns:
            if col in cols:
                del mapping_df[col]
        if 'product' not in mapping_df.columns:
            mapping_df['product'] = 'None'
        return mapping_df

    @staticmethod
    def map_json(s):
        """
        Converts json data into python dictionary.
        :param s: str: text with JSON data.
        :return: dict: a dictionary representing the JSON data.
        """
        json_acceptable_string = s.replace("'", "\"")
        d = json.loads(json_acceptable_string)
        return d

    @staticmethod
    def _map_country_iso(df):
        """
        Maps country codes to ISO3 codes.
        :param df: pd.DataFrame: the dataframe to convert country.
        :return:
        """
        logger.debug('Mapping countries by ISO code.')
        db_area = pd.DataFrame(get_dimension_db_data('area'))
        db_area = db_area[['iso_alpha_3', 'code']]
        df = pd.merge(df, db_area, how='inner',
                      left_on='ISO', right_on='iso_alpha_3')
        df.rename(columns={'code': 'area'}, inplace=True)
        del df['ISO'], df['iso_alpha_3']
        return df

    @classmethod
    def wait_for_click(cls):
        """
        Wait for delay seconds.
        :return: NoReturn
        """
        logger.debug(f'Wait for click: {cls.wait_click_delay}s')
        time.sleep(cls.wait_click_delay)
