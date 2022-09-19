import itertools
import logging
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta
from selenium.common.exceptions import NoSuchElementException

import iea_scraper.jobs.utils as utilities
from iea_scraper.core.job import ExtDbApiJobV2, MAX_WORKER
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.jobs import utils
from iea_scraper.settings import FILE_STORE_PATH, PLATFORM

logger = logging.getLogger(__name__)


class BrOilProdJob(ExtDbApiJobV2):
    """
    Class for loading Brazil oil & condensate supply data.
    This version is based on the formulary available at the ANP website:
    'https://cpl.anp.gov.br/anp-cpl-web/public/sigep/consulta-producao-mensal-hidrocarbonetos/consulta.xhtml'
    """
    title: str = "Brazil Oil&Condensate Monthly Production"

    # job code: it identifies the scraper job
    job_code = Path(__file__).parent.parts[-1]

    # mandatory parameters to define provider
    provider_code: str = job_code.upper()
    provider_long_name: str = "Brazil - Agência Nacional do Petróleo"
    provider_url: str = "https://www.gov.br/anp/"

    # default file name in the source website
    default_file_name = 'Produção Mensal de Hidrocarbonetos.csv' \
        if PLATFORM == 'Windows' else '_Mensal de Hidrocarbonetos.csv'

    # source site URL
    site_url = "https://cpl.anp.gov.br/anp-cpl-web/public/sigep/consulta-producao-mensal-hidrocarbonetos/consulta.xhtml"
    # site_url = 'https://cpl.anp.gov.br/anp-cpl-web/public/inicio.xhtml'
    # file identifier
    file_prefix = 'supply'

    yearfile_start = 1941
    monthfile_start = 1973

    # assuming a delay of 2 months
    publication_delay = 2

    # post method input parameter labels
    label_from = 'txtDeOK'
    label_to = 'txtAteOK'

    # File details
    encoding = 'iso-8859-1'
    sep = ';'
    thousands_char = '.'
    decimal_char = ','

    # fixed columns
    unit = 'KBD'
    frequency = 'Monthly'
    flow = 'SUPPLY'
    area = 'BRAZIL'
    original = True

    cols_mapping = {
        'Bacia': 'basin',
        'Campo': 'field',
        'Estado': 'state',
        'Mês': 'month',
        'Ano': 'year',
        'Localização (Terra/Mar)': 'environment',
        '°API': 'api',
        'Produção de Petróleo (m³)': 'CRUDEOIL',
        'Produção de Condensado (m³)': 'COND'
    }

    env_mapping = {'TERRA': 'onshore',
                   'MAR': 'offshore',
                   'TERRA E MAR': 'onshore and offshore'}

    # parameters for waiting and downloading file from the website with selenium
    download_timeout = 30
    wait = 10
    max_attempts = 3

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.driver = utils.get_driver(headless=True)
        # self.driver = utils.get_driver(headless=True, browser=utils.BrowserType.Firefox)

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def get_sources(self):
        """Should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path' """

        # reference month: current month - self.publication_delay
        ref_month = date.today().replace(day=1) - relativedelta(months=self.publication_delay)
        period = ref_month.strftime('%Y-%m')
        ref_file = f"{self.job_code}_{self.file_prefix}_{period}.csv"

        param = ref_month.strftime('%m/%Y')

        self.sources.append(BaseSource(url=self.site_url,
                                       code=ref_file.split(".")[0],
                                       path=ref_file,
                                       long_name=f"{self.area} {self.provider_code} "
                                                 f"Oil&Condensate Monthly Production by Field {period}",
                                       meta_data={
                                           'post_parameters': {
                                               self.label_from: param,
                                               self.label_to: param
                                           }
                                       }))

        if self.full_load:
            # let's load history
            # months of current year previous to reference month
            # tuple: a list of (period, meta_data)
            tuples = []
            if ref_month.month > 1:
                tuples = [(f"{str(ref_month.year)}-{x:02d}",
                           {'post_parameters': {self.label_from: f"{x:02d}/{ref_month.year}",
                                                self.label_to: f"{x:02d}/{ref_month.year}"}
                            })
                          for x in range(1, ref_month.month)]

            # monthly years: too much data to load full year
            tuples += [(f"{str(x[0])}-{x[1]:02d}",
                        {'post_parameters': {self.label_from: f"{x[1]:02d}/{x[0]}",
                                             self.label_to: f"{x[1]:02d}/{x[0]}"}
                         })
                       for x in itertools.product(reversed(range(self.monthfile_start, ref_month.year)),
                                                  reversed(range(1, 13)))]

            # full year file
            tuples += [(str(x),
                        {'post_parameters': {self.label_from: f"01/{x}",
                                             self.label_to: f"12/{x}"}
                         })
                       for x in reversed(range(self.yearfile_start, self.monthfile_start))]

            self.sources += [BaseSource(url=self.site_url,
                                        code=f"{self.job_code}_{self.file_prefix}_{x[0]}",
                                        path=f"{self.job_code}_{self.file_prefix}_{x[0]}.csv",
                                        long_name=f"{self.area} {self.provider_code} "
                                                  f"Oil&Condensate Monthly Production by Field {x[0]}",
                                        meta_data=x[1])
                             for x in tuples]

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
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                self.__transform_data(df)
            except ValueError as e:
                logger.warning(f"Error while concatenating data frames, not transforming data: {e}")
        return None

    def download_source(self, source):
        """
        Download the file and verify checksum.
        Overrides parent method to download file with selenium.
        It passes parameters from meta_data (at key 'post_parameters').
        :param source: the file to download.
        :return: None
        """
        try:
            meta_data, path, url = source.meta_data, source.path, source.url
        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        file_path = FILE_STORE_PATH / path

        # default name of the file
        # The directory of the current download and delete if it existed previously
        default_file_path = FILE_STORE_PATH / self.default_file_name
        default_file_path.unlink(missing_ok=True)

        # get the content of the file
        self.driver.get(url)
        # allow time for website load and check for prompt to accept accessibility from the website
        time.sleep(self.wait)
        # close the text box for accessibility question
        try:
            self.driver.find_element_by_xpath("//button[@id = 'btnNaoAcessibilidade']").click()
        except NoSuchElementException as e:
            logger.debug('Accessibility button not found. Continuing...')
            pass
        # since the website fails to load occasionally we introduce a loop to allow it refresh everytime
        attempts = 0
        # refresh the page in the browser in 3 attempts
        while attempts < self.max_attempts:
            logger.debug(f'Attempt #{attempts}/{self.max_attempts} to download file {default_file_path}')
            # locate the necessary points and send the required inputs
            de_input_param = meta_data['post_parameters']['txtDeOK']
            ate_input_param = meta_data['post_parameters']['txtAteOK']

            self.driver.find_element_by_xpath('//input[@id = "frmConsulta:anoMesInicio:inputText"]').send_keys(
                de_input_param)

            self.driver.find_element_by_xpath('//input[@id = "frmConsulta:anoMesFim:inputText"]').send_keys(
                ate_input_param)

            # find consult button to return the selected dataset from the website
            self.driver.find_element_by_xpath('//button[@id = "frmConsulta:buttonConsultaQuantidade"]').click()
            # wait for some time for the csv file to load
            time.sleep(self.wait)
            # download csv file from the website

            try:
                self.driver.find_element_by_xpath("//button[@id = 'frmConsulta:buttonExportarCSV']").click()
                break
            except NoSuchElementException as e:
                logger.debug('The download button did not appear, we will refresh it and try the process again...')
                self.driver.refresh()
                logger.debug(f'Wait for {self.wait}s')
                time.sleep(self.wait)
                attempts += 1
                pass

        # wait to check when file is loaded into the filestore
        try:
            utils.wait_file(default_file_path, self.wait, self.download_timeout)
        except Exception as e:
            logger.exception(f"Could not download: file was not found in filestore after clicking download button.")
            raise e

        # rename file
        logger.debug('renaming the file to the name in the source')
        utils.rename_file(default_file_path, file_path)
        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    @classmethod
    def __get_data_from_source(cls, source):
        """
        Transform each downloaded source file.
        :param source: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """
        full_path = FILE_STORE_PATH / source.path
        logger.debug(f'Getting data from {full_path}')
        # keep only the first data frame (we expect only one table per file)
        try:
            df = pd.read_csv(str(full_path),
                             encoding=cls.encoding,
                             sep=cls.sep,
                             thousands=cls.thousands_char,
                             decimal=cls.decimal_char)
            logger.info(f'{len(df)} rows read from {source.path}.')

            # Keep only wanted fields
            df = df[list(cls.cols_mapping.keys())] \
                .rename(columns=cls.cols_mapping) \
                .assign(source=source.code) \
                .drop_duplicates()
            logger.info(f'{len(df)} rows left from {source.path} after removing duplicates.')
        except ValueError as e:
            if str(e) == 'No tables found':
                logger.warning(f"Error while reading source {source.path}: {str(e)}. Ignoring this source.")
                df = None
            else:
                raise e
        return df

    def __transform_data(self, df):
        """
        Transform the data frame and calculate entity dimension.
        :param df: data frame containing data from all files
        :return: None
        """
        logger.debug("Transforming data frame")
        # calculate period
        df['period'] = pd.to_datetime(df.month.map(str) + '/' + df.year.map(str))

        # calculate entity
        df['entity'] = df['basin'].map(str) + '_' + df['field'].str.upper()

        # transform entity dimension
        self.__transform_entity(df)

        # drop unnecessary columns for data point
        df = df[['source', 'entity', 'period', 'CRUDEOIL', 'COND']]

        # pivot measures
        df = df.melt(id_vars=['source', 'entity', 'period'],
                     value_vars=['CRUDEOIL', 'COND'],
                     var_name="product", value_name="value")
        logger.debug(f'{len(df)} rows after melting.')

        # removing zero values
        df = df[df['value'] != 0]
        logger.debug(f'{len(df)} rows after removing zero values.')

        # convert measures from m3 to kbd
        df.value = df.apply(lambda x: utilities.
                            convert_m3_to_kbd(x['value'], x['period'].year,
                                              x['period'].month), axis=1)

        # convert period to string
        df['period'] = df['period'].dt.strftime("%b%Y").str.upper()

        # replace nulls and add fixed columns for this dataset
        df = df.dropna() \
            .assign(provider=self.provider_code,
                    area=self.area,
                    frequency=self.frequency,
                    flow=self.flow,
                    unit=self.unit,
                    original=self.original)
        # load results into self.data
        self.data.extend(df.to_dict('records'))

    def __transform_entity(self, df):
        """
        Deduplicate entity, exclude existing and then load them into self.dynamic_dim.
        :param df: the data frame with the data.
        :return: None.
        """
        # prepare entity dimension
        #   drop duplicates

        logger.debug(f'Number of rows in data frame before dedup: {str(df["entity"].count())}')

        entity = df[['entity', 'basin', 'field', 'state', 'environment', 'api']] \
            .drop_duplicates(subset=['entity']) \
            .rename({'entity': 'code'}, axis='columns')

        logger.debug(f'Number of rows in data frame after dedup: {str(entity["code"].count())}')

        # translate environment
        entity['environment'] = entity['environment'].map(self.env_mapping)

        #   calculate attributes
        entity['long_name'] = self.provider_code + ' ' + entity['basin'].map(str) + ' ' + entity['field'].map(str)
        entity['category'] = 'field'
        entity["meta_data"] = entity.apply(
            lambda x: {'basin': x['basin'],
                       'field': x['field'],
                       'state': x['state'],
                       'environment': x['environment'],
                       'api': x['api']
                       },
            axis=1)

        # keep only final columns
        entity = entity[['code', 'long_name', 'category', 'meta_data']]

        logger.debug(f'Number of entities detected: {entity["code"].count()}')

        # add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity.to_dict('records')
        self.remove_existing_dynamic_dim('entity')
