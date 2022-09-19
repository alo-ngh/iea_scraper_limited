import os
import zipfile
from calendar import month_abbr
from datetime import date

import pandas as pd
from dateutil.relativedelta import relativedelta

from iea_scraper.core.job import MAX_WORKER
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.jobs.utils import convert_bbl_to_kbd
from iea_scraper.jobs.gov_bsee.base_gov_bsee import BaseGovBseeJob
from iea_scraper.settings import FILE_STORE_PATH

import logging

logger = logging.getLogger(__name__)


class OgorAJob(BaseGovBseeJob):
    """
    Class for loading US's Gulf of Mexico production data.

    Field data comes from this web page, button 'CSV' (https://www.data.bsee.gov/Other/DataTables/DeepQualFields.aspx).
    Production data comes from the links on this page (https://www.data.bsee.gov/Main/OGOR-A.aspx).
    """
    title: str = "USA - Gulf of Mexico (OGOR-A) Monthly Production"

    frequency = 'Monthly'
    unit = 'KBD'
    flow = 'SUPPLY'
    product = 'CRUDEOIL'
    original = True
    environment = "offshore"

    # First year with data
    start_date = 1996
    # Publication delay in months
    publication_delay = 3

    base_url = "https://www.data.bsee.gov/Production/Files/"

    # For header def see: https://www.data.bsee.gov/Main/HtmlPage.aspx?page=pacogorA
    file_columns = ['LEASE_NUMBER', 'COMPLETION_NAME', 'PRODUCTION_DATE',
                    'DAYS_ON_PROD', 'PRODUCT_CODE', 'MON_O_PROD_VOL',
                    'MON_G_PROD_VOL', 'MON_WTR_PROD_VOL', 'API_WELL_NUMBER',
                    'WELL_STAT_CD', 'AREA_CODE', 'OPERATOR_NUM', 'SORT_NAME',
                    'FIELD_NAME_CODE', 'INJECTION_VOLUME', 'PROD_INTERVAL_CD',
                    'FIRST_PROD_DATE', 'UNIT_AGT_NUMBER', 'BUG']

    def get_sources(self):
        """
        Generates the source file names to load:
        - the current year file (mask ogoradelimit.zip)
        - if full_load is True, the history from START_DATE constant (mask ogora{year}delimit.zip)
        """
        logger.debug("Generating source for current period.")
        # cur_year = (datetime.datetime.now() - datetime.timedelta(days=90)).year
        cur_year = (date.today().replace(day=1) - relativedelta(months=self.publication_delay)).year

        source = BaseSource(url=f"{self.base_url}ogoradelimit.zip",
                            code=f"{self.job_code}_ogora_{cur_year}",
                            path=f"{self.job_code}_ogora_{cur_year}.zip",
                            long_name=f"{self.area} {self.provider_code} OGOR-A Monthly Production by Field {cur_year}")
        # append to self.sources
        self.sources.append(source)

        if self.full_load:
            logger.debug("Generating source for history.")
            for year in reversed(range(self.start_date, cur_year)):

                source = BaseSource(url=f"{self.base_url}ogora{year}delimit.zip",
                                    code=f"{self.job_code}_ogora_{year}",
                                    path=f"{self.job_code}_ogora_{year}.zip",
                                    long_name=f"{self.area} {self.provider_code} "
                                    f"OGOR-A Monthly Production by Field {year}")
                self.sources.append(source)

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
            logger.debug("Concatenating results ...")
            df = pd.concat(dfs)
            self.__transform_data(df)

    @classmethod
    def __get_data_from_source(cls, source):
        """
        Get the base data frame from  the binary zipfile.
        :param source: the source object defining the source file to read.
        """
        logger.debug(f"Opening file {source.path}")
        full_path = os.path.join(FILE_STORE_PATH, source.path)
        with zipfile.ZipFile(full_path) as data_zip:
            for file in data_zip.namelist():
                logger.debug(f"Reading file {file}")
                with data_zip.open(file) as txt:
                    df = pd.read_csv(txt, header=None, names=cls.file_columns)

        df = df[["PRODUCTION_DATE", "FIELD_NAME_CODE", "MON_O_PROD_VOL"]]
        df = df.groupby(["PRODUCTION_DATE", "FIELD_NAME_CODE"],
                        as_index=False).sum()
        df['source'] = source.code
        return df

    def __transform_data(self, df):
        """
        Apply the transformations on the data frame and load it into self.data as a list of dictionaries.
        :param df: data frame to process.
        """
        logger.debug(f"Transforming data ({str(len(df.index))} rows)")
        df.dropna(inplace=True)

        if len(df) > 0:
            # process entitities
            self.__transform_entity(df)

            # process fact data
            df['month'] = df['PRODUCTION_DATE'] % 100
            df['year'] = df['PRODUCTION_DATE'] // 100
            df["period"] = df.apply(lambda x: f"{month_abbr[x.month].upper()}{str(x.year)}", axis=1)
            df['value'] = df.apply(lambda x: convert_bbl_to_kbd(x['MON_O_PROD_VOL'],
                                                                x['year'],
                                                                x['month']), axis=1)

            df = df.drop(columns=["PRODUCTION_DATE", "MON_O_PROD_VOL", "month", "year"]) \
                   .rename(columns={"FIELD_NAME_CODE": "entity"}) \
                   .assign(provider=self.provider_code,
                           frequency=self.frequency,
                           area=self.area,
                           flow=self.flow,
                           unit=self.unit,
                           product=self.product,
                           original=self.original)

            # load results into self.data
            logger.info(f"Number of transformed rows: {str(len(df.index))}")
            self.data.extend(df.to_dict('records'))

    def __transform_entity(self, df):
        """
        Deduplicate entity, exclude existing and then load them into self.dynamic_dim.
        :param df: the data frame with the data.
        :return: None.
        """
        entity_df = df[['FIELD_NAME_CODE']].drop_duplicates()\
                                           .rename(columns={"FIELD_NAME_CODE": "code"})\
                                           .assign(category="field")

        entity_df["meta_data"] = entity_df.apply(lambda x: {'environment': self.environment}, axis=1)
        entity_df["long_name"] = self.provider_code + " - " + entity_df["code"]

        logger.debug(f'Number of entities detected: {entity_df["code"].count()}')

        # add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity_df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')
