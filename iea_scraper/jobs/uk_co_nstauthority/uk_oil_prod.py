import logging
import json

from calendar import month_abbr
from copy import copy
from datetime import date
from pathlib import Path
from dateutil.relativedelta import relativedelta

import pandas as pd

from iea_scraper.core.job import ExtDbApiJobV2, MAX_WORKER
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)


class UkOilProdJob(ExtDbApiJobV2):
    """
    Class for loading UK oil & condensate supply data.
    This version queries the REST service available at
    https://opendata-nstauthority.hub.arcgis.com/search?tags=Production.

    Specifically, this job loads data from this service:
    https://opendata-nstauthority.hub.arcgis.com/datasets/NSTAUTHORITY::-nsta-field-production-pprs-wgs84/explore

    ATTENTION: Though the API says OILPRODMBD and GCONDMBD are in MB/D, in reality they are in KB/D.
    """
    title: str = "UK - Monthly Oil&Condensate Production"

    job_code = Path(__file__).parent.parts[-1]

    provider_code = job_code.upper()
    provider_long_name = "North Sea Transition Authority"
    provider_url = "https://data.nstauthority.co.uk"

    frequency = 'Monthly'
    source = 'PPRS_WGS84'
    unit = 'KBD'
    area = 'UK'
    flow = 'SUPPLY'
    original = True

    year_mask = "%%%START_YEAR%%%"

    base_url = "https://data.nstauthority.co.uk" \
               "/arcgis/rest/services/Public_WGS84/UKCS_PPRS_Fields_WGS84/MapServer/0/query?" \
               "outFields=FIELDNAME,FIELDAREA,LOCATION,PERIODMNTH,PERIODYR,OILPRODMBD,GCONDMBD" \
               "&outSR=4326&f=json&returnGeometry=false" \
               f"&where=PERIODYR={year_mask}"

    # limiting job_code to 10 chars to avoid exceeding source.code size limit (30 chars)
    filename = f"{job_code[:10]}_{source.lower()}_{year_mask}.json"

    col_mapping = {"FIELDAREA": "detail",
                   "FIELDNAME": "entity",
                   "GCONDMBD": "COND",
                   "LOCATION": "environment",
                   "OILPRODMBD": "CRUDEOIL",
                   "PERIODMNTH": "month",
                   "PERIODYR": "year"}

    start_year = 1975

    publication_delay = 2

    def get_sources(self):
        """
        Generates the list of files to extract.
        """
        # get current year (considering their publication delay)
        ref_date = date.today().replace(day=1) - relativedelta(months=self.publication_delay)
        ref_year = ref_date.year
        # from 02.03.2020, loading current and previous year for full_load = False
        start_year = self.start_year if self.full_load else ref_year - 1

        for year in reversed(range(start_year, ref_year + 1)):
            logger.debug(f"Creating source for {year}")
            base_url, ref_file = [x.replace(self.year_mask, str(year)) for x in [self.base_url, self.filename]]
            long_name = f"{self.area} {self.provider_code} " \
                        f"Oil&Condensate Monthly Production by Field {year}"
            source = BaseSource(url=base_url,
                                code=ref_file.split(".")[0],
                                path=ref_file,
                                long_name=long_name)
            # append to self.sources
            self.sources.append(copy(source))

    def transform(self):
        """
        Loads the provider into dynamic_dims
        Transform all dimensions and load into dynamic_dims
        Then it loads all sources into a self.data
        """
        self.data = []
        logger.debug("Reading data from files in parallel ...")
        dfs = parallelize(self.__get_data_from_source, self.sources, MAX_WORKER)
        logger.debug(f"Concatenating results...")

        dfs = [df for df in dfs if df is not None]

        if len(dfs) > 0:
            df = pd.concat(dfs)
            self.__transform_data(df)

    @staticmethod
    def __get_data_from_source(source):
        """
        Transform each downloaded source file.
        :param source: a BaseSource instance detailing the source file.
        :return: data frame containing the source rows
        """
        logger.debug(f'Getting data from {source.path}')
        full_path = FILE_STORE_PATH / source.path

        content = None
        with open(full_path, 'r') as file:
            content = file.read()

        if not content:
            logger.warn(f"file {source.path} is empty.")
            return None

        # json content to data frame
        data = json.loads(content)['features']
        if len(data) == 0:
            logger.warn(f"No data for {source.path}.")
            return None

        data = [x['attributes'] for x in data]
        return pd.DataFrame(data)\
                 .assign(source=source.code)

    def __transform_data(self, df):
        """
        Transform a data frame and put results into self.data as a list of dictionaries.
        :param source: a BaseSource object detailing one source file.
        :return: None
        """
        logger.debug(f"Number of records in input data frame: {df.count()}")
        # rename columns
        df.rename(columns=self.col_mapping, inplace=True)

        # calculate period
        df['period'] = df.apply(lambda x: (f"{month_abbr[int(x.month)].upper()}"
                                           f"{x.year}"), axis=1)
        df.drop(columns=['year', 'month'], inplace=True)

        # transform entity
        self.__transform_entity(df)

        # pivot columns CRUDEOIL and COND as rows
        df = df.melt(id_vars=["source", "detail", "entity", "environment", "period"],
                     var_name='product')

        # drop null values
        df.dropna(subset=['value'], inplace=True)

        # drop columns
        df = df.drop(columns=['detail', 'environment'])

        # a group by to ensure we don't have duplicate keys
        df = df.groupby(['source', 'entity', 'period', 'product'], as_index=False).sum()

        # assign constant columns
        df = df.assign(area=self.area)           \
               .assign(frequency=self.frequency) \
               .assign(provider=self.provider_code)   \
               .assign(unit=self.unit)           \
               .assign(flow=self.flow)           \
               .assign(original=self.original)

        logger.debug(f"Number of records in output data frame: {df.count()}")

        self.data.extend(df.to_dict('records'))

    def __transform_entity(self, df):
        """
        Check if entities exist and add them if not.
        :param df: a data frame with UK's data.
        """
        # keep only columns needed
        ent_df = df[['entity', 'environment', 'detail']].copy()
        # replace nulls with 'None'
        ent_df.fillna(value={'detail': "None"}, inplace=True)
        # remove duplicates on entity column
        ent_df = ent_df.drop_duplicates(subset=['entity'])
        # add new columns
        ent_df['category'] = 'field'
        ent_df['long_name'] = self.provider_code + ' ' + ent_df['detail'] + '_' + ent_df['entity']
        # calculate meta_data
        ent_df["meta_data"] = ent_df.apply(
            lambda x: {'environment': x['environment'].lower(),
                       'provider': self.provider_code,
                       'region': x['detail'],
                       'field': x['entity']},
            axis=1)
        ent_df.drop(columns=['detail'], inplace=True)
        ent_df.rename(columns={'entity': 'code'}, inplace=True)
        #   export as list of dictionary to dynamic dim
        logger.debug(f"Number of identified entities: {ent_df.count()}")
        self.dynamic_dim['entity'] = ent_df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')
