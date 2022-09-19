import json
from operator import itemgetter
import pandas as pd
from pathlib import Path
import requests
import re
import zipfile

from iea_scraper.settings import API_END_POINT,FILE_STORE_PATH
from iea_scraper.core.job import ExtDbApiJob, BATCH_SIZE_DIM
from iea_scraper.core.source import BaseSource
from iea_scraper.core.ts import mapping
from iea_scraper.core.utils import get_dimension_db_data, batch_upload
from iea_scraper.jobs.utils import to_detail_format, convert_area_to_code

import logging

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = 'US_EIA'
ORIGINAL = True
BASE_URL = "https://api.eia.gov/bulk/"

FILES = [("International Energy Data", "INTL")]
MANIFEST_URL = "https://api.eia.gov/bulk/manifest.txt"

FREQ_MAPPING = {'A': 'Annual', 'Q': 'Quarterly', 'M': 'Monthly', 'W': 'Weekly', 'D': 'Daily'}

UNIT_MAPPING = pd.DataFrame([
    ('Thousand Btu Per USD at Purchasing Power Parities', 'MBTU', 0.001),
    ('Million Btu per Person', 'MBTU', 1),
    ('Quadrillion Btu', 'TJ', 1055055.85262),
    ('Billion Dollars at Purchasing Power Parities', 'USD', 1000000000),
    ('People in Thousands', 'PERS', 1000),
    ('million metric tons carbon dioxide', 'KT', 1000),
    ('million metric tonnes carbon dioxide', 'KT', 1000),
    ('Million Metric Tons', 'KT', 1000),
    ('Million Metric Tons of Oil Equivalent', 'KTOE', 1000),
    ('1000 metric tons', 'KT', 1),
    ('Tera Joules', 'TJ', 1),
    ('terajoules', 'TJ', 1),
    ('Percent', 'PERC', 1),
    ('Thousand Gallons per Day', 'KGD', 1),
    ('Thousand Gallons', 'KGL', 1),
    ('Dollars per Gallon', 'USD', 1),
    ('Dollars per Barrel', 'USD', 1),
    ('Number of Elements', 'COUNT', 1),
    ('Thousand Short Tons', 'KST', 1),
    ('Thousand Barrels Per Day', 'KBD', 1),
    ('Thousand Barrels', 'KBBL', 1),
    ('Millions Barrels', 'KBBL', 1000),
    ('Billion Kilowatthours', 'GWh', 1000),
    ('Billion Barrels', 'KBBL', 1000000),
    ('Billions Cubic Meters', 'MCM', 1000),
    ('Billion Cubic Meters', 'MCM', 1000),
    ('Billion Cubic Feet', 'MCF', 1000),
    ('Trillion Cubic Feet', 'MCF', 1000000),
    ('Million Short Tons', 'KST', 1000),
    ('Million Kilowatts', 'MWe', 1000)
]).rename(columns={0: 'eia_unit', 1: 'unit', 2: 'conversion_factor'}) \
    .apply(lambda x: x.str.lower() if x.name == 'eia_unit' else x)


class BulkIntlJob(ExtDbApiJob):
    """
    Class for loading EIA's bulk data file.
    This is used currently to load only INTL file.
    """
    title: str = "EIA - International data"

    mapped_dim = ('product', 'flow', 'sector')

    def get_sources(self):
        """
        Defines the source data files.
        """
        for file in FILES:
            source = {'code': f"{JOB_CODE}_{file[1]}",
                      'file': file[1],
                      'url': f"{BASE_URL}{file[1]}.zip",
                      'path': f"{JOB_CODE}_{file[1]}.zip"}
            self.sources.append(BaseSource(**source))
            source.pop('file')
            r = requests.get(
                f"{API_END_POINT}/dimension/source?code={source['code']}")
            if r.status_code == 404:
                source.update(long_name=f"{JOB_CODE}: {file[0]}")
                r = requests.post(
                    f"{API_END_POINT}/dimension/source", json=[source])

        self.source_complements.append(BaseSource(
            code=f"{JOB_CODE}_BulkManifest",
            url=MANIFEST_URL,
            path=f"{JOB_CODE}_BulkManifest.txt"))

    def transform(self):
        """
        Transforms data from the original format into External DB schema.
        """
        self.dynamic_dim["provider"] = [{'code': PROVIDER,
                                         'long_name': 'U.S Energy Information Administration',
                                         'url': 'https://www.eia.gov/'}]
        for source in self.sources:
            data = _get_data_series(source)
            self.update_details(data)
            ts_batch = 10000
            self.data = self.make_generator(data, ts_batch, source)
        self.remove_existing_dynamic_dim('provider')
        self.remove_existing_dynamic_dim('detail')

    def update_details(self, data):
        """
        Updates detail dimension.
        :param data: an array with detail data
        """
        get_details = itemgetter('series_id', 'name', 'source')
        details = [get_details(x) for x in data]
        df = pd.DataFrame(details,
                          columns='code description source'.split())
        df['mapping'] = mapping(df['description'], self.mapped_dim)
        category = f"EIA_{df['code'][0].split('.')[0]}"
        df['category'] = df['code'].map(lambda x: f"EIA_{x.split('.')[0]}")
        data = to_detail_format(df)
        self.dynamic_dim['detail'] = data
        # TODO: replace this code after changing External DB API to use merge for details
        db_details = get_dimension_db_data('detail', f"category={category}")
        get_dimension_db_data.cache_clear()
        db_details_code = [x['code'] for x in db_details]
        data_to_insert = [x for x in data if x['code'] not in db_details_code]
        data_to_update = [x for x in data if x['code'] in db_details_code]
        endpoint = f"{API_END_POINT}/dimension/detail"
        if len(data_to_insert) > 0:
            try:
                batch_upload(data_to_insert, endpoint, BATCH_SIZE_DIM)
            except OSError as e:
                return None

    def make_generator(self, data, ts_batch, source):
        """
        Creates a data generator of a batch size to avoid loading them entirely in memory.
        :param data: the data array
        :param ts_batch: the batch size
        :param source: a Source object
        :return: yelds batches of data.
        """

        logger.debug(f"Data size: {len(data)} batch size: {ts_batch}  data//batch: {len(data) // ts_batch}")

        for i in range(len(data) // ts_batch):
            logger.info(f"Starting Batch: {i}")
            batch = data[i * ts_batch: (i + 1) * ts_batch]
            logger.debug(f"Batch size: {len(batch)} batch rows: {batch[:10]}")
            df = _get_df(batch)
            logger.debug(f"df size after _get_df: {len(df)} batch rows: {df[:10]}")
            df = map_and_scale_units(df)
            logger.debug(f"df size after map_and_scale_units: {len(df)} batch rows: {df[:10]}")
            mapping_df = get_mapping_df(df, self.mapped_dim)
            df = pd.merge(df, mapping_df, how='left', left_on='detail',
                          right_on='code')
            del df['code']
            df = convert_area_to_code(df)
            df = convert_area_to_code(df, to_area=True)
            df = map_period(df)
            df['frequency'] = df['frequency'].map(FREQ_MAPPING)
            df = df[~df['frequency'].isnull()]
            df.fillna('None', inplace=True)
            df = (df.assign(provider=PROVIDER).
                assign(original=ORIGINAL).
                assign(source=source.code))
            yield from df.to_dict('records')


def _get_data_series(source):
    """
    Reads data series from file.
    :param source: a Source file object.
    :return: an array of data
    """
    path = Path(FILE_STORE_PATH, source.path)
    file = f"{source.file}.txt"
    data = []
    z = zipfile.ZipFile(path)
    with z.open(file) as f:
        for line in f:
            data.append(json.loads(line.decode()))
    data = [x for x in data if 'data' in x.keys()]
    return data


def _get_df(data):
    """
    Auxiliary function to transform the data array into a Pandas' data frame.
    :param data: the data array.
    :return: a Pandas' data frame
    """

    def data_iter(data):
        for serie in data:
            _id = serie['series_id']
            unit = serie.get('units', None)
            frequency = serie.get('f', None)
            area = serie.get('geography', None)
            to_area = serie.get('geography', None)

            # Handle State level data
            if re.match(r'USA-\w{2}$', str(area)) is not None:
                entity = area
                area = "USA"
            else: entity = None

            values = [x[1] for x in serie['data']]
            if set(values) != {0}:
                for x in serie['data']:
                    yield (_id, unit, frequency, area, to_area, entity, x[0], x[1])

    df = pd.DataFrame(data_iter(data),
                      columns=['detail', 'unit', 'frequency', 'area', 'to_area',
                               'entity', 'period', 'value'])

    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df[~df['value'].isnull()]
    return df


def map_and_scale_units(df):
    """
    Maps EIA units into IEA External DB units and apply respect conversion factor to value.
    Unknown units will be filtered and a warning will be logged.
    :param df: the original data frame
    :return: the resulting data frame with new units and values
    """
    # We start by convert all units to lowercase and then we merge with unit mappings
    df = df.apply(lambda col: col.str.lower() if col.name == 'unit' else col) \
           .merge(UNIT_MAPPING, left_on="unit", right_on="eia_unit",
                  how="left", suffixes=('', '_mapped'), indicator=True)

    # Warn if unknown units found
    not_mapped = df[df['_merge'] != 'both']
    if len(not_mapped) > 0:
        logger.warning(f"Unit not mapped! Filtering rows out: {', '.join(not_mapped['unit'].unique())}")

    # Filter out unknown units
    df = df[df['_merge'] == 'both']
    # Change unit and apply conversion factor
    df['value'] = df['value'] * df['conversion_factor']
    df = df.drop(columns=['unit', 'eia_unit', 'conversion_factor', '_merge']).rename(columns={'unit_mapped': 'unit'})

    # Filter out 'None' unit
    none_unit = df[df['unit'] == 'None']
    none_unit_size = len(none_unit)
    if none_unit_size > 0:
        logger.warning(f"Filtering out {none_unit_size} rows with unit 'None'")
    df = df[df['unit'] != 'None']
    return df


def get_mapping_df(df, mapped_dim):
    """
    Apply mappings to dimensions.
    :param df: original data frame
    :param mapped_dim: dimension to map
    :return: mapped data frame
    """
    # TODO: To be revised after changing External DB API to use merge for details
    cols = df.columns
    details = pd.unique(df['detail']).tolist()
    logger.debug(f"df size: {len(df)} details: {details}")
    category = f"EIA_{details[0].split('.')[0]}"
    db_details = get_dimension_db_data('detail', f"category={category}")
    db_details = [(x['code'], json.loads(x['json'])) for x in db_details
                  if x['code'] in details]
    db_details = [dict(code=x[0], **x[1]['mapping']) for x in db_details]
    mapping_df = pd.DataFrame(db_details)
    del db_details
    for col in mapping_df.columns:
        if col in cols:
            del mapping_df[col]

    for col in mapped_dim:
        if col not in mapping_df.columns:
            mapping_df[col] = 'None'
    return mapping_df


def map_period(df):
    """
    Maps periods.
    :param df: original data frame
    :return: data frame with period mapped
    """
    df['period'] = df['period'].map(lambda x: int(x.replace('Q', '')))
    db_periods = get_dimension_db_data('period')
    db_periods = pd.DataFrame(db_periods)[['code', 'id']]
    df = pd.merge(df, db_periods, left_on='period', right_on='id', how='left')
    del df['id'], df['period']
    df.loc[df['code'].isnull(), 'code'] = None
    df.rename(columns={'code': 'period'}, inplace=True)
    return df
