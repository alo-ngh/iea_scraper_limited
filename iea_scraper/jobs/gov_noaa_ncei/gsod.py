import gc
import io
import logging
import tarfile
from datetime import date, datetime
from pathlib import Path
from typing import NoReturn, Any

import pandas as pd
import requests

from iea_scraper.core.job import ExtDbApiJobV2, MAX_WORKER
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize, calc_checksum_download, timeit
from iea_scraper.jobs.utils import split_url, ftp_fetch_file_through_http_proxy
from iea_scraper.settings import PROXY_DICT, FILE_STORE_PATH, API_END_POINT

logger = logging.getLogger(__name__)

FTP_USER: str = 'anonymous'
FTP_PASSWORD: str = 'oilmarketreport@iea.org'
PROXY_HOST: str = PROXY_DICT['http'].split('://')[1]


class GsodJob(ExtDbApiJobV2):
    """
    Scraper for Global Summary of the Day dataset from http://ncei.noaa.gov.

    GSOD front page: https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00516/html#
    Station list is available here: ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv
    """
    title: str = "NCEI - Global Summary of the Day"

    station_url: str = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'
    country_url: str = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/country-list.txt'
    measures_url: str = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive'
    list_of_files_url: str = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/'

    current_year: int = date.today().year
    default_start_year: int = 1929

    filestore_dir = Path(__file__).parent.stem
    provider = filestore_dir.upper()
    job_code = Path(__file__).stem

    provider_code = provider
    provider_long_name = "US - NOAA - National Centers for Environmental Information"
    provider_url = "https://www.ncei.noaa.gov"

    frequency = 'Daily'
    flow = 'TEMPERATURE'
    product = 'None'
    area = 'None'
    to_area = 'None'
    # Degrees Fahrenheit
    unit = 'DEGF'
    original = True

    def __init__(self,
                 start_year: int = None,
                 end_year: int = None,
                 **kwargs):
        """
        In addition to existing parent's parameters, this defines a year.
        :param start_year: int: start year to load. 1929 if not specified.
        :param end_year: int: end year to load.
        :param kwargs: parent's parameters
        """
        super().__init__(**kwargs)
        self.start_year = start_year
        self.end_year = end_year

    def get_source_file_list(self):
        """
        List existing data files on the source.
        @return: pd.DataFram: existing files from the source, with their last modified date.
        """
        logger.info(f'Reading list of source files from {self.list_of_files_url}')
        df = pd.read_html(self.list_of_files_url)[0]
        df = df[df['Name'] != 'Parent Directory'].dropna(how='all').drop(columns=['Description'])

        df['stem'] = df['Name'].map(lambda f: f"gsod_{f.split('.')[0]}")
        df['Last modified'] = pd.to_datetime(df['Last modified'], errors='coerce')
        logger.info(f'{len(df)} files detected in source.')
        return df.sort_values(by='stem', ascending=False)

    def get_existing_files_stats(self) -> pd.DataFrame:
        """
        List existing files and last modification date.
        :return: pd.DataFrame: dataframe with the results.
        """
        gsod_dir = FILE_STORE_PATH / self.filestore_dir
        logger.info(f'List existing files in {gsod_dir}')

        existing_df = pd.DataFrame([{'file_name': f.name,
                                     'stem': f.stem,
                                     'modified': datetime.fromtimestamp(f.stat().st_mtime)}
                                    for f in gsod_dir.glob('*.parquet')])

        logger.info(f'{len(existing_df)} existing files detected.')

        return existing_df.sort_values(by='stem', ascending=False)

    def detect_modified_files(self) -> list:
        """
        List new or recently modified files in source.
        @return: list containing the new or recently modified files from source.
        """
        source_df = self.get_source_file_list()
        existing_df = self.get_existing_files_stats()

        df = source_df.merge(existing_df, on='stem', how='left')
        df = df[(df['file_name'].isna()) | (df['Last modified'] > df['modified'])]
        logger.info(f'{len(df)} files new or recently modified in source.')
        # return only the year as int, to be compatible with previous approaches
        return df['Name'].map(lambda x: int(x.split('.')[0])).tolist()

    def get_sources(self) -> NoReturn:
        """
        Get the list of sources to download.
        If full_load = True, loads from start_year, otherwise, current year.
        If end_year not specified, assumes start year + 1.
        :return: NoReturn
        """
        # Stations file
        station_file = BaseSource(code=f'{self.job_code}_isd-history',
                                  long_name=f'{self.provider} weather station',
                                  url=self.station_url,
                                  path=f'{self.filestore_dir}/{self.job_code}_isd-history.csv',
                                  metadata={'type': 'station'})

        country_file = BaseSource(code=f'{self.job_code}_country-list',
                                  long_name=f'{self.provider} country list',
                                  url=self.country_url,
                                  path=f'{self.filestore_dir}/{self.job_code}_country-list.txt',
                                  metadata={'type': 'country'})

        start, end = None, None
        if not self.full_load:
            # if start_year defined, then we load a requested range of years
            if self.start_year:
                start = self.start_year
                end = (self.end_year if self.end_year else start) + 1
        else:
            # if full_load is true, then we load the whole history
            start = self.default_start_year
            end = self.current_year + 1

        if start:
            # range defined (full_load or not)
            logger.info(f'Source list based on range from {start} to {end}.')
            data_list = [BaseSource(code=f'{self.job_code}_{year}',
                                    long_name=f'{self.provider} {year} weather data',
                                    url=f'{self.measures_url}/{year}.tar.gz',
                                    path=f'{self.filestore_dir}/{self.job_code}_{year}.parquet',
                                    metadata={'type': 'data', 'year': year})
                         for year in range(start, end)]
        else:
            # if no range defined, then we list all new or recently modified files
            logger.info(f'Source list based on new or recently modified files in source.')
            data_list = [BaseSource(code=f'{self.job_code}_{year}',
                                    long_name=f'{self.provider} {year} weather data',
                                    url=f'{self.measures_url}/{year}.tar.gz',
                                    path=f'{self.filestore_dir}/{self.job_code}_{year}.parquet',
                                    metadata={'type': 'data', 'year': year})
                         for year in self.detect_modified_files()]

        self.sources.extend(data_list)
        # Careful: FTP sources MUST be downloaded sequentially AFTER HTTP sources.
        # The reason: we had to patch urllib3 used by requests.get() to correctly download FTP files
        # through the proxy.
        # But after the patch, HTTP downloads through proxy return 400.
        self.sources.append(station_file)
        self.sources.append(country_file)

    @timeit
    def download_and_get_checksum(self, download=True, parallel_download=True):
        """
        *** Overriding parent just to force using this version of download_source()
        This function downloads all files listed in self.sources.
        Default behaviour here is to download sequentially to avoid errors (not the case in the parent class).

        :param download: Flag determining whether the file should be downloaded or not. Default is True.
        :param parallel_download: Flag determining whether download should occur in parallel. Default is True.
        :return NoReturn:
        """
        logger.debug(f"Ignoring parallel_download parameter and calling super()."
                     f"download_and_get_checksum(download={download}, parallel_download={False})")
        super().download_and_get_checksum(download, False)

    def download_source(self, source, http_headers=None):
        """
        Overrides parent implementation to be able to download files through ftp
        :param source:
        :param http_headers:
        :return:
        """
        logger.debug("overridden version of download_source (GsodJob)")
        protocol, host, remote_filepath = split_url(source.url)
        target_filepath = FILE_STORE_PATH / source.path

        if 'http' in protocol:
            logger.debug(f"reading {remote_filepath} and writing directly {source.path} in parquet format.")
            r = requests.get(source.url, proxies=PROXY_DICT)
            r.raise_for_status()
            s = io.BytesIO(r.content)
            df = pd.concat(self.__read_tar_gz_to_df(s))
            df.to_parquet(target_filepath)
            logger.debug(f"parquet file {source.path} created successfully.")

        elif protocol == 'ftp':
            ftp_fetch_file_through_http_proxy(host, FTP_USER, FTP_PASSWORD, remote_filepath,
                                              PROXY_HOST,
                                              target_filepath)

        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    @staticmethod
    def __read_tar_gz_to_df(f: Any) -> pd.DataFrame:
        """
        Read directly from tar.gz without extracting to disk
        :param filename: path to tar.gz archive to read.
        """
        logger.info(f"Reading {f}")
        with tarfile.open("r:gz", fileobj=f) as file:
            for member in file.getmembers():
                # logger.debug(f"Reading {member.name}")
                yield pd.read_csv(file.extractfile(member.name), dtype={'STATION': str}, parse_dates=['DATE'])

    def transform(self):
        """
        Transform data sources and load data to dynamic_dim and data attributes.
        :return:
        """
        self.__load_country()
        self.__transform_entity()
        self.__transform_data()

    def __load_country(self) -> NoReturn:
        """
        Loads area dimension into a dataframe in self.areas.
        :return: NoReturn
        """
        # we ignore the sources as if the file changes, it disappears from the list.
        # we always need to read this file
        path = FILE_STORE_PATH / self.filestore_dir / f'{self.job_code}_country-list.txt'
        logger.info(f'Reading file {path}')
        df = pd.read_fwf(path, widths=[12, 80]).dropna(how='all')
        logger.debug(f'{len(df)} rows loaded from country list file.')
        self.country = df

    @staticmethod
    def __get_weather_station_codes() -> pd.DataFrame:
        """
        Loads entity dimension into a dataframe in self.areas.
        :return: pd.DataFrame
        """
        url = f'{API_END_POINT}/dimension/entity'
        logger.debug('loading entity dimension values')
        r = requests.get(url)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        logger.debug(f'{len(df)} rows loaded from dimension entity.')
        df = df[df['category'] == 'weather station']
        logger.debug(f'{len(df)} rows after filtering category.')
        if len(df) > 0:
            df = df[['code']]
        return df

    def __calc_entity_long_name(self, row):
        """
        Calculates the value of long_name column.
        :param row: the row.
        :return: the value of long_name
        """
        value = f"{str(row['USAF'])}"
        if row['STATION NAME'] is not None and not pd.isna(row['STATION NAME']):
            value = row['STATION NAME']
        if row['STATE'] is not None and not pd.isna(row['STATE']):
            value = f"{value}, {row['STATE']}"

        value = f"{self.provider}-{value}-{str(row['USAF']).zfill(6)}{str(row['WBAN']).zfill(5)}"
        return value

    @staticmethod
    def __calc_entity_meta_data(row):
        """
        Calculates the meta_data column. Eliminates null/nan columns.
        :param row: the current row.
        :return: the meta_data value.
        """
        col_list = ['USAF', 'WBAN', 'ICAO', 'CTRY', 'COUNTRY NAME', 'STATE', 'LAT', 'LON', 'ELEV(M)', 'BEGIN', 'END']
        return {col: row[col] for col in col_list if not pd.isna(row[col])}

    def __transform_entity(self):
        """
        Transforms the list of stations for inserting into dimension entity.
        Transformed data is inserted into self.dynamic_dims['entity'] and returned for
        helping further calculations.
        :param df: pd.DataFrame with list of stations
        :return: pd.DataFrame with the formatted data
        """
        logger.info('loading stations in entity dimension')
        list_stations = []
        station_sources = (source for source in self.sources if source.metadata['type'] == 'station')
        for source in station_sources:
            logger.debug(f'loading file {source.path}')
            source_path = FILE_STORE_PATH / source.path
            df = pd.read_csv(source_path, dtype={'USAF': str,
                                                 'WBAN': str,
                                                 'STATION NAME': str,
                                                 'CTRY': str,
                                                 'ICAO': str})
            df.drop_duplicates(inplace=True)
            logger.debug(f'{len(df)} rows loaded from file {source.path}')

            # merge country names
            df = df.merge(self.country, left_on='CTRY', right_on='FIPS ID')

            df['category'] = 'weather station'
            df['code'] = f'{self.provider}_' + df['USAF'].astype(str).str.zfill(6) + df['WBAN'].astype(str).str.zfill(5)
            df['long_name'] = df.apply(self.__calc_entity_long_name, axis='columns')
            df['meta_data'] = df.apply(self.__calc_entity_meta_data, axis='columns')

            df.drop(columns=['USAF', 'WBAN', 'STATION NAME', 'CTRY', 'STATE', 'ICAO',
                             'LAT', 'LON', 'ELEV(M)', 'BEGIN', 'END', 'FIPS ID', 'COUNTRY NAME'], inplace=True)

            list_stations.extend(df.to_dict('records'))
        self.dynamic_dim['entity'] = list_stations
        self.remove_existing_dynamic_dim('entity')

    def __transform_data(self) -> NoReturn:
        """
        Transform data from source and put results in self.data
        :return: NoReturn
        """
        # first we load station codes for lookup
        entities = [self.__get_weather_station_codes()]
        if len(self.dynamic_dim['entity']) > 0:
            entities.append(pd.DataFrame(self.dynamic_dim['entity'])[['code']])
        # reject stations not found in dimension nor in self.dynamic_dim['entity']
        df_e = pd.concat(entities).drop_duplicates()

        # now we start transforming each file
        self.data = []
        logger.info('loading data files')
        data_sources = (source for source in self.sources if source.metadata['type'] == 'data')
        for source in data_sources:
            logger.debug(f'loading {source.path}')
            source_path = FILE_STORE_PATH / source.path
            df = pd.read_parquet(source_path, columns=['DATE', 'STATION', 'NAME', 'TEMP']) \
                .assign(source=source.code)
            logger.debug(f'{len(df)} rows loaded from {source.path}')
            # ok provider, flow, area, source, product, entity, to_area, frequency, period, unit, sector, detail
            df['period'] = df['DATE'].dt.strftime('%Y-%m-%d')
            del df['DATE']

            df['entity'] = self.provider + '_' + df['STATION'].astype(str).str.zfill(11)
            del df['STATION']
            del df['NAME']

            # replace nulls and add fixed columns for this dataset
            df = df.dropna() \
                .rename(columns={'TEMP': 'value'}) \
                .assign(provider=self.provider,
                        flow=self.flow,
                        product=self.product,
                        area=self.area,
                        to_area=self.to_area,
                        frequency=self.frequency,
                        unit=self.unit,
                        original=self.original)

            df = df.merge(df_e, left_on='entity', right_on='code', how='left', indicator=True)

            df_rej = df[df['_merge'] == 'left_only'].copy()
            nb_rej = len(df_rej)
            if nb_rej > 0:
                rej_path = FILE_STORE_PATH / f'{source.code}_rejected.csv'
                logger.warning(f'{nb_rej} rows in data with entity unknown. Rejected to {rej_path}')
                df_rej.drop(columns=['code', '_merge'], inplace=True)
                df_rej.to_csv(rej_path, index=False)

                df = df[df['_merge'] == 'both']
                logger.warning(f'{len(df)} rows left in data after filtering unknown entities (weather stations)')

            df = df.drop(columns=['code', '_merge'])

            # load results into self.data
            self.data.extend(df.to_dict('records'))

            del df, df_rej
            logger.debug('calling garbage collect explicitly')
            gc.collect()
