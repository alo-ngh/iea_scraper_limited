import base64
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Set, Iterator
from pathlib import Path
import pandas as pd

from iea_scraper.core import job
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize
import requests
from iea_scraper.settings import FILE_STORE_PATH, API_END_POINT, KPLER_USERNAME, KPLER_PASSWORD

import logging

logger = logging.getLogger(__name__)

# URL building constants
BASE_URL: str = "https://api.kpler.com"

# The *_ID constants are used to replace real values when building queries
START_DATE_ID: str = "@@@START_DATE@@@"
END_DATE_ID: str = "@@@END_DATE@@@"
BASE_REQUEST: str = "/v1/flows?" \
                    "flowDirection=Export" \
                    "&split=Destination Countries" \
                    "&granularity=monthly" \
                    f"&startDate={START_DATE_ID}" \
                    f"&endDate={END_DATE_ID}" \
                    "&unit=kbd"
FROM_ZONE_ID: str = "@@@FROM_ZONES@@@"
FROM_INST_ID: str = "@@@_FROM_INST@@@"
FROM_ZONE_COMPLEMENT: str = f"&fromZones={FROM_ZONE_ID}"
FROM_INST_COMPLEMENT: str = f"&fromInstallations={FROM_INST_ID}"

# OPEC countries (key: KPLER codes value: External DB code)
OPEC_COUNTRIES: Dict[str, str] = {
    "Algeria": "ALGERIA",
    "Angola": "ANGOLA",
    "Ecuador": "ECUADOR",
    "Equatorial Guinea": "EQGUINEA",
    "Gabon": "GABON",
    "Iraq": "IRAQ",
    "Iran": "IRAN",
    "Kuwait": "KUWAIT",
    "Libya": "LIBYA",
    "Nigeria": "NIGERIA",
    "Republic of the Congo": "CONGO",
    "Saudi Arabia": "SAUDIARABI",
    "United Arab Emirates": "UAE",
    "Venezuela": "VENEZUELA"}

# installations list: hard coded
FROM_INSTALLATIONS: Dict[str, List[str]] = {"Iraq": ["Botas Ceyhan"],
                                            "Equatorial Guinea": ["Aseng FPSO", "Sendje Ceiba FPSO", "Zafiro FPSO"],
                                            "Nigeria": ["AO Apapa",
                                                        "Abo",
                                                        # "Agbami",
                                                        "Ailsa Craig FPSO",
                                                        # "Akpo",
                                                        "Antan",
                                                        "Armada Perdana",
                                                        "Atlas Cove",
                                                        "BOP Apapa",
                                                        "Bond Energy",
                                                        "Bonga FPSO",
                                                        "Bonny Island",
                                                        "Bonny Offshore",
                                                        "Bovas Apapa",
                                                        "Brass River",
                                                        "Calabar",
                                                        "Chipet",
                                                        "Ebok FPSO",
                                                        "Egina FPSO",
                                                        "Erha FPSO",
                                                        "Escravos",
                                                        "Forcados",
                                                        "Ibafon",
                                                        "Lagos Apapa",
                                                        "Odudu FPSO",
                                                        "Okono FPSO",
                                                        "Okpoho FPSO",
                                                        "Otakikpo",
                                                        "PPMC",
                                                        "PW Apapa",
                                                        "Pennington",
                                                        "Port Harcourt",
                                                        "Qua Iboe Offshore",
                                                        "Sapele",
                                                        "Sea Eagle FPSO",
                                                        "Sendje Berge FPSO",
                                                        "Techno Oil",
                                                        "Tulja Bhavani FSO",
                                                        "Ugo Ocha FSO",
                                                        "Usan FPSO",
                                                        "WRPC",
                                                        "Warri Tank Farm",
                                                        "Yoho FPSO"]}


class SmartDict(dict):
    """
    A dictionary that returns the key as value when key is not in dict.
    Useful for mapping values in data frame.
    """

    def __missing__(self, key):
        """
        This defines the value to return when a key is missing.
        In this case, we return the key as default value.
        :param key: searched key
        :return: the key as value
        """
        return key


# Mapping Kpler country names to External DB (for those not matching)
COUNTRY_MAPPING: Dict[str, str] = SmartDict({"Australia": "AUSTRALI",
                                             "Cape Verde": "CABOVERDE",
                                             "Caribbean Netherlands": "BONAIRE",
                                             "Sint Eustatius": "BONAIRE",
                                             "Comoros Islands": "COMOROS",
                                             "Equatorial Guinea": "EQGUINEA",
                                             "Guinea-Bissau": "GUINEABISSAU",
                                             "Hong Kong": "HONGKONG",
                                             "Ivory Coast": "COTEIVOIRE",
                                             "Republic of the Congo": "CONGO",
                                             "Saudi Arabia": "SAUDIARABI",
                                             "Singapore Republic": "SINGAPORE",
                                             "Solomon Islands": "SOLOMON",
                                             "South Korea": "KOREA",
                                             "Taiwan": "TAIPEI",
                                             "United Arab Emirates": "UAE",
                                             "United States Virgin Islands": "VIRGINUS",
                                             "Unknown": "NONSPEC"})

KPLER_DT_FORMAT: str = "%Y-%m-%d"

# First period with data in history
START_HIST: date = date(2014, 1, 1)
END_HIST: date = date(2018, 12, 31)
# Publication delay in months
PUBLICATION_DELAY: int = 1

# External-DB static columns (for this job)
JOB_CODE = 'opec_exp'

FREQUENCY = 'Monthly'
PRODUCT = 'CRUDEOIL'
UNIT = 'KBD'
FLOW = 'EXPORTS'
ORIGINAL = True

# Kpler returns data as a CSV with ; as separator
FILE_DELIMITER = ';'
MAX_WORKER = 15

# URL path to External DB API to get countries
EXTDB_API_DIM_AREA = "/dimension/area"


class OpecExportsJob(job.ExtDbApiJobV2):
    """
    Extracts OPEC countries' exports by destination countries from KPLER API.
    Queries must be submitted sequentially to KPLER website.
    It was also the opportunity to play with typed python.
    Special cases:
    - "Caribbean Netherlands" figures are summed up together with "Bonaire"
    - For Iraq, extracts total for country (south exports) and again for "Botas Kirkuk Ceyhan" (north exports)
    - For Equatorial Guinea, gets only data from FPSO installations
    - For Nigeria, excludes 'Agbami' and 'Akpo' installations
    """
    title: str = "Kpler - OPEC exports by destination"

    provider_code = Path(__file__).parent.parts[-1].upper()
    provider_long_name = "KPLER"
    provider_url = "https://www.kpler.com/"

    def __init__(self, full_load: str = None) -> None:
        """"
        Constructor.
        @param full_load: True for full-load.
                          False loads latest available month (current month - PUBLICATION_DELAY).
        """
        self.full_load = full_load
        self.auth_header = self.__get_auth_header(KPLER_USERNAME, KPLER_PASSWORD)
        self.country_list = self.__get_country_list()
        super().__init__()

    def __get_country_list(self):
        """
        Helper function to get the list of countries from External DB.
        :return: a Pandas data frame with the list of countries.
        """
        logger.debug("Getting the list of countries from External DB.")
        url: str = API_END_POINT + EXTDB_API_DIM_AREA
        result = requests.get(url)

        if result.status_code != 200:
            raise IOError(f"Unable to contact External DB at {url} to read country list.")

        return pd.DataFrame(result.json())[['code', 'iso_alpha_2', 'long_name']]

    def __get_auth_header(self, login: str, pwd: str) -> Dict[str, str]:
        """
        Calculates the authentication header.
        It must be encoded in base64.

        :param login: the user login to KPLER API
        :param pwd: the user password to KPLER API
        :return: the calculated authentication header
        """
        auth_str = f"{login}:{pwd}"
        auth_b = bytes(auth_str, 'utf-8')
        b64encode_str = base64.b64encode(auth_b)
        http_header = {"Authorization": f"Basic {b64encode_str.decode('utf-8')}"}
        return http_header

    def __get_url(self, start_date: date, end_date: date) -> Dict[str, object]:
        """
        Helper function to encapsulate URL preparation.
        :param start_date: initial date (filter)
        :param end_date: end date (filter)
        :return: a dictionary containing start_date, end_date and url with a complete URL with dates replaced
        """
        return {
            "start_date": start_date,
            "end_date": end_date,
            "url": BASE_URL + BASE_REQUEST.replace(START_DATE_ID, start_date.strftime(KPLER_DT_FORMAT))
                .replace(END_DATE_ID, end_date.strftime(KPLER_DT_FORMAT))
        }

    def __expand_url_per_complement(self,
                                    data: List[Dict[str, object]],
                                    zones: List[str],
                                    installations: Dict[str, List[str]]) \
            -> Iterator[Dict[str, object]]:
        """
        Returns an iterator returning all the time urls expanded by zones and installations.
        :param data: a list of dictionaries containing start_date, end_date, and url.
        :param zones: all the zones to get.
        :param installations: all the installations to get.
        :return: an iterator returning all the expanded URLs.
        """
        for d in data:
            # expand zones
            for zone in zones:
                yield {"country": zone,
                       "start_date": d['start_date'],
                       "end_date": d['end_date'],
                       "url": d['url'] + FROM_ZONE_COMPLEMENT.replace(FROM_ZONE_ID, zone)}
            # expand installations
            for country, inst_list in installations.items():
                if country == "Iraq":
                    yield {"country": country,
                           "entity": inst_list[0],
                           "start_date": d['start_date'],
                           "end_date": d['end_date'],
                           "url": d['url'] + FROM_INST_COMPLEMENT.replace(FROM_INST_ID, ",".join(inst_list))}
                else:
                    yield {"country": country,
                           "start_date": d['start_date'],
                           "end_date": d['end_date'],
                           "url": d['url'] + FROM_INST_COMPLEMENT.replace(FROM_INST_ID, ",".join(inst_list))}

    def __create_source(self, data: Dict[str, object]) -> BaseSource:
        """
        Helper function that creates a BaseSource object based on a dictionary.
        :param data: dictionary with information about the source
        :return: a BaseSource object.
        """
        country_list: pd.DataFrame = self.country_list

        start_year: int = data['start_date'].year
        end_year: int = data['end_date'].year
        period: str = str(start_year) + ('-' + str(end_year) if start_year != end_year else '')

        kpler_country: str = data['country']
        extdb_country: str = OPEC_COUNTRIES[data['country']]
        # get code iso from country list data frame
        iso_alpha2_country: str = country_list[country_list['code'] == extdb_country]['iso_alpha_2'].values[0]

        meta_data = {
            'start_year': start_year,
            'end_year': end_year,
            'kpler_country': kpler_country,
            'extdb_country': extdb_country
        }
        # special case for Iraq that has one full line and another for installations
        code: str
        if 'entity' not in data:
            code = f"{JOB_CODE}_{iso_alpha2_country}_{period}"
            long_name = f"{self.provider_code} {extdb_country} Exports by Destination {period}"
        else:
            code = f"{JOB_CODE}_{iso_alpha2_country}_inst_{period}"
            long_name = f"{self.provider_code} {extdb_country} Exports from {data['entity']} by Destination {period}"
            meta_data['entity'] = data['entity']

        path = f"{code}.csv"

        return BaseSource(url=data['url'],
                          code=code,
                          path=path,
                          long_name=long_name,
                          meta_data=meta_data)

    def get_sources(self):
        """
        Defines the list of data sources to extract.
        It should create a list of object BaseSource in self.sources with at least
        3 attributes: 'url', 'code', 'path'
        It creates one file per:
        - period (history: 2014-2018, then 1 file per year)
        - zone (14 OPEC countries + 1 additional data for Iraq)
        :return: None
        """
        logger.debug("Generating sources...")
        current_period = date.today().replace(day=1) - relativedelta(months=PUBLICATION_DELAY)
        current_year = current_period.year
        periods_and_urls: List[Dict[str, object]] = []

        if self.full_load:
            #  generate base url for the history (2014-2018)
            periods_and_urls.append(self.__get_url(START_HIST, END_HIST))

            # generate base url from years after history (2018 + 1) to current
            period_and_url = [self.__get_url(date(year, 1, 1), date(year, 12, 31))
                              for year in range(END_HIST.year + 1, current_year)]
            periods_and_urls.extend(period_and_url)

        # add url for current year
        periods_and_urls.append(self.__get_url(date(current_year, 1, 1), date(current_year, 12, 31)))
        logger.debug(f"List of base URLs: {periods_and_urls}")

        # expand period urls per zone, installations
        # For countries listed in Installations, we get only by installations
        # except for Iraq, that we get both
        zones: Set[str] = set(OPEC_COUNTRIES) - set(FROM_INSTALLATIONS)
        zones.add("Iraq")
        zones_list = sorted(zones)
        logger.debug(f"Zones to get: {zones_list}")

        self.sources = [self.__create_source(d)
                        for d in self.__expand_url_per_complement(periods_and_urls, zones_list, FROM_INSTALLATIONS)]

    def download_and_get_checksum(self, download=True, parallel_download=False):
        """
        Overrides super() to ensure that it runs sequentially.
        :param download: True for downloading the file.
        :param parallel_download: True for downloading in parallel.
        :return:
        """
        super().download_and_get_checksum(download, parallel_download=False)

    def download_source(self, source: BaseSource):
        """
        Overrrides super method to be able to pass the http_header for authentication.

        :param source: the source object describing the object
        :return:
        """
        logger.debug(f"Downloading {source.code}")
        super().download_source(source, http_headers=self.auth_header)

    def __get_data_from_source(self, source: BaseSource) -> pd.DataFrame:
        """
        Helper function that reads file content from one data source.
        It already performs some transformations for convenience:
        - renames first column to period
        - unpivot data to have to_area and value
        - adds column source, area, entity

        :param source: a BaseSource object.
        :return: a Pandas' data frame with data from the source.
        """
        logger.debug(f'Getting data from {source.path}')
        full_path = FILE_STORE_PATH / source.path
        df = pd.read_csv(full_path, delimiter=FILE_DELIMITER)\
               .drop(columns=['Period End Date'])\
               .rename(columns={'Date': 'period'}) \
               .melt(id_vars=['period'], var_name=['to_area']).query('value > 0') \
               .assign(source=source.code,
                       area=source.meta_data['extdb_country'],
                       entity=source.meta_data['extdb_country']
                              + '_'
                              + source.meta_data['entity'].upper().replace(' ', '_')
                       if 'entity' in source.meta_data else 'None',
                       long_name=source.meta_data['extdb_country']
                                 + ': '
                                 + source.meta_data['entity']
                       if 'entity' in source.meta_data else 'None')
        return df

    def __transform_entity(self, df):
        """
        Deduplicate entity, exclude existing and then load them into self.dynamic_dim.
        :param df: the data frame with the data.
        :return: None.
        """
        logger.debug('Transforming entity.')
        entity = df[['entity', 'long_name']].drop_duplicates().query("entity != 'None'")
        entity.rename(columns={'entity': 'code'}, inplace=True)
        entity['category'] = 'pipeline'

        logger.debug(f"Number of entities: {entity.size}")

        #   export entity to dictionary
        entity_dict = entity.to_dict('records')
        #   add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity_dict
        # load it!
        self.remove_existing_dynamic_dim('entity')

    def __transform_data(self, df: pd.DataFrame):
        """
        Helper function to transform data into External DB schema.
        Results are put as a list of dictionaries at self.data.
        :param df: data frame with columns period, area, value
        """
        logger.debug("Transforming data.")
        country_list = self.country_list[['code', 'long_name']]
        self.__transform_entity(df)
        df = df.drop(columns=['long_name'])

        # map to_area to extdb country values
        # 1st step: map known countries
        logger.debug("Mapping known 'to_area' to External DB countries.")
        df['to_area'] = df['to_area'].map(COUNTRY_MAPPING)

        # 2nd step: join with country list by to_area.str.upper() = country_list.code
        logger.debug("Filtering countries (to_area.str.upper()) not in External DB (country_list.code).")
        df['to_area'] = df['to_area'].str.upper()
        df_ok = df[df['to_area'].isin(country_list['code'])]

        if len(df_ok.index) < len(df.index):
            df_nok = df[~df['to_area'].isin(country_list['code'])]
            logger.debug(f"Remaining unmatched rows: {len(df_nok.index)}. Matching by long_name.")
            country_list.loc[:, 'long_name'] = country_list['long_name'].str.upper()
            df_nok_ok = df_nok[df_nok['to_area'].isin(country_list['long_name'])]

            if len(df_nok_ok.index) < len(df_nok.index):
                df_nok_nok = df_nok[~df_nok['to_area'].isin(country_list['long_name'])]
                error_msg = f"Kpler countries not found in External DB: {', '.join(df_nok_nok['to_area'].unique())}"
                logger.warning(error_msg)
                raise Exception(error_msg)

            # join by long_name to get country_list.code
            df_nok_ok = df_nok_ok.merge(country_list,
                                        how='left', left_on='to_area', right_on='long_name', indicator='_merge')
            df_nok_ok['to_area'] = df_nok_ok['code']
            # drop merge columns
            df_nok_ok.drop(columns=['code', 'long_name', '_merge'], inplace=True)
            df = pd.concat([df_ok, df_nok_ok])

        # convert period from YYYY-MM to MMMYYY
        df['period'] = pd.to_datetime(df['period'], format='%Y-%m').dt.strftime("%b%Y").str.upper()

        # aggregate values to avoid duplicates on "Bonaire" and "Caribbean Netherlands"
        logger.debug(f"Grouping by source, period, area, entity, to_area and summing up value. "
                     f"Current columns: {df.columns}. Current size: {len(df.index)}")
        df = df.groupby(['source', 'period', 'area', 'entity', 'to_area'], as_index=False).sum()

        df = (df.assign(frequency=FREQUENCY).
              assign(provider=self.provider_code).
              assign(product=PRODUCT).
              assign(unit=UNIT).
              assign(flow=FLOW).
              assign(original=ORIGINAL))
        logger.debug(f"Number of rows transformed: {len(df.index)}")
        self.data = df.to_dict('records')

    def transform(self):
        """
        Read data from each data source in self.sources and return it as a Pandas' data frame.
        :return: a Pandas' data frame.
        """
        self.data = []
        logger.debug("Reading data from files in parallel ...")
        dfs = parallelize(self.__get_data_from_source, self.sources, job.MAX_WORKER)
        if len(dfs) > 0:
            try:
                logger.debug("Concatenating results ...")
                df = pd.concat(dfs)
                self.__transform_data(df)
            except ValueError as e:
                logger.warning(f"Error while concatenating data frames, not transforming data: {e}")
                raise e
        return None
