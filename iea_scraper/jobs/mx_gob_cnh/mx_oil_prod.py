from pathlib import Path
import pandas as pd
import requests
import calendar
import logging
from datetime import date

from iea_scraper.core.job import ExtDbApiJob, API_END_POINT
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = 'CNH_MX'
FREQUENCY = 'Monthly'
SOURCE = 'CNH_SIH'
UNIT = 'KBD'
AREA = 'MEXICO'
FLOW = 'SUPPLY'
PRODUCT = 'CRUDEOIL'
ORIGINAL = True
BASE_URL = "https://sih.hidrocarburos.gob.mx/downloads/PRODUCCION"

ROWS_TO_SKIP = 9

DELTA_LOAD_START_YEAR = date.today().year - 1

ENVIRONMENT_MAPPING = {'terrestre': 'onshore', 'aguas someras': 'offshore'}


class MxOilProdJob(ExtDbApiJob):
    """
    This class implements the scraper for Mexican Oil production.
    """
    title: str = "Mexico - CNH Monthly Oil Production"

    def get_sources(self):
        """
        Implements method get_sources from parent class.
        It defines all data sources that have to be processed.
        :return: None
        """
        logger.info("Preparing data sources...")
        FILES = ("CAMPOS", "CUENCA")
        for file in FILES:
            source = {'code': f"{JOB_CODE}_PRODUCCION_{file}",
                      'url': f"{BASE_URL}_{file}.csv",
                      'path': f"{JOB_CODE}_PRODUCCION_{file}.csv"}

            self.sources.append(BaseSource(**source))
            r = requests.get(
                f"{API_END_POINT}/dimension/source?code={source['code']}")
            if r.status_code == 404:
                logger.debug(f"Adding source to source dimension: {source}")
                source.update(
                    long_name=f"MEXICO CNH OIL PRODUCTION {file.title()}")
                r = requests.post(
                    f"{API_END_POINT}/dimension/source", json=[source])
        return None

    def transform(self):
        """
        Implements the method transform() from parent class.
        Transform all data.
        :return: None
        """
        logger.info("Transforming data...")
        df = pd.DataFrame()
        df = pd.concat([df, self._transform_cuenca()])
        df = pd.concat([df, self._transform_campos()])
        if len(df) == 0:
            logger.warning("No data for cuenca or campos in source file.")
            return None
        df['period'] = df.period.map(_to_period_code)
        df['filter'] = df['period'].map(lambda x: int(x[-4:]))
        if self.full_load:
            logger.debug('Full load: loading the whole file.')
        else:
            logger.debug(f'Delta Load: loading data from {str(DELTA_LOAD_START_YEAR)}')
            df = df[df['filter'] >= DELTA_LOAD_START_YEAR]
        del df['filter']

        df = (df.assign(provider=PROVIDER).
              assign(frequency=FREQUENCY).
              assign(unit=UNIT).
              assign(area=AREA).
              assign(flow=FLOW).
              assign(original=ORIGINAL)
              )
        logger.debug(f"Number of transformed rows: {len(df)}")
        self.data = df.to_dict('records')
        self.remove_existing_dynamic_dim('entity')
        return None

    def _transform_cuenca(self):
        """
        Transform cuenca data.
        :return: a data frame with cuenca data.
        """
        logger.info("Transforming cuenca data.")
        source_code = f"{JOB_CODE}_PRODUCCION_CUENCA"
        logger.debug(f'Getting source for code {source_code}')
        source = self.get_source_from_code(source_code)

        if source is None:
            logger.warning(f"No source for cuenca found: {source_code}")
            return pd.DataFrame()

        try:
            logger.debug(f'Selected source: {vars(source)}')
            df = _get_clean_df(source.path)
        except AttributeError:
            logger.exception("Error while cleaning data frame.")
            return pd.DataFrame()
        df["long_name"] = "CNH_MX " + df["CUENCA"] + " " + df["UBICACION"]
        df["code"] = df["long_name"].map(
            lambda x: "CNH_MX_" + x.replace(" ", "_").upper())
        self.dynamic_dim['entity'].extend(_prepare_entity_cuenca(df))
        df = df[['code', 'FECHA', "PETROLEO_MBD"]]
        df.rename(columns={'PETROLEO_MBD': 'value', "FECHA": 'period', 'code': 'entity'},
                  inplace=True)
        df['product'] = 'CRUDEOIL'
        df['source'] = source_code

        # delete rows with value = 0
        df = df[df.value != 0]

        logger.debug(f"Number of rows read for cuenca: {len(df)}")
        return df

    def _transform_campos(self):
        """
        Transform campos data.
        :return: a data frame with campos data.
        """
        logger.info("Transforming campos data.")
        source_code = f"{JOB_CODE}_PRODUCCION_CAMPOS"
        logger.debug(f'Getting source for code {str(source_code)}')
        source = self.get_source_from_code(source_code)

        if source is None:
            logger.warning(f"No source for campos"
                           f" found: {source_code}")
            return pd.DataFrame()

        try:
            logger.debug(f'Selected source: {vars(source)}')
            df = _get_clean_df(source.path)
        except AttributeError:
            logger.exception("Error while cleaning data frame.")
            return pd.DataFrame()
        df.rename(columns={"CAMPO_OFICIAL": "long_name",
                           "CAMPO_SIH": "code"}, inplace=True)
        df["long_name"] = "CNH_MX " + df["long_name"]
        df["code"] = "CNH_MX_" + df["code"]
        self.dynamic_dim['entity'].extend(_prepare_entity_campos(df))
        df = df[['code', 'FECHA', "PETROLEO_MBD", 'CONDENSADO_MBD']]
        df.rename(columns={'PETROLEO_MBD': 'CRUDEOIL',
                           'CONDENSADO_MBD': 'COND',
                           "FECHA": 'period',
                           'code': 'entity'}, inplace=True)
        df = df.melt(id_vars=['entity', 'period'],
                     value_vars=['CRUDEOIL', 'COND'], var_name='product')
        df.dropna(inplace=True)
        df['source'] = source_code
        logger.debug(f"Number of rows read for cuenca: {len(df)}")
        return df


def _prepare_entity_cuenca(df):
    """
    Prepare the entity with cuenca data.
    :param df: source data frame.
    :return: a data frame with cuenca entities.
    """
    logger.info('Preparing entity data from cuencas.')
    entity = df[["code", "long_name", "CUENCA", "UBICACION"]].drop_duplicates()
    entity["UBICACION"] = (entity["UBICACION"].str.lower()).map(ENVIRONMENT_MAPPING)
    entity["category"] = "basin"
    entity = entity.to_dict('records')
    for ent in entity:
        ent['meta_data'] = {'basin': ent["CUENCA"],
                            'environment': ent["UBICACION"]}
        del ent["CUENCA"], ent["UBICACION"]
    logger.debug(f"Number of entities for cuenca: {len(entity)}")
    return entity


def _prepare_entity_campos(df):
    """
    Prepares the data for updating entity dimension with campos data.
    :param df: data frame containing all the data.
    :return: data frame will all campos entities.
    """
    logger.info("Preparing entity data from campos.")
    entity = df[["long_name", "code", "UBICACION"]].drop_duplicates()
    entity["UBICACION"] = (entity["UBICACION"].str.lower()).map(ENVIRONMENT_MAPPING)
    entity["category"] = "field"
    entity = entity.to_dict('records')
    for ent in entity:
        ent['meta_data'] = {'environment': ent["UBICACION"]}
        del ent["UBICACION"]
    logger.debug(f"Number of entities from campos: {len(entity)}")
    return entity


def _get_clean_df(path):
    """
    get the data from csv file and keep kb
    :param path: the path to the file
    :return:
    """
    file = FILE_STORE_PATH / path
    logger.info(f'Getting clean data frame from file: {file}')
    df = pd.read_csv(file, encoding='ISO-8859-1', skiprows=ROWS_TO_SKIP)
    df.dropna(inplace=True)
    return df


def _to_period_code(x):
    """
    Function to convert period to period code format.
    :param x: a date in format month/year.
    :return: a string in format MMMYYYY.
    """
    x = x.split("/")
    x = f"{calendar.month_abbr[int(x[0])]}{x[1]}".upper()
    return x
