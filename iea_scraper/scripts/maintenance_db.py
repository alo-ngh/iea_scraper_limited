import logging
from typing import List, Dict, NoReturn
from sqlalchemy import create_engine

from iea_scraper.core.utils import config_logging
from iea_scraper.settings import (EXT_DB_STR,
                                  LOGGING_DAILY)

config_logging(LOGGING_DAILY)
logger = logging.getLogger()

# List of clustered columnstore index tables to rebuild.
# Assuming CCI index name is 'cci_<table name>'.
table_list: List[Dict[str, str]] = [{'db_schema': 'argus_prices', 'table_name': 'argus_prices_data'},
                                    {'db_schema': 'argus_prices', 'table_name': 'forward_curves_data'},
                                    {'db_schema': 'main', 'table_name': 'google_mobility_data'},
                                    {'db_schema': 'main', 'table_name': 'futures_options_data'},
                                    {'db_schema': 'main', 'table_name': 'oxford_economics_api_data'}]


def rebuild_cci(db_schema: str, table_name: str) -> NoReturn:
    """
    This function runs a rebuild index statement a given table.
    Assumes cci index name as table name with a prefix cci_.
    :param: db_schema: str: the database schema of the table.
    :param: table_name: str: the table name.
    :return: NoReturn
    """
    cci_index: str = f'cci_{table_name}'
    logger.info(f'Rebuilding index {cci_index}...')
    cci_query = f'ALTER INDEX {cci_index} ON {db_schema}.{table_name} REBUILD;'
    logger.debug(f'Rebuild index statement: {cci_query}')
    engine = create_engine(EXT_DB_STR)
    logger.debug(f'Opening connection: {EXT_DB_STR}')
    with engine.begin() as con:
        con.execute(cci_query)
    logger.info(f'Rebuild of {cci_index} finished successfully.')


def main():
    """
    Main program.
    It calls rebuild_cci for every entry of table_list.
    """
    [rebuild_cci(**params) for params in table_list]


if __name__ == "__main__":
    main()
