import sys

sys.path.append('C:\Repos\scraper')
from iea_scraper.settings import EDC_DAILY_JAPAN_JOBS
from iea_scraper.instance import EXT_DB_STR
from datetime import datetime
from iea_scraper.scripts.edc_populate_db import run_many_years_and_countries, check_start_end_countries


def populate_db_japan(start, end):
    start_end_countries = [(start, end, [tso for tso in EDC_DAILY_JAPAN_JOBS])]
    check_start_end_countries(start_end_countries)
    run_many_years_and_countries(start_end_countries, folder=None, db_str=EXT_DB_STR,
                                 parallelise='country')

def populate_db_japan_generation(start, end):
    start_end_countries = [(start, end, [tso for tso in EDC_DAILY_JAPAN_JOBS if 'generation' in tso])]
    check_start_end_countries(start_end_countries)
    run_many_years_and_countries(start_end_countries, folder=None, db_str=EXT_DB_STR,
                                 parallelise='country')

def populate_db_japan_demand(start, end):
    start_end_countries = [(start, end, [tso for tso in EDC_DAILY_JAPAN_JOBS if 'demand' in tso])]
    check_start_end_countries(start_end_countries)
    run_many_years_and_countries(start_end_countries, folder=None, db_str=EXT_DB_STR,
                                 parallelise='country')


if __name__ == "__main__":
    populate_db_japan_generation(datetime(2016, 1, 1), datetime(2022, 1, 11))
    populate_db_japan_demand(datetime(2016, 1, 1), datetime(2022, 1, 11))

