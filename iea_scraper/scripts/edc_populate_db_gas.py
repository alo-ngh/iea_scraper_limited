import sys

sys.path.append('C:\Repos\scraper')
from iea_scraper.settings import EDC_DAILY_GAS_JOBS
from iea_scraper.instance import EXT_DB_STR
from datetime import datetime
from iea_scraper.scripts.edc_populate_db import run_many_years_and_countries, check_start_end_countries

def populate_db_entsog(start, end):
    start_end_countries = [(start, end, [tso for tso in EDC_DAILY_GAS_JOBS])]
    check_start_end_countries(start_end_countries)
    run_many_years_and_countries(start_end_countries, folder=None, db_str=EXT_DB_STR,
                                 parallelise='country')

if __name__ == "__main__":
    populate_db_entsog(datetime(2021, 1, 1), datetime(2022, 3, 22))


