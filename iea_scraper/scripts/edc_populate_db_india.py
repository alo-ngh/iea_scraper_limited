import sys

sys.path.append('C:\Repos\scraper')
from iea_scraper.settings import EDC_DAILY_USA_PRICES_JOBS
from iea_scraper.instance import EXT_DB_STR
from datetime import datetime
from iea_scraper.scripts.edc_populate_db import run_many_years_and_countries


if __name__ == "__main__":
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2022, 3, 23)
    start_end_countries = [(start_date, end_date, ['India_historical'])]
    run_many_years_and_countries(start_end_countries)
