import requests
import pandas as pd
from datetime import datetime
import logging
import sys

from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import REQUESTS_HEADERS, SSL_CERTIFICATE_PATH
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseChubuGenerationStatsJob(EdcJapanJob):
    title: str = 'CHUBU - Japanese Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Generation'
        self.url = "https://powergrid.chuden.co.jp/denki_yoho_content_data/{year}_areabalance_current_term.csv"
        self.fuel_mapping = {'原子力': 'Nuclear',
                             '火力': 'Thermal',
                             '水力': 'Hydro',
                             '地熱': 'Geothermal',
                             'バイオマス': 'Biomass',
                             '太陽光（実績）': 'Solar PV',
                             '太陽光（出力制御量）': 'Solar PV',
                             '風力（実績）': 'Wind Onshore',
                             '風力（出力制御量）': 'Wind Onshore',
                             '揚水': 'Hydro Pumped Storage'
                             }
        self.conversion_mw = 10
        self.region = 'Chubu'
        self.source = 'Chubu'

    def get_data(self, tdate):
        """
        collects csv data from link and stores in year_df_generation with year as key
        warning: year is fiscal year
        @return: dictionary
        """
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            link = self.url.format(year=csv_year)
            r = requests.get(link, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
            if r.status_code == 200:
                self.year_df_generation[csv_year] = pd.read_csv(io.StringIO(r.content.decode('mskanji')), skiprows=4)
            else:
                logger.warning(f"JAPAN CHUBU: generation data not available for {tdate.date()}")

    def format_df(self, tdate):
        csv_year = tdate.year if tdate.month >= 4 else tdate.year - 1
        if self.year_df_generation[csv_year] is None:
            raise EdcJobError(f"JAPAN CHUBU: generation data not available for {tdate.date()}")
        else:
            df = self.year_df_generation[csv_year].copy()
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                            axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = df.drop(columns=['DATE', 'TIME'])
            df = df.loc[df['local_date'] == tdate.date()]
            df = self.format_generation_for_all_scrapers(df)
            self.output_df = pd.concat([self.output_df, df])
            logger.info(f"Japan CHUBU: generation data extracted and formatted for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    moon_scraper = JapaneseChubuGenerationStatsJob()
    moon_scraper.test_run(folder, historical=True)
    # moon_scraper.run_date(datetime(2022,3,30))
