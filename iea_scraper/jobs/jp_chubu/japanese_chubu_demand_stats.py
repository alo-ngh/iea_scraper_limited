from zipfile import ZipFile

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
from iea_scraper.core.japan_job import EdcJapanJob
from iea_scraper.core.exceptions import EdcJobError
from iea_scraper.core.job import EdcBulkJob
from iea_scraper.settings import REQUESTS_HEADERS, SSL_CERTIFICATE_PATH
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')


class JapaneseChubuDemandStatsJob(EdcJapanJob):
    title: str = 'CHUBU - Japanese Generation Statistics'

    def __init__(self):
        EdcJapanJob.__init__(self)
        self.metric = 'Demand'
        self.url = "https://powergrid.chuden.co.jp/denki_yoho_content_data/download_csv/%Y%m_power_usage.zip"
        self.all_demand_dict = {}
        self.conversion_mw = 10
        self.region ='Chubu'
        self.source='Chubu'

    def get_data(self, tdate):
        """
        collects all data from csv in zip and stores them into all_demand_dict
        csv are parsed to get only demand data (rest of csv contains solar power output)
        @param date: datetime
        @return: dictionary
        """
        date_url = tdate.strftime(self.url)
        r = requests.get(date_url, verify=SSL_CERTIFICATE_PATH, headers=REQUESTS_HEADERS)
        zip = ZipFile(io.BytesIO(r.content))
        for file in zip.namelist():
            self.all_demand_dict[file[-24:-16]] = pd.read_csv(zip.open(file), encoding='mskanji',  skiprows=13, nrows=24)

    def format_df(self, tdate):
        """
        checks if DF exists in all_demand_dict and then formats DF
        @param date: datetime
        @return: DataFrame (output_df)
        """
        if tdate.strftime("%Y%m%d") not in self.all_demand_dict:
            raise EdcJobError(f"Japan CHUBU: damand data not available for {tdate.date()}")
        else:
            df = self.all_demand_dict[tdate.strftime("%Y%m%d")]
            df = df.rename(columns={'当日実績(万kW)':'Value'})
            df['local_datetime'] = df.apply(lambda x: datetime.strptime(x['DATE'] + ' ' + x['TIME'], "%Y/%m/%d %H:%M"),
                                            axis=1)
            df['local_date'] = df['local_datetime'].dt.date
            df = self.format_demand_for_all_scrapers(df)
            self.output_df = pd.concat([self.output_df, df])
            logger.info(f"JAPAN CHUBU: demand data collected for {tdate.date()}")


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    chubu_scraper = JapaneseChubuDemandStatsJob()
    # chubu_scraper.run_date(datetime(2019,3,20))
    chubu_scraper.test_run(folder, historical=True)
