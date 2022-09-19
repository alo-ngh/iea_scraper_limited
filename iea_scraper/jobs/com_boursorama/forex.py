import logging

import json
import pandas as pd
from datetime import date, datetime, timedelta

from iea_scraper.settings import FILE_STORE_PATH
from iea_scraper.core.source import BaseSource
from iea_scraper.core.job import ExtDbApiDedicatedTableJob

logger = logging.getLogger(__name__)


class ForexJob(ExtDbApiDedicatedTableJob):
    """
    Temporary scraper to get daily RUB/USD, EUR/USD rates from Boursorama (last 6 months).
    Currently used in Daily Prices Report.

    URL: https://www.boursorama.com/bourse/devises/taux-de-change-dollar-rouble-USD-RUB/
    https://www.boursorama.com/bourse/devises/taux-de-change-dollar-rouble-EUR-USD/
    """
    title: str = "Boursorama - Forex data"

    rates = [{'from': 'usd', 'to': 'rub', 'symbol': 'USDRUB'},
             {'from': 'eur', 'to': 'usd', 'symbol': 'EURUS'}]

    provider_code: str = 'com_boursorama'
    provider_url: str = 'https://www.boursorama.com'
    provider_long_name: str = 'Boursorama'

    _url: str = 'https://www.boursorama.com' \
                '/bourse/action/graph/ws/GetTicksEOD?symbol=1x$$$SYMBOL$$$&length=180&period=0&guid='
    symbol: str = '$$$SYMBOL$$$'
    col_mappings: dict = {
                          'd': 'date',
                          'o': 'ouv',
                          'h': 'haut',
                          'l': 'bas',
                          'c': 'clot',
                          'v': 'vol'
                          }
    # the key is the date
    key_columns: list = ['date', 'from', 'to']

    db_schema: str = 'main'
    db_table_prefix: str = 'boursorama_forex'

    def get_sources(self):
        """
        Define the data sources related to each request.
        :return: NoReturn.
        """
        today = date.today().strftime('%Y%m%d')

        for rate in self.rates:
            _rate = f"{rate['from']}{rate['to']}"
            _code = f"boursorama_{_rate}_{today}"
            _url = self._url.replace(self.symbol, rate['symbol'])

            self.sources.append(BaseSource(code=_code,
                                        path=f'{_code}.json',
                                        long_name=f"Boursorama - {rate['from']}/{rate['to']} - {today}",
                                        url=_url,
                                        meta_data=rate))
        logger.info(f'{len(self.sources)} source files to load.')

    def transform(self):
        """
        Transforms the data read into a dataframe.
        Data is set to self.data as a pd.DataFrame.
        :return: NoReturn.
        """
        if len(self.sources) == 0:
            logger.info("No file to process.")
            return

        dfs = []
        for s in self.sources:
            rate = s.meta_data

            logger.info(f'Processing file {s.path}')
            path = FILE_STORE_PATH / s.path
            df = pd.json_normalize(json.loads(path.read_text()), record_path=['d', 'QuoteTab'])
            logger.info(f'{len(df)} rows read from {s.path}')

            # convert d to date
            df['d'] = df['d'].map(lambda d: datetime(1970, 1, 1) + timedelta(days=d))
            df.rename(columns=self.col_mappings, inplace=True)
            df['from'] = rate['from'].upper()
            df['to'] = rate['to'].upper()

            # change order of columns:
            # date, from, to, the rest of columns
            df = df[df.columns[0:1].to_list() + df.columns[-2:].to_list() + df.columns[1:-2].to_list()]

            dfs.append(df)

        self.data = pd.concat(dfs)
