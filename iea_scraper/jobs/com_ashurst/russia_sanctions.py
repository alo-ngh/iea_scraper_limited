import logging
import pandas as pd
from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import FILE_STORE_PATH
from datetime import datetime

logger = logging.getLogger(__name__)


class RussiaSanctionsJob(ExtDbApiDedicatedTableJob):
    """
    Scraper to load list of Russia Sanctions.

    Data source: https://www.ashurst.com/en/news-and-insights/hubs/sanctions-tracker/

    For internal-use only.
    """

    title: str = "Ashurst - Russia Sanctions Tracker"

    provider_code: str = "com_ashurst"
    provider_long_name: str = "Ashurst Group"
    provider_url: str = "https://www.ashurst.com/"

    source_code = f'{provider_code}_russia_sanctions'
    source_long_name = f'{provider_long_name} - Russia Sanctions Tracker'
    source_url = "https://www.ashurst.com/en/news-and-insights/hubs/sanctions-tracker/"
    source_path = f'{source_code}.html'

    # list of columns
    key_columns: list = ['country', 'Date of imposition', 'order']
    db_schema: str = 'main'
    db_table_prefix: str = 'ashurst_russia_sanctions'

    # List of covered countries
    countries: list = ['UK', 'EU', 'Japan', 'Australia']

    # date format in source: dd Month Year
    date_format: str ='%d %B %Y'

    def get_sources(self):
        """
        This method defines how the data will be retrieved and saved.
        Only one file is saved as we don't track history.
        """
        source = BaseSource(code=self.source_code,
                            long_name=self.source_long_name,
                            url=self.source_url,
                            path=self.source_path)
        logger.info('Adding 1 source to self.sources.')
        logger.debug(f'Source details: {vars(source)}')
        self.sources.append(source)

    def transform(self):
        """
        Extract the tables from HTML page and prepare the data to load into a table.
        :return: NoReturn.
        """
        if len(self.sources) == 0:
            logger.info('No data to load. Exiting.')
            return

        source = self.sources[0]
        path = FILE_STORE_PATH / source.path
        logger.debug(f'Reading file {path}')

        dfs = pd.read_html(path)

        if len(self.countries) != len(dfs):
            raise ValueError(f'The number of tracked countries ({len(self.countries)}) is different '
                             f'from what we get from the website ({len(dfs)}). '
                             f'Please check the list of covered countries for changes.')

        logger.info(f'{len(dfs)} lists of sanctions loaded from file.')

        for c, df in zip(self.countries, dfs):
            # add the country
            df['country'] = c
            # forward-fill date
            df.ffill(axis=1)

        # merge dataframes
        df = pd.concat(dfs)

        # convert date
        df['Notes'] = df['Date of imposition'].apply(get_date_notes)
        df['Date of imposition'] = df['Date of imposition'].apply(get_date)
        df['Date of imposition'] = pd.to_datetime(df['Date of imposition'], errors='coerce', format=self.date_format)

        # sanction order for a country/day
        df['order'] = df.groupby(['country', 'Date of imposition']).cumcount() + 1

        # load into self.data
        self.data = df


def get_date(d):
    split = d.split()
    return d if len(split) == 3 else ' '.join(split[:3])


def get_date_notes(d):
    split = d.split()
    return None if len(split) == 3 else ' '.join(split[3:]).strip('(').strip(')')
