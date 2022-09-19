from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.com_kpler.base_kpler import BaseKplerJob
from iea_scraper.settings import FILE_STORE_PATH
from pathlib import Path
from urllib.parse import unquote
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class RussianCrudecondExportsJob(BaseKplerJob):
    """
    This scraper extracts data from Kpler necessary to calculate Russian seaborn crude and condensates exports.
    We cannot use the data as-is because some terminals also export crude/condensates from other countries.

    The exceptions:
    - In the Black Sea, CPC terminal should be excluded, but it does ship a small volume of rail-delivered
    Russian crude too (30-40 kb/d). You can present data “by seller” and after downloading, use only Lukoil data;

    - Also in the Black Sea, Sheskharis terminal exports should be drilled down to seller level. Exclude KMG and Socar;

    - Ust-Luga includes some Kazakh volumes. Present data by seller and exclude KMG;

    So, to attend this purposes, we download:

    - Total Russian crude/condensates exports by origin installations
    - CPC Terminal exports by seller
    - Sheskharis exports by seller
    - Ust Luga (TNTK Crude Ust Luga,Ust Luga Oil Terminal) exports by seller

    All these data is loaded into a table with the following columns:

    - date
    - product
    - origin_installation
    - seller
    - value

    Granularity is daily.
    """

    title: str = "Kpler - Russian Crude/Cond Exports"

    job_code = Path(__file__).parent.parts[-1]
    provider_code = job_code.upper()
    provider_long_name = "KPLER"
    provider_url = 'https://www.kpler.com/'

    db_schema = 'kpler'
    db_table_prefix = 'russian_exports'
    key_columns = ['date', 'product', 'origin_installation', 'seller']

    product_name = 'Crude/Co'

    code_prefix = 'com_kpler_ru_exp'

    # list of installations split by seller to download
    installations = {'cpc': 'CPC%20Terminal',
                     'she': 'Sheskharis',
                     'ust': 'TNTK%20Crude%20Ust%20Luga,Ust%20Luga%20Oil%20Terminal'}

    file_delimiter = ';'

    minimal_year = 2016

    def __init__(self,
                 year: int = None,
                 **kwargs):
        """
        In addition to existing parent's parameters, this defines a year.
        :param year: int: year to load.
        :param kwargs: parent's parameters
        """
        super().__init__(**kwargs)

        self.base_url = "https://api.kpler.com/v1/flows?" \
                        "flowDirection=Export" \
                        "&products=Crude%2FCo" \
                        "&granularity=daily" \
                        "&withForecast=false"

        self.year = year
        if year:
            logger.info(f'Year to load: {year}')

            if year < self.minimal_year:
                raise ValueError(f'Cannot load year {year}: first year available in Kpler is {2016}')

            start_date = f'{year}-01-01'
            end_date = f'{year}-12-31'

            self.base_url = f"{self.base_url}&startDate={start_date}&endDate={end_date}"

    def get_sources(self):
        """
        Defines the data sources to be downloaded.
        This scraper don't relay on full_load as it always download the full history.
        """
        year_str = f' {self.year}' if self.year else ""
        # Total Russian Exports
        code_total = f'{self.code_prefix}{year_str.replace(" ", "_")}_tot'
        total = BaseSource(code=code_total,
                           long_name=f'KPLER - Total Russian Crude/Co Exports{year_str}',
                           url=f'{self.base_url}'
                               '&split=Origin%20Installations'
                               '&fromZones=Russian%20Federation',
                           path=f'{code_total}.csv')
        self.sources.append(total)

        # installations by sellers
        for k, v in self.installations.items():
            _code = f'{self.code_prefix}{year_str.replace(" ", "_")}_{k}'
            s = BaseSource(code=_code,
                           long_name=f'KPLER - Russian Exports - {unquote(v)}{year_str}',
                           url=f'{self.base_url}'
                                '&split=Seller'
                               f'&fromInstallations={v}',
                           path=f'{_code}.csv')
            self.sources.append(s)
        logger.info(f'{len(self.sources)} sources to download.')

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """
        logger.info('Transforming data')
        if len(self.sources) == 0:
            logger.info('No data to load.')
            return

        dfs = [self.transform_source(source) for source in self.sources]
        # in this scraper, we load directly a dataframe on data
        self.data = pd.concat(dfs)

    def transform_source(self, source) -> pd.DataFrame:
        """
        Transforms each file downloaded from Kpler into the final table format.
        :param source: an instance of BaseSource describing the file.
        :return: pd.DataFrame
        """
        path = FILE_STORE_PATH / source.path
        file_type = path.stem.split('_')[-1]

        logger.info(f'Loading file {path.name}...')
        df = pd.read_csv(path, delimiter=self.file_delimiter, parse_dates=['Date'])
        logger.info(f'{len(df)} rows read from {path.name}.')

        # transform data
        df = df.drop(columns=['Period End Date'])
        df['Date'] = df['Date'].dt.normalize()

        # melt data frame
        _var_name = 'origin_installation' if file_type == 'tot' else 'seller'
        df = df.melt(id_vars='Date', var_name=_var_name)\
               .assign(product=self.product_name)

        # remove zero values
        df = df[df['value'] != 0]

        # add missing columns and format data frame
        if file_type == 'tot':
            df['seller'] = 'TOTAL'
        elif file_type == 'ust':
            df['origin_installation'] = 'Ust Luga'
        else:
            df['origin_installation'] = unquote(self.installations[file_type])

        df = df.rename(columns={'Date': 'date'})
        df = df[['date', 'product', 'origin_installation', 'seller', 'value']]

        logger.info(f'{len(df)} rows after transforming {path.name}')
        return df
