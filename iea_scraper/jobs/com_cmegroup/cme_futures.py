import logging
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from iea_scraper.core.job import ExtDbApiDedicatedTableJob
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import parallelize, calc_checksum_download
# from iea_scraper.jobs.utils import get_driver
from iea_scraper.settings import FILE_STORE_PATH, BROWSERDRIVER_PATH

logger = logging.getLogger(__name__)

MAX_WORKER = 4


class CmeFuturesJob(ExtDbApiDedicatedTableJob):
    """
    CME crude etc futures settlement data - Daily Data

    Provider: CME (https://www.cmegroup.com/markets/energy)

    Currently, it's getting the following quotes:

    * WTI - light-sweet crude
    * heating oil
    * RBOB gasoline
    * Henry Hub natural gas
    """

    title: str = "CME Futures Prices"

    job_code = Path(__file__).parent.parts[-1]
    # source_prefix = f'{job_code}_traffic'

    provider_code = job_code.upper()
    provider_long_name = 'CME'
    provider_url = 'https://www.cmegroup.com'

    base_url = "https://www.cmegroup.com/markets/energy"

    target_urls = {
        'WTI': {
            'online_sources_url': f'https://www.cmegroup.com/markets/energy/crude-oil/light-sweet-crude.html',
            'target_product_group': f'crude-oil',
            'target_product': f'light-sweet-crude'  # ,
            # 'source_prefix' : f'{target_product_group}_{target_product}'
        },
        'HeatingOil': {
            'online_sources_url': f'https://www.cmegroup.com/markets/energy/refined-products/heating-oil.html',
            'target_product_group': f'refined-products',
            'target_product': f'heating-oil'  # ,
            # 'source_prefix' : f'{target_product_group}_{target_product}'
        },
        'RBOB': {
            'online_sources_url': f'https://www.cmegroup.com/markets/energy/refined-products/rbob-gasoline.html',
            'target_product_group': f'refined-products',
            'target_product': f'rbob-gasoline'  # ,
            # 'source_prefix': f'{target_product_group}_{target_product}'
        },
        'HenryHub': {
            'online_sources_url': f'https://www.cmegroup.com/markets/energy/natural-gas/natural-gas.html',
            'target_product_group': f'natural-gas',
            'target_product': f'natural-gas'  # ,
            # 'source_prefix': f'{target_product_group}_{target_product}'
        }
    }

    # dedicated subdirectory in the filestore
    filestore_dir = Path(__file__).parent.stem

    delay = 2

    key_columns = ['provider', 'source', 'Year', 'Month', 'PriceType']
    db_schema = 'main'
    db_table_prefix = 'futures_cme'

    def __init__(self, **kwargs):
        """
        Initiates selenium browser driver into self.driver
        """
        super().__init__(**kwargs)
        self.driver = self.get_driver(headless=True)
        filestore_path = FILE_STORE_PATH / self.filestore_dir
        filestore_path.mkdir(exist_ok=True)

    def __del__(self):
        """
        Closes selenium driver before destroying the object.
        """
        self.driver.close()

    def get_driver(self, headless=True):
        """
        :return: an instance of selenium Chrome webdriver.
        """
        chrome_options = Options()
        # uncomment line below to hide browser window
        if headless:
            chrome_options.add_argument("--headless")
        # low memory option
        chrome_options.add_argument("--disable-dev-shm-usage")
        # enables clicks
        chrome_options.add_argument('window-size=1920x1480')
        # change default download directory
        prefs = {"download.default_directory": str(FILE_STORE_PATH)}
        chrome_options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(executable_path=str(BROWSERDRIVER_PATH),
                                  options=chrome_options)

        self.enable_download_in_headless_chrome(driver, str(FILE_STORE_PATH))

        return driver


    def enable_download_in_headless_chrome(self, driver, download_dir):
        """
        Enables download in headless mode in chrome.
        :param driver: the browser selenium driver
        :param download_dir: the directory where downloaded files will be saved.
        """
        # add missing support for chrome "send_command"  to selenium webdriver
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        command_result = driver.execute("send_command", params)

    def get_sources(self):
        """
        Defines the data sources to be downloaded.
        This scraper don't relay on full_load as it always download the full history.
        :return: NoReturn
        """
        logger.info('Getting sources...')

        # source_prefix is defined with "{v["target_product_group"]}_{v["target_product"]}"
        data_list = [BaseSource(url=f'{self.base_url}/{v["target_product_group"]}/{v["target_product"]}.html',
                                code=f'{date.today().strftime("%y%m%d")}_{v["target_product"]}',
                                path=f'{self.filestore_dir}/{date.today().strftime("%y%m%d")}_'
                                     f'{v["target_product"]}.csv',
                                target_product_group=f'{v["target_product_group"]}',
                                target_product=f'{v["target_product"]}',
                                long_name=f'CME FUTURES PRICE DATA FILE {v["target_product_group"]}_'
                                          f'{v["target_product"]} {date.today().strftime("%y%m%d")}')
                     for v in self.target_urls.values()]

        self.sources.extend(data_list)

        logger.info(f'{len(self.sources)} sources to load.')

    def download_and_get_checksum(self, download=True, parallel_download=True):
        """
        This function downloads all files listed in self.sources.

        :param download: Flag determining whether the file should be downloaded or not. Default is True.
        :param parallel_download: Flag determining whether download should occur in parallel. Default is True.
        :return NoReturn:
        """
        if download:
            logger.debug(f"download: {download}, parallel download: {parallel_download}")
            file_for_download = self.sources + self.source_complements

            for f in file_for_download:
                self.download_source(f)

        parallelize(calc_checksum_download, self.sources, MAX_WORKER)


    def interceptor(self, request):
        del request.headers['Connection']
        request.headers['Connection'] = 'keep-alive'
        del request.headers['User-Agent']
        request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'
        del request.headers['Accept-Language']
        request.headers['Accept-Language'] = 'en-US'


    def download_source(self, source, http_headers=None):
        """
        Download one given file.
        :param source: BaseSource object describing the file to download.
        :param http_headers: optional headers to pass in the request. Default is None.
        Defined as a static method to allow overloading
        """
        try:
            path, url, target_product_group, target_product = source.path, source.url, source.target_product_group, \
                                                              source.target_product

            logger.debug(f'url : {source.url}')

            # get target url through driver
            self.driver.request_interceptor = self.interceptor
            self.driver.get(url)

            # click settlements
            self.driver.find_element(By.LINK_TEXT, 'SETTLEMENTS').click()

            # wait for the browser to prepare the web sites reloaded.
            time.sleep(self.delay)

            # Go to load-all class and try to click the button
            target_element = self.driver.find_element(By.CLASS_NAME, 'load-all')
            self.driver.execute_script(
                f'window.scrollTo({target_element.location["x"]}, {(target_element.location["y"] - 500)});')
            self.driver.find_element(By.CLASS_NAME, 'load-all').click()

            # Search for the target table
            self.driver.find_element(By.CLASS_NAME, 'main-table-wrapper')
            trs = self.driver.find_elements(By.TAG_NAME, 'tbody')[0].find_elements(By.TAG_NAME, 'tr')

            # get all the target numbers or strings
            prices = []
            for i in range(0, (len(trs) - 1)):
                prices.append(
                    [target_product_group,
                     target_product,
                     trs[i].find_elements(By.TAG_NAME, "td")[0].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[1].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[2].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[3].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[4].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[6].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[7].text,
                     trs[i].find_elements(By.TAG_NAME, "td")[8].text])

            # Put it into the table
            df_settlement = pd.DataFrame(prices,
                                         columns=['Group', 'Product', 'Month', 'OPEN', 'HIGH', 'LOW', 'LAST', 'SETTLE',
                                                  'EST.VOLUME', 'PRIOR DAY OI'])

            # Because the first 12 month 'Month' dates are not captured in this scraping flow,
            start_date = datetime.strptime(df_settlement.iloc[12, 2].replace("JLY","JUL"), '%b %y') + relativedelta(years=-1)
            end_date = datetime.strptime(df_settlement.iloc[-1, 2].replace("JLY","JUL"), '%b %y') + relativedelta(months=+1)
            df_settlement['date'] = pd.date_range(start=start_date, end=end_date, freq='M')
            df_settlement['Year'] = df_settlement.date.dt.year
            df_settlement['Month'] = df_settlement.date.dt.month
            df_settlement.drop(columns=['date'], inplace=True)

            file_path = FILE_STORE_PATH / path
            df_settlement.to_csv(f"{file_path}", index=False)

        except AttributeError as e:
            raise AttributeError(f"Missing an essential source attribute: {e}")

        logger.debug(f'{len(df_settlement)} lines written to {file_path.name}.')

        # if downloaded and saved successfully, we fill last_download column in table source
        setattr(source, 'last_download', datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    def transform_source(self, source: BaseSource):
        """
        Transforms one data source.
        :param source: BaseSource: data source definition.
        :return: pd.DataFrame: transformed data.
        """
        file_path = FILE_STORE_PATH / source.path

        df_settlement = pd.read_csv(file_path)
        logger.info(f'{len(df_settlement)} rows read from {file_path}')

        # Make the table into proper tabular format
        df_settlement['EST.VOLUME'] = pd.to_numeric(df_settlement['EST.VOLUME'].str.replace(',', ''))
        df_settlement['PRIOR DAY OI'] = pd.to_numeric(df_settlement['PRIOR DAY OI'].str.replace(',', ''))
        df_tmp = df_settlement.set_index(['Group', 'Product', 'Year', 'Month']).stack().reset_index()
        df_tmp.rename(columns={'level_4': 'PriceType', 0: 'Value'}, inplace=True)
        df_tmp = df_tmp[~(df_tmp.Value == '-')]

        # Add date types
        df_tmp['DealType'] = ''
        df_tmp_num = df_tmp[pd.to_numeric(df_tmp.Value, errors="coerce").notna()].copy()
        df_tmp_nonnum = df_tmp[~pd.to_numeric(df_tmp.Value, errors="coerce").notna()].copy()
        df_tmp_nonnum['DealType'] = df_tmp_nonnum[
                                        ~pd.to_numeric(df_tmp_nonnum.Value, errors="coerce").notna()].Value.str[-1:]
        df_tmp_nonnum['Value'] = df_tmp_nonnum[~pd.to_numeric(df_tmp_nonnum.Value, errors="coerce").notna()].Value.str[
                                 :-1]
        df_tmp_nonnum['DealType'] = df_tmp_nonnum.DealType.str.replace('A', 'Ask').replace('B', 'Bid')

        df = pd.concat([df_tmp_num, df_tmp_nonnum]).sort_values(['Group', 'Product', 'Year', 'Month', 'PriceType'])
        df['source'] = source.code
        df['provider'] = self.provider_code

        return df

    def transform(self):
        """
        Transforms each downloaded file and save transformed data into self.data.
        :return: NoReturn
        """

        logger.info('Transforming data')
        dfs = [self.transform_source(source) for source in self.sources]

        if len(dfs) == 0:
            logger.info('No data to load.')
            return

        # in this scraper, we load directly a dataframe on data
        self.data = pd.concat(dfs)
