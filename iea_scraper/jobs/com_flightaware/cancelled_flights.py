from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import BROWSERDRIVER_PATH, FILE_STORE_PATH, API_END_POINT

from pathlib import Path
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from time import sleep
from copy import copy
import logging
import requests
import pandas as pd
from pandas.io.json import json_normalize

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

SOURCE_URL = r'https://flightaware.com/live/cancelled/yesterday'
JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = JOB_CODE.upper()
PRODUCT = 'None'
TO_AREA = 'None'
FREQUENCY = 'Daily'
DETAIL = [
            {"code": f"{PROVIDER}_AL", "json": {"description": "By airline"}},
            {"code": f"{PROVIDER}_OA", "json": {"description": "By origin airport"}},
            {"code": f"{PROVIDER}_DA", "json": {"description": "By destination airport"}}
         ]
ORIGINAL = True

# First period with data in history
START_DATE = date(2020, 3, 9)
# We get data from yesterday
PUBLICATION_DELAY = 1
# Current period
CURRENT_PERIOD = date.today() - relativedelta(days=PUBLICATION_DELAY)
# standard period to wait for page to load
SLEEP = 5
# maximum number of attempts
MAX_RETRY = 3


class CancelledFlightsJob(ExtDbApiJob):
    """
    Scrape to load data from cancelled flights from flightaware.com
    """
    title: str = "FlightAware - Cancelled Flights"

    def get_sources(self):
        """
        Generate one source for current data.
        :return:
        """
        logger.debug("Generating sources...")
        start_date = START_DATE if self.full_load else CURRENT_PERIOD

        for period in reversed(pd.date_range(start_date, CURRENT_PERIOD)):
            logger.debug(f'Creating source for {period}')
            str_period = period.strftime('%Y%m%d')
            code = f"{JOB_CODE}_{str_period}"
            path = f"{code}.html"
            long_name = f"{PROVIDER} cancelled flights {str_period}"
            source = BaseSource(url=SOURCE_URL,
                                code=code,
                                path=path,
                                long_name=long_name)
            self.sources.append(source)

        # add dictionary to dynamic dims
        dicto = {}
        for source in self.sources:
            dicto = vars(copy(source))
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.info("Loading provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = f"FlightAware.com"
        provider["url"] = "https://flightaware.com"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')

    @staticmethod
    def __get_driver():
        """
        Hides details on launching the browser
        """
        chrome_options = Options()
        # uncomment line below to hide browser window
        chrome_options.add_argument("--headless")
        # low memory option
        chrome_options.add_argument("--disable-dev-shm-usage");
        # launch chrome instance
        logger.debug("Launching browser instance.")
        return webdriver.Chrome(executable_path=str(BROWSERDRIVER_PATH),
                                options=chrome_options)

    def download_source(self, source):
        """
        Overrrides super method to ensure only current period is downloaded.
        It also uses selenium to be able to reach data.

        :param source: the source object describing the object
        :return:
        """
        period = source.code.split('_')[-1]
        logger.info(f"Period: {period} {period[0:4]} {period[-4:-2]}")
        date_period = date(int(period[0:4]), int(period[-4:-2]), int(period[-2:]))

        if date_period == CURRENT_PERIOD:
            driver = self.__get_driver()
            file = FILE_STORE_PATH / source.path
            retry = 0
            while retry < MAX_RETRY:
                try:
                    logger.debug(f"Opening {SOURCE_URL}")
                    driver.get(SOURCE_URL)
                    logger.debug(f"Waiting {SLEEP} seconds for page download.")
                    sleep(SLEEP)
                    boards = driver.find_element_by_class_name("cancellation_boards")
                    html = boards.get_attribute('innerHTML')
                    # the following command throws ValueError if no table found
                    pd.read_html(html)
                    file.write_text(html, encoding='UTF-8')
                    logger.debug(f"Data written to {source.path}")
                    break
                except ValueError:
                    retry += 1
                    print(f"Attempt #{retry}: no table found!")
            driver.close()
            setattr(source, 'last_download', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
        else:
            logger.debug(f"Source date {date_period} not current period ({CURRENT_PERIOD}): skipping download.")

    @staticmethod
    def __get_entity_details(category):
        """
        Get entity details from entity dimension.
        @param category: the category to filter
        :return: dataframe with entity details
        """
        endpoint = f"{API_END_POINT}/dimension/entity"
        response = requests.get(endpoint)

        if not response.ok:
            raise ValueError("Impossible to connect to External DB API {endpoint}")
        data = response.json()
        return pd.DataFrame([x for x in data if x['category'] == category])

    @staticmethod
    def __get_area():
        """
        Get area details from area dimension.
        :return: dataframe with area details
        """
        endpoint = f"{API_END_POINT}/dimension/area"
        response = requests.get(endpoint)
        if not response.ok:
            raise ValueError("Impossible to connect to External DB API {endpoint}")
        return pd.DataFrame(response.json())

    def __transform_detail(self):
        """
        Insert static values into dimension detail to individualize each table:
        - airline
        - origin airport
        - destination airport
        :return: None
        """
        logger.debug(f"Adding 'details' to dynamic_dim: {DETAIL}")
        self.dynamic_dim['detail'] = DETAIL
        self.remove_existing_dynamic_dim('detail')

    def __parse_source_files(self):
        """
        Auxiliary method to parse all source files
        :return: list(dataframe)
        """
        columns = ["cancelled_n", "cancelled_p", "delayed_n", "delayed_p", "entity_name"]
        category = ["airline", "airport", "airport"]
        results = []

        for source in self.sources:
            try:
                # parse period from source code
                period = datetime.strptime(source.code.split("_")[-1], "%Y%m%d")
                path = FILE_STORE_PATH / source.path
                logger.debug(f"Reading file {path}")
                dfs = pd.read_html(path.read_text(encoding='UTF-8'))

                if len(dfs) != 3:
                    raise ValueError(f"Schedule tables not found in file {path}")

                # process each of the 3 tables (stats by airline, origin airport, destination airport
                for i, df in enumerate(dfs):
                    logger.debug(f'file {source.path}: reading table {i}')
                    # drop multi-levels
                    df.columns = df.columns.droplevel()
                    # set understandable column names
                    df.columns = columns
                    df["category"] = category[i]
                    df["detail"] = DETAIL[i]['code']
                    df["source"] = source.code
                    df["period"] = period.strftime("%Y-%m-%d")

                    results.append(df)
            except ValueError as e:
                logger.warning(f"Exception ValueError occurred while processing source {source.path}: {e}")

        logger.debug(f"Concatenating {len(results)} data frames.")
        df = pd.concat(results)
        return df

    def __transform_schedules(self):
        """
        This method opens each source file in self.sources
        and loads it into External DB.
        :return: None
        """
        if self.data is None:
            self.data = []
        df = self.__parse_source_files()

        # filter: keep for now only airport statistics
        df = df[df['category'] == 'airport']

        # flow: cancelled_flights, delayed_flights
        # unit: ind, percent
        df = df.melt(id_vars=["entity_name", "category", "detail", "source", "period"], var_name=["flow_unit"])
        df["flow"] = df["flow_unit"].apply(lambda x: "".join([x.split("_")[0], "_flights"]).upper())
        df["unit"] = df["flow_unit"].apply(lambda x: "IND" if x.split("_")[-1] == "n" else "PERC")
        del df["flow_unit"]

        # time to fix value: when unit = PERC, values are string with "%"
        df['value'] = df.apply(lambda x: float(x['value'][:-1])/100 if x['unit'] == 'PERC' else float(x['value'])
                               , axis=1)

        # create entity from entity_name (airport-specific)
        df['entity'] = df['entity_name'].str.extract(pat=r'\(([A-Z]{3})\)')
        del df['entity_name']

        # get area code from airport details
        airports = self.__get_entity_details('airport')
        # transform metadata string into dict and then to data frame
        airport_meta_data = json_normalize(airports['meta_data'].map(eval))
        # concatenate meta data columns to airports data frame
        full_airports = pd.concat([airports, airport_meta_data], axis=1)
        df = pd.merge(df, full_airports[['code', 'iso_country']],
                      left_on="entity", right_on="code", how="left", indicator="mr")
        df.loc[df['mr'] == 'left_only', 'iso_country'] = 'None'
        df.drop(columns=['mr', 'code'], inplace=True)

        # now get area (countries)
        areas = self.__get_area()
        logger.debug(f"areas: {areas.head()}")
        areas = areas[['code', 'iso_alpha_2']]
        df = pd.merge(df, areas,
                      left_on="iso_country", right_on="iso_alpha_2", how="left", indicator="mr")
        # just in case it has no matches, set them to None
        df.loc[df['mr'] == 'left_only', 'code'] = 'None'
        df.rename(columns={'code': 'area'}, inplace=True)
        df.drop(columns=['category', 'iso_country', 'iso_alpha_2', 'mr'], inplace=True)

        # adding other columns
        df = df.assign(provider=PROVIDER,
                       product=PRODUCT,
                       to_area=TO_AREA,
                       frequency=FREQUENCY,
                       original=ORIGINAL)

        df.drop_duplicates(inplace=True)

        logger.debug(f'A preview of the resulting data: {df.head()}')

        # load results into self.data
        self.data.extend(df.to_dict('records'))

    def transform(self):
        self.__transform_provider()
        self.__transform_detail()
        self.__transform_schedules()
