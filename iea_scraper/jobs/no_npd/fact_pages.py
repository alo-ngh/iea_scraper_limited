import os
from calendar import month_abbr
from copy import copy
from pathlib import Path

import pandas as pd

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.jobs.utils import convert_m3_to_kbd
from iea_scraper.settings import FILE_STORE_PATH
import logging

logger = logging.getLogger(__name__)

JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = 'NO_NPD'
FREQUENCY = 'Monthly'
SOURCE = 'NO'
UNIT = 'KBD'
AREA = 'NORWAY'
FLOW = 'SUPPLY'
PRODUCT = 'CRUDEOIL'
ORIGINAL = True

SERVER_IP = '172.20.48.40'

BASE_URL = "https://factpages.npd.no/ReportServer_npdpublic?/FactPages/TableView/field_production_monthly" \
           "&rs:Command=Render" \
           "&rc:Toolbar=false" \
           "&rc:Parameters=f" \
           "&rs:Format=CSV" \
           "&Top100=false" \
           f"&IpAddress={SERVER_IP}" \
           "&CultureCode=en"

FILE_NAME = 'field_prod_monthly.csv'


class FactPagesJob(ExtDbApiJob):
    """
    Scraper for Norwegian Monthly Field Oil Production.
    """
    title: str = "Norway - Field Monthly Production"

    def get_sources(self):
        """
        Add sources into self.sources and insert into database.
        """
        source = BaseSource(code=f"{JOB_CODE}_{FILE_NAME.split('.')[0]}",
                            url=BASE_URL,
                            path=f"{JOB_CODE}_{FILE_NAME}",
                            long_name=f"{AREA} {PROVIDER} "
                                      f"Monthly Production by Field"
                            )
        self.sources.append(source)

        # add dictionary to dynamic dims
        for source in self.sources:
            dicto = vars(copy(source))
            self.dynamic_dim['source'] += [dicto]

        self.remove_existing_dynamic_dim('source')

    def transform(self):
        """
        Transform data from each sources.
        """
        self.__transform_provider()

        for source in self.sources:
            full_path = os.path.join(FILE_STORE_PATH, source.path)
            df = pd.read_csv(full_path, sep=",")
            df.drop(columns=["prfPrdGasNetBillSm3", "prfPrdOeNetMillSm3",
                             "prfPrdProducedWaterInFieldMillSm3",
                             "prfNpdidInformationCarrier"], inplace=True)

            df.columns = ['entity', 'year', 'month', 'CRUDEOIL', 'NGLS', 'COND']

            self.__transform_entity(df)

            df = df.melt(id_vars=['entity', 'year', 'month'], var_name='product')
            df['value'] = df['value'] * 1000000
            df['value'] = df.apply(lambda x: convert_m3_to_kbd(x['value'], x['year'], x['month']), axis=1)
            df['period'] = df.apply(lambda x: f"{month_abbr[x.month].upper()}{x.year}", axis=1)

            df = (df.drop(columns=['year', 'month']).
                  assign(area=AREA).
                  assign(frequency=FREQUENCY).
                  assign(provider=PROVIDER).
                  assign(source=source.code).
                  assign(unit=UNIT).
                  assign(flow=FLOW).
                  assign(original=ORIGINAL))
            self.data = df.to_dict('records')

    def __transform_entity(self, df):
        """
        Check if entities exist and add them if not.
        :param df: data frame containing entities.
        """
        entity = df[['entity']].drop_duplicates()
        entity = entity.rename(columns={'entity': 'code'})
        entity['long_name'] = entity['code']
        entity['category'] = 'field'

        #   export entity to dictionary
        entity_dict = entity.to_dict('records')
        #   add dictionary to dynamic dims
        self.dynamic_dim['entity'] = entity_dict
        # load it!
        self.remove_existing_dynamic_dim('entity')

    def __transform_provider(self):
        """
        Loads the provider dimension.
        :return: None
        """
        logger.debug("Transforming provider ...")
        provider = dict()
        provider["code"] = PROVIDER
        provider["long_name"] = "Norwegian Petroleum Directorate"
        provider["url"] = "https://www.npd.no/"

        logger.debug(f"Adding provider to dynamic_dim: {PROVIDER}")
        self.dynamic_dim['provider'] = [provider]
        self.remove_existing_dynamic_dim('provider')
