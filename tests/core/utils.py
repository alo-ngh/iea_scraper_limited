from pathlib import Path

from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.settings import ROOT_PATH


TEST_FILE_STORE = Path(ROOT_PATH, 'scraper/tests/filestores_')

class JobTest(ExtDbApiJob):
    def get_sources(self):
        self.sources = [BaseSource('a', 'http://b', 'c')]
        self.source_complements = [BaseSource('ac', 'http://b:bc', 'cc')]
        return None

    def transform(self):
        self.dynamic_dim_dfs = {'source': [{'code': 'code1'}]}
        self.data = [{'provider': 'provider 1','value': 1.0}]
        return None
