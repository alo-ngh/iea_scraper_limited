import json
from pathlib import Path
import pandas as pd
import requests
import zipfile

from iea_scraper.settings import API_END_POINT, FILE_STORE_PATH
from iea_scraper.core.job import ExtDbApiJob
from iea_scraper.core.source import BaseSource
from iea_scraper.core.utils import get_dimension_db_data
from iea_scraper.jobs.utils import to_detail_format


JOB_CODE = Path(__file__).parent.parts[-1]
PROVIDER = [{'code': 'US_EIA', 'long_name': 'U.S Energy Information Administration',
             'url': 'https://www.eia.gov/'}]
ORIGINAL = True
BASE_URL = "https://api.eia.gov/bulk/"
FILES = [("Petroleum", "PET"),
         ("Natural Gas", "NG")]
MANIFEST_URL = "https://api.eia.gov/bulk/manifest.txt"


class BulkPetNgJob(ExtDbApiJob):
    """
    Scraper for loading EIA series through bulk interface.
    Currently loading PET & NG files.
    """
    title: str = "EIA - PET & NG data"

    def get_sources(self):
        for file in FILES:
            source = {'code': f"{JOB_CODE}_{file[1]}",
                      'file': file[1],
                      'url': f"{BASE_URL}{file[1]}.zip",
                      'path': f"{JOB_CODE}_{file[1]}.zip"}
            self.sources.append(BaseSource(**source))
            source.pop('file')
            r = requests.get(
                f"{API_END_POINT}/dimension/source?code={source['code']}")
            if r.status_code == 404:
                source.update(long_name=f"{JOB_CODE}: {file[0]}")
                r = requests.post(
                    f"{API_END_POINT}/dimension/source", json=[source])

        self.source_complements.append(BaseSource(
            code=f"{JOB_CODE}_BulkManifest",
            url=MANIFEST_URL,
            path=f"{JOB_CODE}_BulkManifest.txt"))
        return None

    def transform(self):
        self.data = []
        self.dynamic_dim["provider"] = PROVIDER
        for source in self.sources:
            ts_df = pd.read_csv(Path(Path(__file__).parent,
                                     f"{source.file}.csv"))
            self.add_details_to_dynamic_dim(ts_df, source.code)
            ts = ts_df['series_id'].tolist()
            del ts_df['description'], ts_df['name']
            data = _get_data_series(source, ts)
            df = _get_df(data)
            df = df[~df['value'].isna()]
            del data, ts
            df = map_period(df)
            df = df.merge(ts_df, how='left', on='series_id')
            df['original'] = True
            del df['series_id']
            self.data.extend(df.to_dict('records'))
        self.remove_existing_dynamic_dim("provider")
        return None

    def add_details_to_dynamic_dim(self, df, category):
        df = df[['series_id', 'description', 'name']]
        df.rename(columns={'series_id': 'code'}, inplace=True)
        df['category'] = category
        data = to_detail_format(df)
        self.dynamic_dim["detail"].extend(data)
        self.remove_existing_dynamic_dim('detail', {'category': category})
        return None


def _get_data_series(source, id_lists):
    path = Path(FILE_STORE_PATH, source.path)
    file = f"{source.file}.txt"
    data = []
    z = zipfile.ZipFile(path)
    with z.open(file) as f:
        for line in f:
            series = json.loads(line.decode())
            if 'data' in series.keys():
                if series['series_id'] in id_lists:
                    data.append(json.loads(line.decode()))
    return data


def _get_df(data):
    dfs = []
    for series in data:
        tmp_df = pd.DataFrame(series['data'], columns=['period', 'value'])
        tmp_df['series_id'] = series['series_id']
        dfs.append(tmp_df)
    return pd.concat(dfs, ignore_index=True)


def map_period(df):
    df['period'] = df['period'].map(lambda x: int(x.replace('Q', '')))
    db_periods = get_dimension_db_data('period')
    db_periods = pd.DataFrame(db_periods)[['code', 'id']]
    df = pd.merge(df, db_periods, left_on='period', right_on='id', how='left')
    del df['id'], df['period']
    df.loc[df['code'].isna(), 'code'] = None
    df.rename(columns={'code': 'period'}, inplace=True)
    return df