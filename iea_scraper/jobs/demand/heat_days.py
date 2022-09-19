import logging
from ftplib import FTP
import os
import pandas as pd
from os import listdir
from os.path import isfile, join
from pathlib import Path
import gzip
import sqlalchemy
from sqlalchemy import create_engine
import tarfile


DB_STR = 'postgresql://omr:BigBarrel74@vipenta:5432/omr_etl'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_tar_file(path):
    ftp_filename = "gsod_2018.tar"
    allfiles = [ f for f in os.listdir(path) if f.endswith(".gz") ]
    for f in allfiles:
        try:
            os.remove(f"{path}/{f}")
        except Exception:
            logger.warning(f"file {f} not found")
    ftp = FTP("ftp.ncdc.noaa.gov", "ftp", "pierre.monferrand@iea.org")
    ftp.retrlines("LIST")
    ftp.cwd("pub/data/gsod/2018")
    ftp.retrbinary("RETR " + ftp_filename,
                   open(f"{path}/{ftp_filename}", 'wb').write)
    tar = tarfile.open(Path(path, ftp_filename))
    tar.extractall(path)
    tar.close()
    return None

    
def create_raw_df(path):
    df = pd.DataFrame()
    allfiles = [ f for f in os.listdir(path) if f.endswith(".gz") ]
    for f in allfiles:
        with gzip.open(f"{path}/{f}", 'rb') as gfile:    
                t_df = pd.read_csv(gfile, delim_whitespace=True)
                df = pd.concat([df, t_df])

    df.reset_index(inplace=True)
    df.columns = ['STN', 'WBAN', 'YEARMODA', 'TEMP', 'TEMP_H', 'DEWP', 'DEWP_H',
                  'SLP', 'SLP_H', 'STP', 'STP_H', 'VISIB', 'VISIB_H', 'WDSP',
                  'WDSP_H', 'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP',
                  'FRSHTT']
    df.drop(['TEMP_H', 'DEWP', 'DEWP_H', 'SLP', 'SLP_H', 'STP', 'STP_H',
             'VISIB', 'VISIB_H', 'WDSP', 'WDSP_H', 'MXSPD', 'GUST', 'MAX',
             'MIN', 'PRCP', 'SNDP', 'FRSHTT'], axis=1, inplace=True)    
    df.to_csv(f"{path}/raw_data.csv", index=False)
    return df


def get_station_list(path):
    #ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv
    ftp_path = "/pub/data/noaa/"
    ftp_filename = "isd-history.csv" 
    ftp = FTP("ftp.ncdc.noaa.gov", "ftp", "pierre.monferrand@iea.org")
    ftp.retrlines("LIST")
    ftp.cwd(ftp_path)
    ftp.retrbinary("RETR " + ftp_filename,
                   open(f"{path}/list_station.csv", 'wb').write)
    df = pd.read_csv(f"{path}/list_station.csv")
    df = df[['USAF', 'WBAN', 'CTRY']]
    df.to_csv(f"{path}/list_station.csv", index=False)
    return df


def temp_by_coutry(st_list, df_data):
    df_data['STN'] = df_data['STN'].astype(str)
    df = pd.merge(df_data, st_list, left_on=['STN','WBAN'],
                  right_on=['USAF', 'WBAN'])
    df = df[['YEARMODA', 'CTRY', 'TEMP']]
    df = df.groupby(['YEARMODA', 'CTRY']).mean()
    df.reset_index(level=0, inplace=True)
    df.to_csv(f"{path}/temp_iso.csv", index=False)
    return df


def add_iea_country_name(df, path):
    engine = create_engine(DB_STR)
    df_country = pd.read_sql_table("dim_country_iso", con=engine,
                                   schema="shared_dim")
    df_country = df_country[['iso_alpha_2', 'iea_code']]
    df = pd.merge(df, df_country, left_on='CTRY', right_on='iso_alpha_2')
    df.rename(columns={'iea_code': 'COUNTRY', 'iso_alpha_2': 'ISO2'},
              inplace=True)
    df = df[['COUNTRY', 'YEARMODA', 'TEMP']]
    df.to_sql("heating_days", engine, schema='heat',
              if_exists='replace', index=False)
    df.to_csv(f"{path}/temp_country.csv", index=False)
    return None


if __name__ == "__main__":
    path = Path(Path(__file__).parent, "tmp")
    download_tar_file(path)
    df_data = create_raw_df(path)
    df_sl = get_station_list(path)
    df = temp_by_coutry(df_sl, df_data)
    add_iea_country_name(df, path)