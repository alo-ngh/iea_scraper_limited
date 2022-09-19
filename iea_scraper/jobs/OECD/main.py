"""Scripts to load OECD data. Next step add the National account data"""
from .oecd_utilities import make_q_url, get_oecd_data
from pathlib import Path
import sys
import requests
import pandas as pd

path = Path(__file__).absolute().parent
sys.path.append(str(path))
import config
import utilities

country_list = ["AUS", "AUT", "BEL", "CAN", "CHL", "CZE", "DNK", "EST", "FIN",
                "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
                "KOR", "LVA", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
                "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA", "EA16",
                "OTO", "WLD", "NMEC", "ARG", "BRA", "CHN", "COL", "CRI", "IND",
                "IDN", "LTU", "RUS", "ZAF", "DAE"]


country_list_qna = ["AUS", "AUT", "BEL", "CAN", "CHL", "CZE", "DNK", "EST", "FIN",
                "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
                "KOR", "LVA", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
                "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA", "EA16",
                "OTO", "WLD", "NMEC", "ARG", "BRA", "CHN", "COL", "CRI", "IND",
                "IDN", "LTU", "RUS", "DAE"]


def upload_main_oecd():
    update_eo()
    return None


def update_eo():
    qtype = "EO"
    stTime = "2000-Q1"
    df = get_oecd_data(qtype, country_list, "OOP.GDPV_USD.Q", stTime, 'TIME_PERIOD')
    df['unit'] = 'USD'
    df['flow'] = 'GDP'
    df = pd.concat([df, get_oecd_data(qtype, country_list, "OOP.GDPV_ANNPCT.Q",
                                      stTime, 'TIME_PERIOD')], sort=False)
    df['period'] = df['TIME_PERIOD'].map(lambda x: x[-2:][::-1] + x[:4])
    df['frequency'] = 'Quarterly'

    df_a = get_oecd_data(qtype, country_list, "OOP.GDPV_ANNPCT.A", "2000", 'TIME_PERIOD')
    df_a['period'] = df_a['TIME_PERIOD']
    df_a['frequency'] = 'Annual'
    df = pd.concat([df, df_a], sort=False)
    df.loc[df['unit'].isnull(), 'unit'] = 'PERC'
    df.loc[df['flow'].isnull(), 'flow'] = 'GDPGROWTH'

    countries = pd.DataFrame(requests.get(
        f"{config.API_END_POINT}/dimension/country").json())
    countries.rename(columns={'iso_alpha_3': 'LOCATION'}, inplace=True)
    df = pd.merge(df, countries[['short_name', 'LOCATION']], on='LOCATION',
                  how='inner')
    df = (df.rename(columns={'short_name': 'country'}).
          assign(provider='OECD').
          assign(product='None').
          assign(source='OECD_SDMX_EO').
          drop(columns=['TIME_PERIOD', 'LOCATION', 'VARIABLE', 'FREQUENCY'])
          )
    data = df.to_dict('records')
    utilities.batch_upload(data, f"{config.API_END_POINT}/main/datapoint", 500)
    return None


if __name__ == "__main__":
    upload_main_oecd()
