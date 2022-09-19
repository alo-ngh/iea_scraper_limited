# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 19:39:37 2020
``
@author: DAUGY_M
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys

from iea_scraper.settings import SSL_CERTIFICATE_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
sys.path.append(r'C:\Repos\scraper')
from iea_scraper.core.job import EdcJob, EdcBulkJob


class PeruvianPowerStatsJob(EdcBulkJob):
    title: str = 'coes.pe - Peru Power Statistics'

    def __init__(self):
        EdcBulkJob.__init__(self)
        self.urls = {'Generation': 'https://www.coes.org.pe/Portal/portalinformacion/generacion',
                     'Demand': 'https://www.coes.org.pe/Portal/portalinformacion/demanda'}
        self.product_mapping = {
            'DIESEL': 'Oil',
            'RESIDUAL': 'Other',
            'CARBÓN': 'Coal',
            'GAS': 'Natural Gas',
            'HÍDRICO': 'Hydro',
            'BIOGÁS': 'Biomass',
            'BAGAZO': 'Biomass',
            'SOLAR': 'Solar',
            'EÓLICA': 'Wind Onshore'
        }
        self.output_df = {'Generation': pd.DataFrame(),
                          'Demand': pd.DataFrame()}

    def get_json_data(self, tdate, metric):
        """
        This method extracts generation data from a json for tdate.
        Data is extracted using the method developed by coes.pe to upload the data to the website. The original code
        can be found here  : https://www.coes.org.pe/Portal/Content/Scripts/PortalInformacion/generacion.js
        The function that is used to upload data is cargarGeneracion. This means that we can make a POST request to
        the website with the corresponding params and the output is a json with all the data for the chose date
        :param date: datetime
        :return: json
        """

        r = requests.session()
        r.verify = SSL_CERTIFICATE_PATH
        json_data = {}
        if metric == 'Generation':
            response_url = r.post(self.urls[metric], data={
                'fechaInicial': tdate.strftime("%d/%m/%Y"),
                'fechaFinal': tdate.strftime("%d/%m/%Y"),
                'indicador': 0
            })
            json_data = response_url.json()['GraficoTipoCombustible']['Series']
        elif metric == 'Demand':
            response_url = r.post(self.urls[metric], data={
                'fechaInicial': (tdate - timedelta(days=1)).strftime("%d/%m/%Y"),
                'fechaFinal': tdate.strftime("%d/%m/%Y"),
            })
            json_data = response_url.json()['Data']
        return json_data

    def format_generation(self, json_data, metric):
        df = pd.DataFrame()
        for i in list(range(len(json_data))):
            df_json = pd.DataFrame(json_data[i]['Data'])
            df_json = df_json[['Nombre', 'Valor']]
            df_json['Product'] = self.product_mapping[json_data[i]['Name']]
            df_json = df_json.rename(columns={'Nombre': 'local_datetime', 'Valor': 'Value'})
            df_json['Metric'] = metric
            df = pd.concat([df, df_json])
        return df

    def format_demand(self, json_data, metric):
        df = pd.DataFrame()
        for i in list(range(len(json_data))):
            df_json = pd.DataFrame(json_data[i], index=[i])
            df_json = df_json[['Fecha', 'ValorEjecutado']]
            df_json = df_json.rename(columns={'Fecha': 'local_datetime', 'ValorEjecutado': 'Value'})
            df_json['Metric'] = metric
            df_json['Product'] = 'ELE'
            df = pd.concat([df, df_json])
        return df

    def format_data(self, json_data, metric):
        if metric == 'Generation':
            df = self.format_generation(json_data, metric)
        elif metric == 'Demand':
            df = self.format_demand(json_data, metric)
        df['local_datetime'] = pd.to_datetime(df['local_datetime'])
        df['local_date'] = df['local_datetime'].dt.date
        df['utc_datetime'] = df['local_datetime'].dt.tz_localize('America/Lima').dt.tz_convert('UTC')
        df['utc_date'] = df['utc_datetime'].dt.date
        df['Export Date'] = self.export_datetime
        df['Country'] = 'Peru'
        df['Source'] = 'coes.pe'
        return df

    @property
    def offset_now(self):
        return 1

    @property
    def df_dw(self):
        df_dw = pd.concat([self.output_df['Generation'], self.output_df['Demand']], sort=True)
        return df_dw

    def run_date(self, tdate):
        for metric in self.urls:
            json_data = self.get_json_data(tdate, metric)
            self.output_df[metric] = pd.concat([self.output_df[metric], 
                                                self.format_data(json_data, metric)])


if __name__ == '__main__':
    folder = r'C:\Repos\world_electricity_scraper\csvs'
    peru = PeruvianPowerStatsJob()
    tdate = datetime.now()
    peru.test_run(folder, historical=False)
