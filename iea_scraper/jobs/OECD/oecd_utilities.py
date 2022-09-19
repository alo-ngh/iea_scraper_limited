import pandas as pd
import pandasdmx


def make_q_url(qtype, country_list, dset, stTime):
    query = f"{'+'.join(country_list)}+{dset}/all"
    query = f"{query}?startTime={stTime}"
    return query

def get_oecd_data(qtype, country_list, dset, stTime, reset_col):
    query = make_q_url(qtype, country_list, dset, stTime)
    oecd = pandasdmx.Request('OECD')
    data_response = oecd.data(resource_id=qtype, key=query)
    df = data_response.write(data_response.data.series, parse_time=False)
    df = df.reset_index().melt(id_vars=reset_col)
    return df
