import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine

from iea_scraper.core.utils import send_message, config_logging
from iea_scraper.settings import (EXT_DB_STR,
                                  NEW_DATA_DAILY_RECIPIENT,
                                  LOGGING_DAILY)

config_logging(LOGGING_DAILY)
logger = logging.getLogger()

html_new_data = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
table {
  border-collapse: collapse;
  border-spacing: 0;
  width: 100%;
  border: 1px solid #ddd;
}

th, td {
  text-align: left;
  padding: 8px;
}

tr:nth-child(even){background-color: #f2f2f2}
</style>
</head>
<body>
<h2>IEA-External-DB New Data Alert</h2>
<p>
<p>Dear Analyst,</p>
<p>
<p>New data have been loaded into IEA-External-DB:</p>

<div style="overflow-x:auto;">
 @@@TABLE@@@
</div>

<h2>Notes</h2>

<p>All sources are checked except EIA and KPLER (that change everyday).</p>
<p/>
<p>For more information: <a href="http://vimars/powerbireports/powerbi/IEA%20External%20DB/Search"/>http://vimars/powerbireports/powerbi/IEA%20External%20DB/Search</a>.</p>

<p>OMR Team</p>
</body>
</html>
"""


def alert_upon_new_data():
    # for now, daily scrapers start the previous day at 8 pm
    from_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 20:00:00.000')
    logger.debug(f'from date: {from_date}')
    engine = create_engine(EXT_DB_STR)
    stmt = f"""select s.long_name as [source], d.period_fk as [period], d.[affected rows]
    from 
    (select provider_fk, source_fk, period_fk, count(*) [affected rows]
     from main.FACT_datapoint
     where date_created > Cast('{from_date}' AS date) 
        or date_modified > Cast('{from_date}' AS date)
     group by provider_fk, source_fk, period_fk) d
    inner join dimension.LU_source s
            on s.id = d.source_fk
    inner join dimension.LU_provider p
            on p.id = d.provider_fk
    where p.code not in ('US_EIA', 'COM_KPLER', 'GOV_NOAA_NCEI')
    union all
    select 'JAPAN PAJ Oil Stats Weekly  (dedicated table)' as [source],
           FORMAT([First Day of Week], 'yyyyMMdd') as [period],
           1 as [affected_rows]
    from [main].[paj_jp_oil_stats_weekly_data]
    where date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date)
    union all
    select 'Commitment of Traders ICE Futures Europe (dedicated table)' as [source],
           FORMAT([Week_Of_Release], 'yyyyMMdd') as [period],
           COUNT(1) as [affected_rows]
    from [main].[futures_options_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
       and provider = 'COM_THEICE'
    group by [Week_Of_Release]
    union all
    select 'Commitment of Traders CFTC (dedicated table)' as [source],
           FORMAT([Week_Of_Release], 'yyyyMMdd') as [period],
           COUNT(1) as [affected_rows]
    from [main].[futures_options_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
       and provider = 'GOV_CFTC'
    group by [Week_Of_Release]
    union all
    select 'UK Road Fuel Sales' as [source],
           FORMAT([date], 'yyyyMM') as [period],
           COUNT(1) as [affected_rows]
    from [main].[uk_road_fuel_sales_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
    group by FORMAT([date], 'yyyyMM')
    union all
    select 'French vehicle traffic' as [source],
           FORMAT([date], 'yyyyMM') as [period],
           COUNT(1) as [affected_rows]
    from [main].[fr_cerema_traffic_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
    group by FORMAT([date], 'yyyyMM')
    union all 
    select 'Platts Fujairah Stocks' as [source],
           FORMAT([date], 'yyyyMM') as [period],
           COUNT(1) as [affected_rows]
    from [main].[com_platts_fujairah_stocks_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
    group by FORMAT([date], 'yyyyMM')
    union all
    select 'Platts Fujairah Bunkers' as [source],
           FORMAT([date], 'yyyyMM') as [period],
           COUNT(1) as [affected_rows]
    from [main].[com_platts_fujairah_bunkers_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
    group by FORMAT([date], 'yyyyMM')
    union all
    select 'Oxford Economics API' as [source],
           [period] as [period],
           COUNT(1) as [affected_rows]
    from [main].[oxford_economics_api_data]
    where (date_created > Cast('{from_date}' AS date) 
       or date_modified > Cast('{from_date}' AS date))
    group by [period]
    order by 2 desc
    """

    logger.debug(f"Query: {stmt}")

    df = pd.read_sql(stmt, engine)

    logger.debug(f'len(df): {len(df)}')

    if len(df) > 0:
        logger.info('Sending new data alert e-mail.')
        table = df.to_html(index=False)
        message = html_new_data.replace("@@@TABLE@@@", table)

        send_message(subject="[IEA-External-DB] New Data Alert",
                     html_content=message,
                     mail_to=NEW_DATA_DAILY_RECIPIENT)


def main():
    alert_upon_new_data()


if __name__ == "__main__":
    main()
