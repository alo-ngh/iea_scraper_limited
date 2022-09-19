from iea_scraper.core.utils import send_message, config_logging
from iea_scraper.jobs.utils import scrape_and_report, send_report
from iea_scraper.settings import EXT_DB_STR, NEW_DATA_CN_CUSTOMS_RECIPIENT, LOGGING_DAILY

import datetime
import pandas as pd
from sqlalchemy import create_engine

import logging

config_logging(LOGGING_DAILY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
job_list = [
    {'provider_code': 'cn_customs', 'source_code': 'cn_crude_imports'},
    {'provider_code': 'cn_customs', 'source_code': 'cn_oil_products'}
]

html_report = """
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
<h2>IEA-External-DB China Customs Load Report</h2>
<p>
<p>Please find below the status of the China Customs extractions:</p>

<div style="overflow-x:auto;">
 @@@TABLE@@@
</div>
<p/>
<p>OMR Team</p>
</body>
</html>
"""

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

<p>All sources are checked except EIA and Kazakhstan (that change everyday).</p>
<p/>
<p>For more information: <a href="http://vimars/powerbireports/powerbi/IEA%20External%20DB/Search"/>http://vimars/powerbireports/powerbi/IEA%20External%20DB/Search</a>.</p>

<p>OMR Team</p>
</body>
</html>
"""


def alert_upon_new_data():
    engine = create_engine(EXT_DB_STR)
    stmt = """select s.long_name as [source], d.period_fk as [period], d.[affected rows]
    from 
    (select source_fk, period_fk, count(*) [affected rows]
     from main.FACT_datapoint
     where date_created > Cast(GetDate() AS date) 
        or date_modified > Cast(GetDate() AS date)
     group by source_fk, period_fk) d
    inner join dimension.LU_source s
    on s.id = d.source_fk
    where s.code like 'cn_customs%'
    order by 2 desc
    """
    df = pd.read_sql(stmt, engine)

    logger.debug(f'len(df): {len(df)}')

    if len(df) > 0:
        logger.info('Sending new data alert e-mail.')
        table = df.to_html(index=False)
        message = html_new_data.replace("@@@TABLE@@@", table)

        send_message(subject="[IEA-External-DB] New Data Alert",
                     html_content=message,
                     mail_to=NEW_DATA_CN_CUSTOMS_RECIPIENT)


def main():
    # '**job_parameter' fills the parameters of scrape_and_report()
    # with entries from job_parameter dict (provider_code, source_code)
    report = [scrape_and_report(**job_parameter) for job_parameter in job_list]

    # send report based on list of status
    send_report(report, "[IEA-External-DB] China Customs Extractions Report", html_report)

    alert_upon_new_data()


if __name__ == "__main__":
    main()
