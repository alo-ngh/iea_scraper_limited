from iea_scraper.core.utils import send_message, config_logging
from iea_scraper.core import factory
from iea_scraper.settings import EXT_DB_STR, NEW_DATA_WEEKLY_RECIPIENT, LOGGING_WEEKLY

import datetime
import pandas as pd
from sqlalchemy import create_engine

import logging

config_logging(LOGGING_WEEKLY)
logger = logging.getLogger()
report = []

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
<h2>IEA-External-DB Weekly Load Report</h2>
<p>
<p>Please find below the status of the weekly extractions:</p>

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


def scrape_and_report(provider_code, source_code):
    """
    Run the scraper and report the result.
    :param provider_code: the provider code. It should correspond to the package name of the scraper job.
    :param source_code: the source code. It should correspond to the module name of the scraper job.
    :return:
    """
    global report
    try:
        status = dict()
        # The factory will load the module and instantiate scraper class accordingly
        job = factory.get_scraper_job(provider_code, source_code)
        logger.info(f"Start Job {job.title}")
        job.run()

        status["timestamp"] = datetime.datetime.now().isoformat()
        status["process"] = job.title
        status["status"] = "OK"

    except Exception as e:
        logger.exception(f"Error Job {job.title} \n {e}")
        status["timestamp"] = datetime.datetime.now().isoformat()
        status["process"] = job.title
        status["status"] = "ERROR"
        send_message(f"{job.title.upper()} ERROR", f"Error: in {job.__class__.__name__} \n"
                     f"Error: {str(e)[:10000]}")
    finally:
        report.append(status)


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
    where s.code = 'com_argusmedia_ru_ref_output'
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
                     mail_to=NEW_DATA_WEEKLY_RECIPIENT)


def send_weekly_report():
    global report

    df = pd.DataFrame(report)
    df = df[["timestamp", "process", "status"]]
    table = df.to_html(index=False)
    html_content = html_report.replace("@@@TABLE@@@", table)
    send_message(subject="[IEA-External-DB] Weekly Extractions Report", html_content=html_content)


def main():
    scrape_and_report('com_argusmedia', 'ru_ref_output')

    send_weekly_report()
    alert_upon_new_data()


if __name__ == "__main__":
    main()
