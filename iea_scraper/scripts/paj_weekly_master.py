from iea_scraper.core.utils import config_logging
from iea_scraper.settings import LOGGING_WEEKLY, NEW_DATA_PAJ_WEEKLY_RECIPIENT
from iea_scraper.jobs.utils import scrape_and_report, send_report

import logging

config_logging(LOGGING_WEEKLY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
job_list = [
    {'provider_code': 'jp_gr_paj', 'source_code': 'jp_oil_stats'}
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
<h2>IEA-External-DB PAJ Weekly Load Report</h2>
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


def main():
    # first add mail_to to the each dict entry, to force scrape_and_report()
    # mailing errors to settings.EDC_MAILING_LIST
    for d in job_list:
        d.update({'mail_to': NEW_DATA_PAJ_WEEKLY_RECIPIENT})

    # '**job_parameter' fills the parameters of scrape_and_report()
    # with entries from job_parameter dict (provider_code, source_code, mail_to)
    # where mail_to is optional and defaults to settings.MAIL_RECIPIENT if not present.
    report = [scrape_and_report(**job_parameter) for job_parameter in job_list]

    # send report based on list of status
    send_report(report=report,
                mail_subject="[IEA-External-DB] PAJ Weekly Extractions Report",
                html_template=html_report,
                mail_to=NEW_DATA_PAJ_WEEKLY_RECIPIENT)


if __name__ == "__main__":
    main()
