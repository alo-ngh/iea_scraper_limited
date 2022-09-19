from iea_scraper.core.utils import config_logging
from iea_scraper.settings import (LOGGING_EDC_GOOGLE_TRENDS_DAILY, EDC_GOOGLE_TRENDS_MAILING_LIST, EDC_DAILY_GOOGLE_TRENDS_JOBS, MAIL_EDC_SENDER, EDC_MAILING_LIST)
from iea_scraper.jobs.utils import scrape_and_report, send_report

import logging

config_logging(LOGGING_EDC_GOOGLE_TRENDS_DAILY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
job_list = list(EDC_DAILY_GOOGLE_TRENDS_JOBS.values())

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
<h2>IEA-External-DB - EDC Google Trends Daily Load Report</h2>
<p>
<p>Please find below the status of the extractions for today:</p>

<div style="overflow-x:auto;">
 @@@TABLE@@@
</div>
<p/>
<p>EDC Data Scraping Team</p>
</body>
</html>
"""


def main():
    # first add mail_to to the each dict entry, to force scrape_and_report()
    # mailing errors to settings.EDC_MAILING_LIST
    for d in job_list:
        d.update({'mail_to': EDC_GOOGLE_TRENDS_MAILING_LIST})

    # '**job_parameter' fills the parameters of scrape_and_report()
    # with entries from job_parameter dict (provider_code, source_code, mail_to)
    # where mail_to is optional and defaults to settings.MAIL_RECIPIENT if not present.
    report = [scrape_and_report(**job_parameter) for job_parameter in job_list]

    # send report based on list of status
    send_report(report=report,
                mail_subject="[IEA-External-DB] EDC Daily Google Trends Extractions Report",
                html_template=html_report,
                mail_from=MAIL_EDC_SENDER,
                mail_to=EDC_MAILING_LIST)


if __name__ == "__main__":
    main()
