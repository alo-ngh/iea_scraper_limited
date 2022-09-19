from iea_scraper.core.utils import config_logging
from iea_scraper.settings import LOGGING_EDC_HOURLY, EDC_MAILING_LIST, EDC_HOURLY_JOBS
from iea_scraper.jobs.utils import scrape_and_report, send_report
from datetime import datetime

import logging

config_logging(LOGGING_EDC_HOURLY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
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
<h2>IEA-External-DB - EDC Hourly Load Report</h2>
<p>
<p>Please find below the status of the extractions for the last hour:</p>

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
    for d in EDC_HOURLY_JOBS.values():
        d.update({'mail_to': EDC_MAILING_LIST})

    # '**job_parameter' fills the parameters of scrape_and_report()
    # with entries from job_parameter dict (provider_code, source_code)
    report = [scrape_and_report(**job_parameter) for job_parameter in EDC_HOURLY_JOBS.values()]
    now = datetime.now()
    if now.hour == 1:
        # send report based on list of status
        send_report(report=report,
                    mail_subject="[IEA-External-DB] EDC Hourly Extractions Report",
                    html_template=html_report,
                    mail_to=EDC_MAILING_LIST)


if __name__ == "__main__":
    main()
