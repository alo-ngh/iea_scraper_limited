from iea_scraper.core.utils import send_message, config_logging
from iea_scraper.jobs.utils import scrape_and_report, send_report
from iea_scraper.settings import (LOGGING_DAILY)

import logging

config_logging(LOGGING_DAILY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
# vessel_details, floating_storage and oil_in_transit disabled (need to reduce granularity)
#job_list = [
#    {'provider_code': 'com_kpler', 'source_code': 'opec_exports'},
#    {'provider_code': 'com_kpler', 'source_code': 'vessel_details'},
#    {'provider_code': 'com_kpler', 'source_code': 'floating_storage'},
#    {'provider_code': 'com_kpler', 'source_code': 'oil_in_transit'}
#    ]
job_list = [
    {'provider_code': 'com_kpler', 'source_code': 'opec_exports'},
    {'provider_code': 'com_kpler', 'source_code': 'russian_crudecond_exports'}
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
<h2>IEA-External-DB Kpler Daily Load Report</h2>
<p>
<p>Please find below the status of the extractions for today:</p>

<div style="overflow-x:auto;">
 @@@TABLE@@@
</div>
<p/>
<p>OMR Team</p>
</body>
</html>
"""


def main():
    # '**job_parameter' fills the parameters of scrape_and_report()
    # with entries from job_parameter dict (provider_code, source_code)
    report = [scrape_and_report(**job_parameter) for job_parameter in job_list]

    # send report based on list of status
    send_report(report, "[IEA-External-DB] Kpler Daily Extractions Report", html_report)


if __name__ == "__main__":
    main()
