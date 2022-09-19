from iea_scraper.core.utils import config_logging
from iea_scraper.settings import (LOGGING_DAILY)
from iea_scraper.jobs.utils import scrape_and_report, send_report, reorganize_cci
import logging
config_logging(LOGGING_DAILY)
logger = logging.getLogger()

# List of jobs to execute:
#    provider_code corresponds to the job package under scraper.jobs
#    source_code corresponds to the job module under scraper.jobs.<provider_code>
#    the job class name is inferred from source_code. Example: mx_oil_prod -> MxOilProdJob
job_list = [
    {'provider_code': 'org_jodidata', 'source_code': 'world_csv'},
    {'provider_code': 'mx_gob_cnh', 'source_code': 'mx_oil_prod'},
    {'provider_code': 'no_npd', 'source_code': 'fact_pages'},
    {'provider_code': 'br_gov_anp', 'source_code': 'br_oil_prod'},
    {'provider_code': 'uk_co_nstauthority', 'source_code': 'uk_oil_prod'},
    {'provider_code': 'gov_bsee', 'source_code': 'deep_qual_fields'},
    {'provider_code': 'gov_bsee', 'source_code': 'ogor_a'},
    {'provider_code': 'gov_eia', 'source_code': 'bulk_intl'},
    {'provider_code': 'gov_eia', 'source_code': 'bulk_pet_ng'},
    {'provider_code': 'kz_iacng', 'source_code': 'kz_daily_oil_prod'},
    {'provider_code': 'dk_ens', 'source_code': 'dk_oil_prod'},
    {'provider_code': 'nz_govt_mbie', 'source_code': 'nz_oil_stats'},
    {'provider_code': 'ca_gc_statcan', 'source_code': 'ca_oil_prod'},
    {'provider_code': 'om_gov_data', 'source_code': 'om_oil_cond_prod'},
    {'provider_code': 'com_google', 'source_code': 'global_mobility'},
    {'provider_code': 'in_gov_ppac', 'source_code': 'in_crudeoil_proc'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_oil_trade'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_oil_supply'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_cond_supply'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_oil_refinery'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_oilproducts_demand'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_lpg_demand'},
    {'provider_code': 'th_go_eppo', 'source_code': 'th_oilproducts_trade'},
    {'provider_code': 'gov_noaa_ncei', 'source_code': 'gsod'},
    {'provider_code': 'com_theice', 'source_code': 'ice_hist_data'},
    {'provider_code': 'gov_cftc', 'source_code': 'futures_and_options_comb'},
    {'provider_code': 'com_argusmedia', 'source_code': 'argus_prices'},
    {'provider_code': 'ca_gc_cer-rec', 'source_code': 'crude_oil_exports_by_rail'},
    {'provider_code': 'in_gov_ppac', 'source_code': 'indian_oil_deliveries'},
    {'provider_code': 'uk_gov', 'source_code': 'uk_road_fuel_sales'},
    {'provider_code': 'ng_gov_dpr', 'source_code': 'nigerian_oil_supply'},
    {'provider_code': 'fr_cerema', 'source_code': 'french_vehicle_traffic'},
    {'provider_code': 'gh_gov_petrocom', 'source_code': 'ghana_oil_supply'},
    {'provider_code': 'com_platts_fujairah', 'source_code': 'platts_fujairah_stocks'},
    {'provider_code': 'com_platts_fujairah', 'source_code': 'platts_fujairah_bunkers'},
    {'provider_code': 'com_oxfordeconomics', 'source_code': 'oxford_economics_api'},
    {'provider_code': 'com_theice', 'source_code': 'ice_futures'},
    {'provider_code': 'com_cmegroup', 'source_code': 'cme_futures'},
    {'provider_code': 'com_boursorama', 'source_code': 'forex'},
    {'provider_code': 'com_ashurst', 'source_code': 'russia_sanctions'}
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
<h2>IEA-External-DB Daily Load Report</h2>
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
    send_report(report, "[IEA-External-DB] Daily Extractions Report", html_report)

    # reorganize CCI of FACT_datapoint
    reorganize_cci('main', 'FACT_datapoint')


if __name__ == "__main__":
    main()
