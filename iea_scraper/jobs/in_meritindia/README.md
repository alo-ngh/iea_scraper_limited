## meritindia.in scraper

Scraper for downloading current electricity generation data for India.

source: http://meritindia.in
target: main.meritindia_data

## Instructions

### First execution

We need to load the history from file in_meritindia_history.csv,
plus all existing in_merit_india_*.html files, without downloading new data.

Run the following code:

````python
import iea_scraper.jobs.in_meritindia.in_elec_cons as job

myjob = job.Job(full_load=True)
myjob.run(download=False)
````

### Recurrent executions

No need to load full history, only newly downloaded data:

````python
import iea_scraper.jobs.in_meritindia.in_elec_cons as job

myjob = job.Job()
myjob.run()
````

Ideally you should schedule it to run every 5 minutes.