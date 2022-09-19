# IEA Scraper

This project is intended to group all automated data extraction, transformation and loading processes from external data sources.
Main target is the [IEA External DB](https://gitlab.com/iea_data_lab/external_db) project.


## Structure
Basic structure of scraper
```
scraper
    - README.md 
    - __init__.py
    - settings.py
    - ...
    - core
        - __init__.py
        - ...
    - jobs
        - __init__.py
        - eia
            - __init__.py
            - eia.py
            - utils.py
            - ...
        - ... 
```

You should have 1 folder in scrapper by job. 1 job is group of sources that make sense to be downloaded/updated at the same time and/or with the same structure. You can have several jobs for 1 provider.

## Requirement
For each data provider you should:
1. Prepare sources information (lists sources + list urls + list path)
   1. Standard interface?
2. Download all files
   1. Update the download time source <- pass: need update api
3. Check if checksum equals <- pass: need update api 
4. Data insertion
   1. Insert dynamic sources
   2. Upsert data
   3. Update source <- pass: need update api

You need to be able to schedule the download on a regular basis.

You need to report the state, the situation, error messages.


In scrapper you need to create a file instance.py based on instance_backup.py.

You need to create the filestore folder.

## Doc

The Python documentation can be access in 
