from pathlib import Path


def list_jobs():
    """
    List existing jobs.
    :return: NoReturn
    """

    path = Path('iea_scraper') / 'jobs'

    job_list = [f for f in path.glob("**/*.py") if f.stem not in ['__init__', 'utils']]
    print(f'Currently {len(job_list)} jobs are available:')

    for f in job_list:
        print(f' provider_code: {f.parent.name}, scraper_name: {f.stem}')


if __name__ == "__main__":
    list_jobs()
