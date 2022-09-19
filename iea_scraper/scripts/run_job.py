import logging
import argparse

from iea_scraper.settings import LOGGING_DAILY
from iea_scraper.core.utils import config_logging
from iea_scraper.core import factory

config_logging(LOGGING_DAILY)
logger = logging.getLogger()


def parse_arguments():
    """
    Configures a parser for expected command line arguments, and parse command line parameters.
    :return: a dictionary with passed arguments.
    """
    logger.debug("Configuring argument parser")
    parser = argparse.ArgumentParser(description="Run a scraper job.")
    parser.add_argument("provider_code",
                        help="The python package name where the scraper is inside, under scraper.jobs.")
    parser.add_argument("scraper_code",
                        help="The python module (.py file) containing the desired scraper.")
    parser.add_argument("--full_load", action="store_true",
                        help="Run in 'full load' mode (load the full history). "
                             "Without this option, it will typically run in incremental mode.")
    parser.add_argument("--no_download", action="store_true",
                        help='Do not download files. Without this option, it download files.')
    parser.add_argument("--sequential_download", action="store_true",
                        help='Download files sequentially. '
                             'Without this option, it will typically download in parallel.')
    parser.add_argument("--verbose", action="store_true",
                        help="Shows more detailed information.")
    return parser.parse_args()


def main():
    """
    Main program.
    :return: NoReturn
    """
    args = parse_arguments()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.info(f'Loading scraper scraper.jobs.{args.provider_code}.{args.scraper_code} '
                f'{"in full load mode" if args.full_load else "in incremental mode"}.')
    job = factory.get_scraper_job(args.provider_code, args.scraper_code, full_load=args.full_load)

    download: bool = not args.no_download
    parallel_download: bool = not args.sequential_download
    log_msg = f'Running scraper {"without downloading files " if args.no_download else ""}'
    if not args.no_download:
        log_msg += f'{"(sequential download)" if args.sequential_download else "(parallel download)"}.'
    logger.info(log_msg)

    job.run(download=download, parallel_download=parallel_download)


if __name__ == "__main__":
    main()




