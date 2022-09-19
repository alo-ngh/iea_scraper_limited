import logging
import importlib
from .job import BaseJob

BASE_PACKAGE = 'iea_scraper.jobs'

logger = logging.getLogger(__name__)


def get_scraper_job(provider_code: str,
                    source_code: str,
                    job_prefix: str = None,
                    **kwargs
                    ) -> BaseJob:
    """
    This method instantiates the corresponding scraper job for a given provider and source.

    It loads a module 'scraper.jobs.<provider_code>.<source_code>' and instantiate a class with
    <job_prefix>Job(<full_load>).

    Examples (for job scraper.jobs.br_gov_anp.job.BrazilOilProd):
    factory.get_scraper_job('br_gov_anp', 'job', job_prefix = 'BrazilOilProd')
    factory.get_scraper_job('br_gov_anp', 'job', job_prefix = 'BrazilOilProd', full_load=True)

    With new standardised job names (assuming the job to be scraper.jobs.br_gov_anp.oil_prod.Job:
    factory.get_scraper_job('br_gov_anp', 'oil_prod')

    :param provider_code: provider code, it should corresponds to an existing python package in scraper.jobs.
    :param source_code: source code, it should corresponds to a python module in scraper.jobs.<provider_code>.
    :param job_prefix: (optional) a prefix to compose to the name of the scraper's job class.
    If not present, the class is assumed to be called Job.
    :param **kwargs: Forward following parameters to class constructor.
    :return: An instance of the corresponding table checker class.
    """
    module_name: str = None
    class_name: str = None

    try:
        module_name = f"{BASE_PACKAGE}.{provider_code}.{source_code}"
        logger.debug(f'Loading module {module_name}')
        module = importlib.import_module(name=module_name)

        class_prefix = job_prefix
        if not job_prefix:
            # calculates a Job class prefix based on source_name
            # example: 'br_oil_prod' -> 'BrOilProd'
            class_prefix = source_code.replace('_', ' ').title().replace(' ', '')
        class_name = f"{class_prefix}Job"
        logger.debug(f'Getting class {class_name}')
        job_class: BaseJob = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        message = f'module {module_name}' if not class_name else f'class {class_name}'
        raise ValueError(f"Error when trying to load {message}") from e
    # returns a new instance of the class forwarding parameters
    return job_class(**kwargs)
