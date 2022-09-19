import logging
from pathlib import Path
from abc import ABCMeta
from iea_scraper.core.job import ExtDbApiJobV2

logger = logging.getLogger(__name__)


class BaseGovBseeJob(ExtDbApiJobV2, metaclass=ABCMeta):
    """
    Abstract base class for downloading data from bsee.gov.
    """

    job_code = Path(__file__).parent.parts[-1]
    area = 'USA'

    provider_code = job_code.upper()
    provider_long_name = f"{area} - Bureau of Safety and Environmental Enforcement"
    provider_url = "https://www.data.bsee.gov"

