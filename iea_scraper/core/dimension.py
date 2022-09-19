import requests

from iea_scraper.settings import API_END_POINT
from iea_scraper.core.utils import parallelize

import logging

logger = logging.getLogger(__name__)

class Updater:
    """Allows to automatically update a dynamic dim based on the transformation
        made by a job. TO USE WITH CAUTION (especially for detail, mapping)
        Example:
            >>> updater = Updater(MyJob, 'my_dim')
            >>> updater.run()
    """

    def __init__(self, job, dimension):
        self.dimension = dimension
        self.job = job
        self.job.remove_existing_dynamic_dim = self.pass_function


    def run(self, download=True):
        job = self.job()
        job.get_sources()
        job.download_and_get_checksum(download)
        job.transform()
        data = job.dynamic_dim[self.dimension]
        a = self.update(data)
        return data

    def pass_function(self, dimension):
        pass

    def update(self, data):
        a = parallelize(self.update_one, data , 10)
        return

    def update_one(self, elem):
        endpoint = f"{API_END_POINT}/dimension/{self.dimension}"
        r = requests.get(f"{endpoint}?code={elem['code']}")
        _id = r.json()[0]['id']
        r = requests.put(f"{endpoint}/{_id}", json=elem)
        if r.status_code >= 300:
            logger.error(f"Issue for insertion of element {elem} \n {r.text}")
        return None
