from unittest import mock, TestCase
import datetime
from .utils import JobTest, TEST_FILE_STORE
from iea_scraper.core.utils import calc_checksum_download, get_db_source_dict
from iea_scraper.settings import PROXY_DICT




class TestDownloadAndGetChecksum(TestCase):

    def setUp(self):
        self.job = JobTest()
        self.job.get_sources()

    def test_launch_download_for_sources_and_complements(self):
        with mock.patch('scraper.core.job.Job.download_source') as down:
            self.job.download_and_get_checksum()
            assert down.called
            assert down.call_count == 2
 
    def test_checksum_called_once(self):
        with mock.patch('scraper.core.job.calc_checksum_download') as cksum:
            self.job.download_and_get_checksum()
            cksum.assert_called_once()

    
class TestCalcChecksumDownload(TestCase):

    @mock.patch('scraper.core.utils.FILE_STORE_PATH', TEST_FILE_STORE)
    def test_cksum(self):
        source = mock.Mock()
        source.path = 'cksum.txt'
        calc_checksum_download(source)
        self.assertEqual(source.checksum, "900150983cd24fb0d6963f7d28e17f72")


class TestDownloadSource(TestCase):

    def setUp(self):
        self.job = JobTest()
        self.job.get_sources()

    def test_launch_download(self):
        with mock.patch('requests.get') as req:
            self.job.download_source(self.job.sources[0])
            req.assert_called_once_with('http://b', proxies=PROXY_DICT) 

