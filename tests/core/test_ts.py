
import unittest
from iea_scraper.core.ts import Normalizer

class TestNormalize(unittest.TestCase):

    def setUp(self):
        self.normalizer = Normalizer()

    def test_replace_nonascii_with_ws(self):
        assert self.normalizer.normalize("a-a") == "a a"

    def test_things(self):
        self.assertEqual(self.normalizer.normalize("CRUDE OIL"), "crude oil")
