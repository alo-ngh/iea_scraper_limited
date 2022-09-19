from unittest import mock, TestCase
from iea_scraper.core.utils import get_dimension_db_data
from iea_scraper.settings import API_END_POINT

class GetDimensionDbData(TestCase):
    
    def test_basic(self):
        data = get_dimension_db_data('product')
        assert len(data) > 0, f"Error with {data}"
        assert type(data[0]) == dict
        assert data[0].get('code') is not None, "All product should have a code"
        assert data[0].get('long_name') is not None, "All product should have a long_name"
        get_dimension_db_data.cache_clear()

    def test_query(self):
        with mock.patch('requests.get') as req:
            get_dimension_db_data('area', "code=FRANCE")
            req.assert_called_once_with(f"{API_END_POINT}/dimension/area?code=FRANCE")
            get_dimension_db_data.cache_clear()
        data = get_dimension_db_data('area', "code=FRANCE")
        assert data[0]['iso_alpha_3'] == "FRA", str(data[0])



