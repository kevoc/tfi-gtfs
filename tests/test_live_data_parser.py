
import unittest

from tfi_gtfs.gtfs import LiveData


LIVE_DATA = '../tests/live_data.bin'


class StaticAssetsTestCase(unittest.TestCase):
    """Test the parsing of the static assets."""

    def setUp(self):
        with open(LIVE_DATA, 'rb') as f:
            self.live_data = LiveData(f.read())

    def test_header(self):
        self.assertIsInstance(self.live_data.timestamp, int)

    def test_dataframe_export(self):
        self.live_data.dataframe.to_csv('live_data.csv', index=False)
