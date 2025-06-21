
import unittest

from tfi_gtfs.gtfs import RealtimeData


REALTIME_DATA = '../tests/realtime_data.bin'


class RealtimeDataTestCase(unittest.TestCase):
    """Test the parsing of the static assets."""

    def setUp(self):
        with open(REALTIME_DATA, 'rb') as f:
            self.realtime_data = RealtimeData(f.read())

    def test_header(self):
        self.assertIsInstance(self.realtime_data.timestamp, int)

    def test_dataframe_export(self):
        self.realtime_data.dataframe.to_csv('realtime_data.csv', index=False)
