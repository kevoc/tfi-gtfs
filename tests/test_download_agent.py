
import unittest
from datetime import datetime, timedelta

from tfi_gtfs.gtfs import seconds_until_timestamp
from tfi_gtfs.gtfs import next_scheduled_exec_time

from tfi_gtfs.gtfs.downloader import DownloadAgent
from tfi_gtfs.gtfs.downloader import cache_control_sleep, expires_sleep


SAMPLE_HEADERS = {
    'Last-Modified': 'Wed, 18 Jun 2025 21:57:36 GMT',
    'Cache-Control': 'no-cache',
    'Age': '86400',
    'Expires': 'Sat, 21 Jun 2025 08:54:49 GMT'
}

GTFS_STATIC_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"



class dummy_datetime:
    @staticmethod
    def now():
        return datetime(2025, 6, 19, 14, 8, 26, 103150)

    strptime = datetime.strptime


# insert this dummy datetime object into the local variables of the
# seconds_until_timestamp() function so the results remain the
# same for the unittests.
seconds_until_timestamp.__globals__['datetime'] = dummy_datetime


class StaticAssetsTestCase(unittest.TestCase):
    """Test the parsing of the static assets."""

    def test_timestamp_parser(self):
        self.assertEqual(seconds_until_timestamp('Wed, BAD_TIMESTAMP 21:57:36 GMT'),
                         0)

    def test_exec_schedule(self):
        next_exec = next_scheduled_exec_time(
            last_exec_time=datetime(2025, 6, 19, 14, 0, 0),
            period=timedelta(minutes=2))

        self.assertEqual(next_exec, datetime(2025, 6, 19, 14, 10, 0))

    def test_cache_control(self):
        # Testing 'no-cache'
        cc_sleep = cache_control_sleep(SAMPLE_HEADERS)
        self.assertEqual(cc_sleep, 0)

        # Testing Age already expired
        SAMPLE_HEADERS['Cache-Control'] = 'max-age=80000'
        cc_sleep = cache_control_sleep(SAMPLE_HEADERS)
        self.assertEqual(cc_sleep, 0)

        # Testing Age with expiry in 10 seconds
        SAMPLE_HEADERS['Cache-Control'] = f'max-age={86400 + 10}'
        cc_sleep = cache_control_sleep(SAMPLE_HEADERS)
        self.assertEqual(cc_sleep, 10)

        # Testing Last-Modified with max-age already expired
        del SAMPLE_HEADERS['Age']
        SAMPLE_HEADERS['Cache-Control'] = 'max-age=3600'
        cc_sleep = cache_control_sleep(SAMPLE_HEADERS)
        self.assertEqual(cc_sleep, 0)

        # Testing Last-Modified with max-age expiring in the future
        SAMPLE_HEADERS['Cache-Control'] = 'max-age=100000'
        cc_sleep = cache_control_sleep(SAMPLE_HEADERS)
        self.assertEqual(round(cc_sleep, 0), 41750)

    def test_expires(self):
        exp_sleep = expires_sleep(SAMPLE_HEADERS)
        self.assertEqual(round(exp_sleep, 0), 153983)

    def test_Etag_header(self):
        da = DownloadAgent('test', GTFS_STATIC_URL, None, None)
        etag = da.etag_header
        self.assertIsInstance(etag, str)
        self.assertEqual(len(etag), 23)
