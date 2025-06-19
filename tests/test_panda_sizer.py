
import unittest

from tfi_gtfs.gtfs import StaticAssets
from tfi_gtfs.gtfs import memory_report_from_private_pandas_objs

from test_static_asset_parser import STATIC_ASSETS


class StaticAssetsTestCase(unittest.TestCase):
    """Test the parsing of the static assets."""

    @unittest.skip('report only needed for debugging')
    def test_pandas_size(self):
        sa = StaticAssets(STATIC_ASSETS)
        report = memory_report_from_private_pandas_objs(sa)
        print(report)
