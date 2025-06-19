
import zipfile
import unittest

import pandas as pd

from tfi_gtfs.gtfs import StaticAssets

from tfi_gtfs.gtfs import load_routes
from tfi_gtfs.gtfs import load_agencies

from tfi_gtfs.gtfs import load_trips, load_stops, load_stop_times
from tfi_gtfs.gtfs import load_calendar, load_calendar_exceptions
from tfi_gtfs.gtfs import build_service_calendar


STATIC_ASSETS = '../tests/GTFS.zip'


class StaticAssetsTestCase(unittest.TestCase):
    """Test the parsing of the static assets."""

    def setUp(self):
        self.zf = zipfile.ZipFile(STATIC_ASSETS)

    def test_agencies(self):
        agencies = load_agencies(self.zf)
        self.assertEqual(len(agencies.columns), 1)
        self.assertEqual(agencies.index.name, 'agency_id')
        self.assertEqual(agencies.columns[0], 'agency_name')

    def test_routes(self):
        routes = load_routes(self.zf)
        self.assertEqual(len(routes.columns), 3)
        self.assertEqual(routes.index.name, 'route_id')
        self.assertEqual(routes.columns[0], 'agency_id')
        self.assertEqual(routes.columns[1], 'route_short_name')
        self.assertEqual(routes.columns[2], 'route_long_name')

    def test_calendar(self):
        calendar = load_calendar(self.zf)
        self.assertEqual(len(calendar.columns), 9)
        self.assertEqual(calendar.index.name, 'service_id')

        for DoW in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
            self.assertIn(DoW, calendar.columns)

    def test_calendar_exceptions(self):
        cal_exc = load_calendar_exceptions(self.zf)
        self.assertEqual(len(cal_exc.columns), 2)
        self.assertEqual(cal_exc.index.name, 'service_id')

        self.assertEqual(cal_exc.columns[0], 'date')
        self.assertEqual(cal_exc.columns[1], 'exception_type')

    def test_stops(self):
        stops = load_stops(self.zf)
        self.assertEqual(len(stops.columns), 5)

        for col in ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon']:
            self.assertIn(col, stops.columns)

    def test_stop_times(self):
        stop_times = load_stop_times(self.zf)
        self.assertEqual(len(stop_times.columns), 3)

        self.assertEqual(stop_times.columns[0], 'trip_id')
        self.assertEqual(stop_times.columns[1], 'departure_time')
        self.assertEqual(stop_times.columns[2], 'stop_id')

    def test_trips(self):
        trips = load_trips(self.zf)
        self.assertEqual(len(trips.columns), 4)

        self.assertEqual(trips.columns[0], 'route_id')
        self.assertEqual(trips.columns[1], 'service_id')
        self.assertEqual(trips.columns[2], 'trip_id')
        self.assertEqual(trips.columns[3], 'trip_headsign')

    def test_convenience_methods(self):
        sa = StaticAssets(STATIC_ASSETS)

        self.assertTrue(sa.stop_number_is_valid(271))
        self.assertFalse(sa.stop_number_is_valid(999999))
        self.assertEqual(sa.stop_number_to_name(271), "O'Connell Street Lower")
        self.assertEqual(sa.stop_number_to_id(271), "8220DB000271")

    def test_expanded_calendar(self):
        cal = load_calendar(self.zf)
        cal_exc = load_calendar_exceptions(self.zf)

        expanded_cal = build_service_calendar(cal, cal_exc)
        self.assertIsInstance(expanded_cal, pd.DataFrame)

    def test_full_import(self):
        sa = StaticAssets(STATIC_ASSETS)
        # success if no exceptions thrown.