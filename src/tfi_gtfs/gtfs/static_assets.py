
import zipfile

import numpy as np
import pandas as pd
from typing import Optional



class StaticAssets:
    """A container to open and parse the static assets from TFI."""

    def __init__(self, path, lazy=False):
        self._path = path

        self._agencies: Optional[pd.DataFrame] = None
        self._routes: Optional[pd.DataFrame] = None
        self._calendar: Optional[pd.DataFrame] = None
        self._calendar_exceptions: Optional[pd.DataFrame] = None
        self._stops: Optional[pd.DataFrame] = None
        self._stop_times: Optional[pd.DataFrame] = None

        self.load_content()

    def load_content(self):
        """Parse all data from the zipped static asset file."""

        zf = zipfile.ZipFile(self._path, 'r')

        self._agencies = load_agencies(zf)
        self._routes = load_routes(zf)
        self._calendar = load_calendar(zf)
        self._calendar_exceptions = load_calendar(zf)
        self._stops = load_stops(zf)
        self._stop_times = load_stop_times(zf)


def load_agencies(zf: zipfile.ZipFile):
    """Load the "agencies.txt" file."""

    with zf.open('agency.txt', 'r') as f:
        return pd.read_csv(f, index_col='agency_id',
                           usecols=['agency_id', 'agency_name'],
                           dtype={'agency_id': int})


def load_routes(zf: zipfile.ZipFile):
    """Load the routes file."""

    with zf.open('routes.txt', 'r') as f:
        return pd.read_csv(f, index_col='route_id',
                           usecols=['route_id', 'agency_id',
                                    'route_short_name','route_long_name'],
                           dtype={'agency_id': int})


def load_calendar(zf: zipfile.ZipFile):
    """Load the calendar file."""

    with zf.open('calendar.txt', 'r') as f:
        return pd.read_csv(f, index_col='service_id', date_format='%Y%m%d',
                           parse_dates=['start_date', 'end_date'])


def load_calendar_exceptions(zf: zipfile.ZipFile):
    """Load the calendar exceptions from the zip."""
    # exception type = 1 means services added, 2 means service removed

    with zf.open('calendar_dates.txt', 'r') as f:
        return pd.read_csv(f, index_col='service_id',
                           date_format='%Y%m%d', parse_dates=['date'],
                           dtype={'exception_type': int})


def load_stops(zf: zipfile.ZipFile):
    """Load the stops from the zip."""

    with zf.open('stops.txt', 'r') as f:
        df = pd.read_csv(f, usecols=['stop_id','stop_code','stop_name',
                                     'stop_lat','stop_lon'],
                            dtype={'stop_lat': np.float32, 'stop_lon': np.float32})

    # create a new index column that uses the irish stop number (on bus stops),
    # or the stop_id in case it's missing (for NI bus stops).
    new_index = df.stop_code.copy()
    new_index.where(new_index != 0, df.stop_id, inplace=True)
    df.index = new_index.values

    new_index.name = 'stop_code_or_id'

    return df


def load_stop_times(zf: zipfile.ZipFile):
    """Load the stop times from the zip."""

    with zf.open('stop_times.txt', 'r') as f:
        return pd.read_csv(f, usecols=['trip_id', 'departure_time' ,'stop_id'],
                              dtype={'trip_id': 'category', 'stop_id': 'category'},
                              date_format='%H:%M:$S', parse_dates=['departure_time'])


def load_trips(zf: zipfile.ZipFile):
    """Load the trips.txt from the zip."""

    with zf.open('trips.txt', 'r') as f:
        return pd.read_csv(f, usecols=['route_id','service_id','trip_id','trip_headsign'],
                              dtype={'route_id': 'category', 'trip_id': 'category',
                                     'service_id': int})

