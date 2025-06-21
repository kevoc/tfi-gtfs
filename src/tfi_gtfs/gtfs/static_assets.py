
import io
import zipfile

import numpy as np
import pandas as pd
from typing import Optional

from .utils import timed_function, OnSchedule
from .calendar_tools import build_service_calendar, now

# these are day offsets from today, the static schedule will only be
# calculated for this period of time.
SCHEDULE_START = -2
SCHEDULE_END = 7
SCHEDULE_REFRESH = 1   # refresh the schedule every day

class StaticAssets:
    """A container to open and parse the static assets from TFI."""

    @classmethod
    def from_file(cls, path):
        """Create an instance of StaticAssets from a file on disk."""

        with open(path, 'rb') as f:
            return cls(f.read())

    def __init__(self, gtfs_zip_file_bytes: bytes):
        self._agencies: Optional[pd.DataFrame] = None
        self._routes: Optional[pd.DataFrame] = None
        self._calendar: Optional[pd.DataFrame] = None
        self._calendar_exceptions: Optional[pd.DataFrame] = None
        self._stops: Optional[pd.DataFrame] = None
        self._stop_times: Optional[pd.DataFrame] = None
        self._trips: Optional[pd.DataFrame] = None

        self._stop_times_by_id: Optional[pd.DataFrame] = None

        self._timezone: Optional[str] = None

        self._expanded_calendar: Optional[pd.DataFrame] = None
        self._cal_refresh = OnSchedule(self._build_expanded_calendar,
                                       every=86400 * SCHEDULE_REFRESH)

        self.load_content(gtfs_zip_file_bytes)

    def __del__(self):
        self._cal_refresh.stop()

    @timed_function
    def load_content(self, gtfs_zip_file_bytes: bytes):
        """Parse all data from the zipped static asset file."""

        zip_buffer = io.BytesIO(gtfs_zip_file_bytes)
        zf = zipfile.ZipFile(zip_buffer)

        self._agencies = load_agencies(zf)
        self._routes = load_routes(zf)
        self._calendar = load_calendar(zf)
        self._calendar_exceptions = load_calendar_exceptions(zf)
        self._stops = load_stops(zf)
        self._stop_times = load_stop_times(zf)
        self._trips = load_trips(zf)

        self._stop_times_by_id = self._stop_times.groupby('stop_id', observed=True)

        # to filter the dataset correctly, we need to know the local time,
        # for which we need to be timezone aware. Take the first timezone.
        self._timezone = self._agencies.agency_timezone.iloc[0]

    def _build_expanded_calendar(self):
        """This function rebuilds the expanded calendar."""

        self._expanded_calendar = build_service_calendar(
            self._calendar, self._calendar_exceptions,
            start_offset=SCHEDULE_START, stop_offset=SCHEDULE_END)

    def _update_expanded_calendar(self):
        """This function rebuilds the expanded calendar."""

        self._expanded_calendar = build_service_calendar(
            self._calendar, self._calendar_exceptions,
            start_offset=SCHEDULE_START, stop_offset=SCHEDULE_END)

    def stop_number_is_valid(self, stop_number: int):
        return stop_number in self._stops.index

    def stop_number_to_name(self, stop_number: int):
        return self._stops.loc[stop_number].stop_name

    def stop_number_to_id(self, stop_number: int):
        return self._stops.loc[stop_number].stop_id

    def _stop_times_for_stop_number(self, stop_number: int) -> pd.DataFrame:
        return self._stop_times_by_id.get_group(self.stop_number_to_id(stop_number))

    @property
    def agencies(self) -> pd.DataFrame:
        return self._agencies

    @property
    def routes(self) -> pd.DataFrame:
        return self._routes

    @property
    def calendar(self) -> pd.DataFrame:
        return self._calendar

    @property
    def calendar_exceptions(self) -> pd.DataFrame:
        return self._calendar_exceptions

    @property
    def expanded_calendar(self) -> pd.DataFrame:
        return self._expanded_calendar

    @property
    def stops(self) -> pd.DataFrame:
        return self._stops

    @property
    def stop_times(self) -> pd.DataFrame:
        return self._stop_times

    @property
    def trips(self) -> pd.DataFrame:
        return self._trips



def load_agencies(zf: zipfile.ZipFile):
    """Load the "agencies.txt" file."""

    with zf.open('agency.txt', 'r') as f:
        return pd.read_csv(f, index_col='agency_id',
                           usecols=['agency_id', 'agency_name', 'agency_timezone'],
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

    with zf.open('calendar_dates.txt', 'r') as f:
        return pd.read_csv(f, date_format='%Y%m%d', parse_dates=['date'],
                           dtype={'exception_type': int})


def load_stops(zf: zipfile.ZipFile):
    """Load the stops from the zip."""

    with zf.open('stops.txt', 'r') as f:
        df = pd.read_csv(f, usecols=['stop_id','stop_code','stop_name',
                                     'stop_lat','stop_lon'],
                            dtype={'stop_lat': np.float32, 'stop_lon': np.float32})

    # return the df with the index being the irish stop number (on bus stops)
    return df.set_index('stop_code', drop=True)


def load_stop_times(zf: zipfile.ZipFile):
    """Load the stop times from the zip."""

    with zf.open('stop_times.txt', 'r') as f:
        df = pd.read_csv(f, usecols=['trip_id', 'departure_time' ,'stop_id'],
                            dtype={'trip_id': 'category', 'departure_time': str,
                                   'stop_id': 'category'})
    df['departure_time'] = pd.to_timedelta(df['departure_time'])

    return df


def load_trips(zf: zipfile.ZipFile):
    """Load the trips.txt from the zip."""

    with zf.open('trips.txt', 'r') as f:
        return pd.read_csv(f, usecols=['route_id','service_id','trip_id','trip_headsign'],
                              dtype={'route_id': 'category', 'trip_id': 'category',
                                     'service_id': int})
