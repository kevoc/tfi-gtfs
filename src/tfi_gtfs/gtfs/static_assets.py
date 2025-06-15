
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
        self._stops: Optional[pd.DataFrame] = None
        self._stop_times: Optional[pd.DataFrame] = None

        self.load_content()

    def load_content(self):
        """Parse all data from the zipped static asset file."""

        zf = zipfile.ZipFile(self._path, 'r')
        self._agencies = load_agencies(zf)


def load_agencies(zf: zipfile.ZipFile):
    """Load the "agencies.txt" file."""

    with zf.open('agency.txt', 'r') as f:
        return pd.read_csv(f, index_col='agency_id',
                           usecols=['agency_id', 'agency_name'],
                           dtype={'agency_id': int})


def load_routes(zf: zipfile.ZipFile):
    """Load the "agencies.txt" file."""

    with zf.open('routes.txt', 'r') as f:
        return pd.read_csv(f, index_col='route_id',
                           usecols=['route_id', 'agency_id',
                                    'route_short_name','route_long_name'],
                           dtype={'agency_id': int})


def load_calendar(zf: zipfile.ZipFile):
    """Load the "agencies.txt" file."""

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


