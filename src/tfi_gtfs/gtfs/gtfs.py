
import os
import io
import logging
import threading

import pandas as pd
from typing import Optional

from .realtime_data import RealtimeData
from .static_assets import StaticAssets
from .downloader import DownloadAgent, ResponseType

from .. import settings


log = logging.getLogger(__name__)


REALTIME_HEADERS = {
    'Cache-Control': 'no-cache',
    'X-API-KEY': settings.API_KEY
}


class GTFS:
    """A wrapper for maintaining the latest GTFS-R static and live data."""

    def __init__(self, static_asset_url: str, realtime_data_url: str,
                 start=False, api_key_check=True):

        self._static_assets: Optional[StaticAssets] = None
        self._realtime_data: Optional[RealtimeData] = None

        self._static_asset_agent = Optional[DownloadAgent] = None
        self._realtime_data_agent = Optional[DownloadAgent] = None

        self._data_available = threading.Event()

        if api_key_check and settings.API_KEY is None:
            raise ValueError('API key must be set in environment variables')

        self._create_download_agents(static_asset_url, realtime_data_url)

        if start:
            self.start_agents()

    def _create_download_agents(self, static_asset_url, realtime_data_url):

        # configure the downloader for static assets
        self._static_asset_agent = DownloadAgent.auto_sleep('static assets',
                                                            static_asset_url)
        self._static_asset_agent.register_callback(self.new_static_assets,
                                                   ResponseType.Bytes)

        # configure the downloader for realtime assets
        self._realtime_data_agent = DownloadAgent.every_minute('realtime data',
                                                               realtime_data_url)
        self._realtime_data_agent.register_callback(self.new_realtime_data,
                                                    ResponseType.Bytes)
        self._realtime_data_agent.set_headers(REALTIME_HEADERS)

        # add a callback to set the data available event.
        self._static_asset_agent.register_callback(self._manage_data_available_event)
        self._realtime_data_agent.register_callback(self._manage_data_available_event)


    def start_agents(self):
        """Start the download agents."""

        self._static_asset_agent.start()
        self._realtime_data_agent.start()

    def new_static_assets(self, new_static_asset_zip: bytes):
        """Callback for an updated static asset Zip file."""

        zip_buffer = io.BytesIO(new_static_asset_zip)
        sa = StaticAssets(zip_buffer)
        log.info('Updating static assets')
        self._static_assets = sa

    def new_realtime_data(self, new_realtime_data: bytes):
        """Callback for an updated static asset Zip file."""

        rd = RealtimeData(new_realtime_data)
        log.debug('Updating realtime data')
        self._realtime_data = rd

    @property
    def realtime_dataframe(self) -> pd.DataFrame:
        return self._realtime_data.dataframe

    @property
    def static_assets(self) -> StaticAssets:
        return self._static_assets

    def _manage_data_available_event(self):
        """A callback to check whether both static assets and realtime data is
        available and set the Event() when both are."""

        if self.static_assets is not None and self.realtime_dataframe is not None:
            self._data_available.set()

    def wait_for_data_available(self, timeout=None):
        """Pause until both the static assets and realtime data are available."""
        self._data_available.wait(timeout)

    def stop_number_is_valid(self, stop_number: int) -> bool:
        return self.static_assets.stop_number_is_valid(stop_number)

    def stop_name(self, stop_number: int):
        return self.static_assets.stop_number_to_name(stop_number)


class CachedGTFS(GTFS):
    """A version of the GTFS class that only uses cached
    assets and never updates them. For debug purposes only."""

    def __init__(self, static_assets_path: str,
                       realtime_data_path: str):

        GTFS.__init__(self, '', '',
                      api_key_check=False)

        with open(static_assets_path, 'rb') as f:
            self.new_static_assets(f.read())

        with open(realtime_data_path, 'rb') as f:
            self.new_realtime_data(f.read())

        self._data_available.set()

    def _create_download_agents(self, *args):
        pass