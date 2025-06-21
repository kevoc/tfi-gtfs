
"""Downloader is a threaded agent to download updates for a
 web resource on a schedule, execute a callback when an update
 is available, and re-download failed resources."""

import logging
import threading

# When creating your callback function, if the data received is
# invalid, your function must throw an exception, that will be
# caught by the DownloadAgent callback executor. This is how the
# agent knows the download was unsuccessful, and the download will
# be retried.
#
# DownloadAgent can download a resource on a schedule, like every
# minute or every hour. It can also use the Etag, Expires and
# Cache-Control headers to know when the remote resource has
# updated, and when the next version should be downloaded.
#

import re
import time

import requests

from enum import IntEnum
from typing import Optional, Callable, Union
from datetime import datetime, timedelta

from requests import RequestException
from requests.structures import CaseInsensitiveDict

from .utils import next_scheduled_exec_time
from .utils import seconds_until_timestamp, clip_at_zero

# if the DownloadAgent is in automatic mode, where it uses the
# Expires/Cache-Control header to know when to download a new
# version of the resource, if the correct headers aren't returned
# and the Agent doesn't know how long to sleep, it will sleep for
# the default wait time.
DEFAULT_WAIT = 3600   # 1 hour


# if an update is attempted, but it fails, an exponential backoff
# sleep time will be used, up to a maximum of this value.
EXP_BACKOFF_MAX_WAIT = 60  # 1 minutes


log = logging.getLogger(__name__)


class ResponseType(IntEnum):
    """A response selector for callback function."""

    NoData = 0
    Bytes = 1
    Text = 2
    JsonDecoded = 3


class DownloadAgent:
    """An agent to download web resources on a schedule."""

    def __init__(self, name: str, url: str, start: Optional[datetime],
                 period: Optional[timedelta]):

        self._name = name
        self._url = url
        self._exec_time = start
        self._period = period

        self._headers = {}
        self._callbacks = []

        self._last_response: Optional[requests.Response] = None
        self._error_wait = 0

        self._agent_thread = threading.Thread(target=self._run)
        self._agent_thread.name = f'{name}-thread'
        self._agent_thread.daemon = True

    @classmethod
    def every_n_minutes(cls, name: str, url: str, minutes: int):
        return cls(name, url, datetime.now(), timedelta(minutes=minutes))

    @classmethod
    def every_n_hours(cls,name: str, url: str, hours: int, at_minute: Optional[int]=None):
        start_ts = datetime.now().replace(minute=at_minute, second=0) \
                if at_minute is not None else datetime.now()
        return cls(name, url, start_ts, timedelta(hours=hours))

    @classmethod
    def every_n_days(cls, name: str, url: str, days: int, at_hour: Optional[int]=None,
                     at_minute: Optional[int]=None):
        if at_hour is not None or at_minute is not None:
            start_ts = datetime.now().replace(hour=at_hour or 0, minute=at_minute or 0, second=0)
        else:
            start_ts = datetime.now()
        return cls(name, url, start_ts, timedelta(days=days))

    @classmethod
    def every_minute(cls, name: str, url: str):
        return cls.every_n_minutes(name, url, 1)

    @classmethod
    def hourly(cls, name: str, url: str, at_minute: Optional[int]=None):
        return cls.every_n_hours(name, url, hours=1, at_minute=at_minute)

    @classmethod
    def daily(cls, name: str, url: str, at_hour: Optional[int]=None, at_minute: Optional[int]=None):
        return cls.every_n_days(name, url, at_hour, at_minute)

    @classmethod
    def auto_sleep(cls, name: str, url: str):
        """Uses the "Expires" header or the "Cache-Control" header to
        determine how long to sleep before re-querying the resource."""
        return cls(name, url, None, None)

    def register_callback(self, function: Callable[[Union[bytes, str, dict]], None],
                          response_type: ResponseType = ResponseType.NoData):
        """Register a callback function to receive updated data when the agent
        has downloaded it."""

        self._callbacks.append((function, response_type))

    def _broadcast_update(self, response: requests.Response) -> bool:
        """Broadcast the response to all registered callbacks."""

        if len(self._callbacks) == 0:
            log.warning(f'No callbacks were registered for agent: {self._name}')
            return True

        success = True
        for callback, response_type in self._callbacks:
            try:
                if response_type == ResponseType.Bytes:
                    callback(response.content)
                elif response_type == ResponseType.Text:
                    callback(response.text)
                elif response_type == ResponseType.JsonDecoded:
                    callback(response.json())
                elif response_type == ResponseType.NoData:
                    callback()
            except Exception:
                log.error(f'while executing callback: {callback}\n', exc_info=True)
                success = False
            else:
                log.debug(f'Successfully executed callback for {callback}\n')

        return success

    def set_headers(self, headers: dict):
        """Set the headers to be used for the URL requests."""

        self._headers = headers

    @property
    def response_headers(self) -> CaseInsensitiveDict[str]:
        return self._last_response.headers

    def start(self):
        """Start the download agent."""

        self._agent_thread.start()

    def _wait(self):
        """Sleep until the next update is needed."""

        if self._exec_time is None and self._period is None:
            self._wait_using_expiry_and_cache_control()
        else:
            self._wait_using_schedule()

    def _wait_using_expiry_and_cache_control(self):
        """Use the latest cache control and expiry."""

        sleep_time = DEFAULT_WAIT
        cc_sleep = cache_control_sleep(self.response_headers)
        exp_sleep = expires_sleep(self.response_headers)

        log.info(f'{self._name} agent -> Cache-Control: {cc_sleep:.0f} secs., Expires: {exp_sleep:.0f} secs.')

        # based on https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Expires
        # if a Cache-Control directive exists, the expires header should be ignored.
        if cc_sleep > 0:
            sleep_time = cc_sleep
        if cc_sleep == 0 and exp_sleep > 0:
            sleep_time = exp_sleep

        log.info(f'{self._name} agent will sleep for {sleep_time:.1f} secs.')
        time.sleep(sleep_time)

    def _wait_using_schedule(self):
        """Use the latest cache control and expiry."""

        now = datetime.now()
        self._exec_time = next_scheduled_exec_time(self._exec_time, self._period)
        sleep_time = (self._exec_time - now).total_seconds()

        log.debug(f'{self._name} agent will sleep for {sleep_time:.1f} secs.')
        time.sleep(sleep_time)

    def _wait_after_error(self):
        """When an error occurs, wait for a certain cooling off period
         until trying again, the wait time backs off exponentially. If
         a 429 error was returned, wait the max time immediately."""

        if self._last_response.status_code == 429:
            log.error('Too many requests, using max exponential backoff wait...')
            cur_wait = EXP_BACKOFF_MAX_WAIT
        else:
            cur_wait = max([self._error_wait, 1])

        self._error_wait = min([cur_wait * 2, EXP_BACKOFF_MAX_WAIT])

        log.error(f'{self._name} agent waiting {cur_wait} secs. before retrying...')
        time.sleep(cur_wait)

    def _reset_error_wait(self):
        self._error_wait = 0

    @property
    def resource_has_etag(self):
        """Returns true if the remote resource has an Etag header"""
        return self.response_headers is not None and 'Etag' in self.response_headers

    def resource_headers(self):
        """Returns the headers for the resource, using a HEAD request."""

        try:
            head = requests.head(self._url, headers=self._headers)
            return head.headers
        except requests.RequestException:
            return {}

    @property
    def etag_header(self):
        return self.resource_headers().get('Etag')

    def resource_needs_update(self):
        """Return false if the resource has an Etag and it shows the resource
        is unchanged."""

        if self.resource_has_etag:
            headers = self.resource_headers()
            current_etag = headers.get('Etag')
            old_etag = self.response_headers.get('Etag')
            if current_etag == old_etag:
                log.warning(f'{self._name} agent may have woken up too early.')
                log.warning(f'   -> Etag is unchanged: {current_etag}')
                log.warning(f'      remote resource will not be updated...')
                return False

        return True

    def _update(self) -> bool:
        """Run an update of the remote resource."""

        try:
            self._last_response = requests.get(self._url, headers=self._headers)
            self._last_response.raise_for_status()
        except RequestException as e:
            log.error(f'{self._name} agent encountered an exception:\n', exc_info=True)
            return False

        return self._broadcast_update(self._last_response)

    def _run(self):
        """Main agent thread."""

        while True:
            if self.resource_needs_update:
                success = self._update()

                if success:
                    self._reset_error_wait()
                else:
                    self._wait_after_error()
                    continue

            self._wait()


def cache_control_sleep(headers):
    """Return the number of seconds to sleep based on the cache control headers"""

    cc_header = headers.get('Cache-Control').lower()

    if 'no-cache' in cc_header:
        return 0

    elif 'max-age' in cc_header:
        max_age = int(re.search(r'max-age=([0-9]+)', cc_header).group(1))

        if 'Age' in headers:
            age = int(headers.get('Age'))
            return clip_at_zero(max_age - age)
        elif 'Last-Modified' in headers:
            return clip_at_zero(seconds_until_timestamp(headers.get('Last-Modified'),
                                                        offset=max_age))

    # unable to determine how long to wait based on cache control headers
    return 0


def expires_sleep(headers):
    """return the number of seconds to sleep based on the expires header"""

    exp_header = headers.get('Expires')
    if exp_header == '0':
        return 0   # resource already expired

    return seconds_until_timestamp(exp_header)

