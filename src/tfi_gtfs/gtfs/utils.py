
import time
import math
import logging
import threading

from typing import Callable
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def timed_function(func):
    """A timing decorator to print the runtime of a long runnings function."""

    def _wrapper(*args, **kwargs):
        start = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            duration = time.time() - start
            print(f'function "{func.__name__}()" took {duration:.3f} secs.')

    return _wrapper


def http_timestamp(timestamp: str):
    """Parse the HTTP timestamp and return a datetime obj."""
    return datetime.strptime(timestamp, '%a, %d %b %Y %H:%M:%S %Z')


def seconds_until_timestamp(timestamp: str, offset=0):
    """Parse the given timestamp, returning the number of seconds
    until that time, with an optional offset added. """

    try:
        datetime_obj = http_timestamp(timestamp)
    except ValueError:
        return 0
    else:
        # return the number of seconds to sleep, never negative
        offsetted = datetime_obj + timedelta(seconds=offset)
        return max([(offsetted - datetime.now()).total_seconds(), 0])


def clip_at_zero(my_number):
    """Never allow the given number to go below zero."""
    return max([my_number, 0])


def next_scheduled_exec_time(last_exec_time: datetime, period: timedelta):
    """Calculate the next timestamp for the execution, given the
    execution schedule should be integer multiples of period."""

    period_count = math.ceil((datetime.now() - last_exec_time) / period)
    return last_exec_time + (period_count * period)


class OnSchedule(threading.Thread):
    """Run a function on schedule forever, or until stopped."""

    def __init__(self, func: Callable, every: int, run_at_launch=False):
        threading.Thread.__init__(self)
        self._func = func
        self._every = every

        self._closing = threading.Event()
        self._run_at_launch = run_at_launch

        self.daemon = True
        self.start()

    def stop(self):
        log.debug(f'OnSchedule({self._func.__name__}()) signalled to close')
        self._closing.set()

    def wait(self):
        self._closing.wait(self._every)
        if self._closing.is_set():
            log.info(f'OnSchedule({self._func.__name__}()) update thread is exiting')
            raise KeyboardInterrupt('exit signalled')

    def run(self):
        if not self._run_at_launch:
            log.debug(f'OnSchedule({self._func.__name__}()) going to sleep before first execution')
            self.wait()

        while True:
            try:
                self._func()
            except Exception:
                log.error(f'OnSchedule({self._func.__name__}()) threw an exception:\n', exc_info=True)
            else:
                log.debug(f'OnSchedule({self._func.__name__}()) execution was successful!')

            self.wait()