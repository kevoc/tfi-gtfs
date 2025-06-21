
import time
import math
from datetime import datetime, timedelta

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