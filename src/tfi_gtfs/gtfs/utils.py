
import time


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