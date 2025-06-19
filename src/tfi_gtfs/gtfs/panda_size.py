
import io
import pandas as pd
from typing import Union
from typing import List, Tuple


class PandaSize:
    """A size estimator for pandas objects"""

    def __init__(self):
        self._objects: List[Tuple[str, pd.DataFrame]] = []
        self._done = False
        self._report = io.StringIO()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finish()

    def add(self, name, obj: Union[pd.DataFrame, pd.Series]):
        """Add a new object whose memory footprint should be added."""

        if self._done:
            raise RuntimeError('you cannot add more objects after exiting the context manager')

        if not isinstance(obj, (pd.Series, pd.DataFrame)):
            raise TypeError('only series and dataframes are supported')

        self._objects.append((name, obj))

    def _finish(self):
        """Do the final computation of the memory sizes."""

        total = 0
        for name, obj in self._objects:
            self._report.write(f'\n\n\n******** {name} ********\n')
            obj.info(buf=self._report, memory_usage='deep')

            size = obj.memory_usage(deep=True)
            total += size if isinstance(size, int) else size.sum()

        self._report.write(f'\n\nTotal: {total} bytes')

    @property
    def report(self):
        return self._report.getvalue().strip()


def pandas_series_and_frames_on_object(obj):
    """Generate a list of all private variables on the given
    object that are pandas dataframes or series."""

    for var_name, var_object in obj.__dict__.items():
        if isinstance(var_object, (pd.Series, pd.DataFrame)):
            yield var_name, var_object


def private_pandas_objs_on_obj(obj):
    """Return a dictionary with all private pandas Dataframes
    and series on that objects. Private, meaning their variable
    name starts with an _, and this underscore will be stripped
    in the returned dictionary."""

    d = {}
    for var_name, var_object in pandas_series_and_frames_on_object(obj):
        if var_name.startswith('_'):
            d[var_name.lstrip('_')] = var_object

    return d


def memory_report_from_private_pandas_objs(obj):
    """Return a dataframe showing the memory usage for all private
    dataframes and series on an object."""

    pandas_objects = private_pandas_objs_on_obj(obj)

    with PandaSize() as ps:
        for name, frame_or_series in pandas_objects.items():
            ps.add(name, frame_or_series)

    return ps.report
