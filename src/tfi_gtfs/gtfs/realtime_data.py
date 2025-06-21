
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2 as gtfsr

from tfi_gtfs.gtfs.utils import timed_function


class RealtimeData:
    """A container to store all live data from the TFI API"""

    def __init__(self, feed_bytes: bytes):
        self._feed = gtfsr.FeedMessage()
        self._feed.ParseFromString(feed_bytes)
        self._df = pd.DataFrame(self._iter_trip_updates())

    @property
    def timestamp(self) -> int:
        """The current Unix timestamp, no timezone offset."""
        return self._feed.header.timestamp

    @property
    def dataset_type(self):
        return self._feed.header.incrementality

    @property
    def version(self):
        return self._feed.header.gtfs_realtime_version

    def _entities(self):
        return iter(self._feed.entity)

    def _iter_trip_updates(self):
        for entity in self._entities():
            yield from _trip_update_series_generator(entity)

    @property
    def dataframe(self) -> pd.DataFrame:
        return self._df


def _to_timestamp(start_date, start_time):
    """Convert the start date and time strings to a timestamp object."""

    return datetime.strptime(f'{start_date} {start_time}',
                             '%Y%m%d %H:%M:%S')


def _trip_update_series_generator(entity):
    """Flatten an entity with trip_updates to pandas series,
    yielding them as they come."""

    # one trip update can contain multiple updates for all the stops along that route

    for update in entity.trip_update.stop_time_update:
        yield pd.Series({'id': entity.id,
                         'trip_id': entity.trip_update.trip.trip_id,
                         'route_id': entity.trip_update.trip.route_id,
                         'vehicle_id': entity.trip_update.vehicle.id,
                         'start': _to_timestamp(entity.trip_update.trip.start_date,
                                                entity.trip_update.trip.start_time),
                         'trip_sched_type': entity.trip_update.trip.schedule_relationship,

                         # stop specific items
                         'stop_id': update.stop_id,
                         'stop_sched_type': update.schedule_relationship,
                         'arrival_delay': update.arrival.delay,
                         'departure_delay': update.departure.delay
                         })
