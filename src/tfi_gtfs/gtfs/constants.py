
from enum import IntEnum


class CalendarException(IntEnum):
    ServiceAdded = 1
    ServiceRemoved = 2


# https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship-2
class Trip(IntEnum):
    Scheduled = 0
    Added = 1
    Unscheduled = 2
    Cancelled = 3


# https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship
class Stop(IntEnum):
    Scheduled = 0
    Skipped = 1
    NoData = 2
