
import pytz
import datetime

import pandas as pd

from .constants import CalendarException


def now(timezone):
    """Return the time now in the given timezone."""

    tz = pytz.timezone(timezone)
    return datetime.datetime.now(tz)


# The GTFS-R spec contains a file "calendar.txt" that defines:
#   - the service
#   - which days of the week it runs
#   - the start date and end date of that schedule (inclusive)
# The file "calendar_dates.txt" turns on or off services for specific days
# and operates as exceptions to what's in "calendar.txt"
# This function will combine the two and produce a full expanded dataframe
# of dates each service will run on.
def _expand_calendar_txt(calendar_df: pd.DataFrame,
                         from_date: datetime.date, to_date: datetime.date):
    """Expand the calendar.txt into an exhaustive list of dates that each service
    travels on. `from_date` and `to_date` are inclusive dates to calculate the
    expanded list of travel dates."""

    cal = calendar_df.copy().reset_index(names='service_id')

    def _services_running(my_date):
        """For a timestamp, returns a pandas series of whether the service
        is running on that day of the week."""

        running_on_date = cal[my_date.day_name().lower()]
        schedule_is_valid = (cal.start_date.le(my_date)) & \
                            (cal.end_date.ge(my_date))

        return running_on_date & schedule_is_valid

    dates = pd.date_range(start=from_date, end=to_date)
    expanded_df = pd.DataFrame({pd.to_datetime(d): _services_running(d) for d in dates})

    expanded_df['service_id'] = cal.service_id
    expanded_df = expanded_df.melt(id_vars='service_id', var_name='date', value_name='running')
    expanded_df = expanded_df[expanded_df.running].drop('running', axis=1).copy()
    expanded_df['date'] = pd.to_datetime(expanded_df['date'])

    return expanded_df



def build_service_calendar(calendar_df: pd.DataFrame, cal_exception_df: pd.DataFrame,
                           start_offset=-2, stop_offset=7):
    """Calculate the standard schedule based on calendar.txt, then apply the exceptions."""

    from_date = datetime.date.today() + datetime.timedelta(days=start_offset)
    to_date = datetime.date.today() + datetime.timedelta(days=stop_offset)

    # first generate the standard schedule
    cal = _expand_calendar_txt(calendar_df, from_date=from_date, to_date=to_date)

    # now filter the exception df for exceptions that occur within this period
    _period_selector = (cal_exception_df['date'].ge(pd.to_datetime(from_date)) &
                        cal_exception_df['date'].le(pd.to_datetime(to_date)))

    # add in the service additions
    _for_addition = cal_exception_df[_period_selector &
          (cal_exception_df.exception_type == CalendarException.ServiceAdded)].copy()
    _for_addition.drop('exception_type', axis=1, inplace=True)
    cal = pd.concat([cal, _for_addition])
    cal.reset_index(drop=True, inplace=True)

    # take away any removed services
    _for_removal = cal_exception_df[_period_selector &
          (cal_exception_df.exception_type == CalendarException.ServiceRemoved)].copy()
    _for_removal.drop('exception_type', axis=1, inplace=True)
    _remove_indices = cal[['service_id', 'date']].apply(tuple, axis=1).isin(
                                                    _for_removal.apply(tuple, axis=1))

    cal.drop(index=_remove_indices[_remove_indices].index, axis='rows', inplace=True)

    return cal

