
import sys
import logging


# Some libraries debug log a lot of crap. Add the logger names
# here to exclude them from debug logging.
UNWANTED_LOGGERS = []


class LogFilter(logging.Filter):
    def __init__(self, debug: bool, verbose: bool):
        logging.Filter.__init__(self)
        self.debug_logging = debug
        self.verbose_logging = verbose

    def filter(self, record):
        verbose_only = getattr(record, 'verbose_only', False)

        if self.verbose_logging:
            return True   # everything is logged when verbose
        else:
            return (record.name not in UNWANTED_LOGGERS) and not verbose_only


# Adds the log level to the start of the message for WARNINGs and up.
class LevelFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno >= logging.WARNING:
            record.msg = f"{record.levelname} {record.msg}"
        return super().format(record)


def activate_logging_customisations(debug: bool, verbose: bool):
    """Add the filter/formatter to all existing handlers."""

    for handler in logging.getLogger('').handlers:
        handler.addFilter(LogFilter(debug, verbose))
        handler.setFormatter(LevelFormatter())


def log_to_stderr(debug=False, verbose=False):
    """Activate the logger to log to console."""

    # if debug==False and verbose==False -> you only see INFO and above
    # if debug==True and verbose==False -> you see DEBUG and above, but nothing from UNWANTED_LOGGERS
    # if verbose==True -> you see everything, along with logger names that
    #                           you can add to the UNWANTED_LOGGERS.

    if debug or verbose:
        if verbose:
            level, msg = logging.DEBUG, '%(name)-20s  %(levelname)-7s  '
        else:
            level, msg = logging.DEBUG, '%(levelname)-7s  '
    else:
        level, msg = logging.INFO, ''

    logging.basicConfig(stream=sys.stderr, level=level,
                        format=f'{msg}%(message)s')

    activate_logging_customisations(debug, verbose)
