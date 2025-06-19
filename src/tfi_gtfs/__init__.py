
from . import settings
from . import web_server

from .gtfs import StaticAssets

# TODO: build this into the application properly when
#       the command line launcher is built.
from .logger import log_to_stderr
log_to_stderr(debug=False, verbose=False)
