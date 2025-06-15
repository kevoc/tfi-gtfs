
import os


GTFS_STATIC_URL = os.environ.get('GTFS_STATIC_URL', "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip")
GTFS_LIVE_URL = os.environ.get('GTFS_LIVE_URL', "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates")
API_KEY = os.environ.get('API_KEY')

# Redis URL, probably something like redis://localhost:6379
REDIS_URL = os.environ.get('REDIS_URL', None)
POLLING_PERIOD = os.environ.get('POLLING_PERIOD', 60)
MAX_MINUTES = os.environ.get('MAX_MINUTES', 60)
HOST = os.environ.get('HOST', 'localhost')
PORT = os.environ.get('PORT', 7341)
WORKERS = os.environ.get('WORKERS', 1)
DATA_DIR = './data'

# set default logging level to INFO
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
if LOG_LEVEL not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
    print(f"Invalid log level: {LOG_LEVEL}. Defaulting to 'INFO'.")
    LOG_LEVEL = 'INFO'


def data_dir_file(filename):
    """Return the path to a file in the data directory."""
    return os.path.join(DATA_DIR, filename)