
import sys
import logging
import argparse

from .gtfs import GTFS
from .logger import log_to_stderr
from .web_routes import register_routes
from .web_server import app, serve_forever
from . import settings

log = logging.getLogger(__name__)

# only used in cached mode, to run the server without any live data.
CACHED_STATIC_ASSETS = 'tests/GTFS.zip'
CACHED_REALTIME_DATA = 'tests/realtime_data.bin'
CACHED_TIMESTAMP = 'tests/GTFS.zip'

def main():
    """Main entrypoint on the command line."""

    args = get_args()
    log_to_stderr(args.debug, args.verbose)

    log.info('GTFS starting up...')
    gtfs = GTFS(static_asset_url=settings.GTFS_STATIC_URL,
                realtime_data_url=settings.GTFS_REALTIME_URL,
                start=True)

    gtfs.wait_for_data_available(timeout=60)

    register_routes(app, gtfs)
    serve_forever(settings.HOST, settings.PORT)


def get_args():
    parser = argparse.ArgumentParser(description="GTFS Realtime Data for TFI")

    parser.add_argument('cached', action='store_true',
                        help='run the server using unittest cached data, not live data.')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--debug", help="print debug info", action='store_true')
    group.add_argument("--verbose", help="print verbose debug info, turning off all filters", action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    main()