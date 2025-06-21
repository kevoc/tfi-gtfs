
from datetime import datetime, timedelta

from flask import Flask, request
from .gtfs import GTFS
from .web_server import format_response, show_page



def register_routes(app: Flask, gtfs: GTFS):
    """Register all routes needed for the web server."""

    # basic homepage
    @app.route('/')
    def index():
        return show_page('homepage.html')


    # set up the API endpoint
    @app.route('/api/v2/departures')
    @format_response
    def departures():
        now = datetime.now()
        stop_numbers = [int(n) for n in request.args.getlist('stop') if n.isnumeric()]
        arr = {}
        for stop_number in stop_numbers:
            if gtfs.stop_number_is_valid(stop_number):
                arr[stop_number] = {
                    'stop_name':  gtfs.stop_name(stop_number),
                    'departures': gtfs.get_scheduled_departures(stop_number, now,
                                                                timedelta(minutes=90))
                }
        return arr
