
import datetime
import waitress

from flask import request

from .utils import build_flask_app
from .format import format_response


app = build_flask_app()


# set up the API endpoint
@app.route('/api/v1/arrivals')
@format_response
def arrivals():
    now = datetime.datetime.now()
    stop_numbers = request.args.getlist('stop')
    arr = {}
    for stop_number in stop_numbers:
        if gtfs.is_valid_stop_number(stop_number):
            arr[stop_number] = {
                'stop_name': gtfs.get_stop_name(stop_number),
                'arrivals': gtfs.get_scheduled_arrivals(stop_number, now, datetime.timedelta(minutes=args.minutes))
            }
    return arr


# serve a basic static page with a link to /api/v1/arrivals at the root
@app.route('/')
def index():
    return """
    <html>
        <head>
            <title>GTFS API</title>
        </head>
        <body>
            <h1>GTFS API</h1>
            <p>See <a href="api/v1/arrivals">api/v1/arrivals</a> for the API documentation.</p>
        </body>
    </html>
    """



def serve_forever(host, port, threads=1):
    """Launch the webserver."""

    waitress.serve(app, host=host, port=port, threads=threads)
