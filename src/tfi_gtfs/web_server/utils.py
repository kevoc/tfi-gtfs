
import datetime
import waitress

from flask import Flask
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider


def build_flask_app():
    """Create the flask app instance and configure it."""

    app = Flask(__name__)
    CORS(app)

    # create a subclass of flask.json.provider.DefaultJSONProvider returns JSON responses
    # in which datetime objects are serialized to ISO 8601 strings
    class JsonProvider(DefaultJSONProvider):
        def default(self, obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            return super(JsonProvider, self).default(obj)

    app.json = JsonProvider(app)

    return app

def to_iso_date(d):
    if isinstance(d, datetime.datetime):
        return d.isoformat()
    elif isinstance(d, str):
        return d

    return ""


def serve_forever(app: Flask, host, port, threads=1):
    """Launch the webserver."""

    waitress.serve(app, host=host, port=port, threads=threads)
