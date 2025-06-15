# a test suite to make the sure the re-write of the redis
# and local backends are functionally equivalent.

import io
import time
import unittest
import threading

import pytest
import tempfile
import webbrowser
import pandas as pd

from tfi_gtfs import web_server


# makes flask dump exceptions to stdout.
web_server.app.debug = True


@pytest.fixture
def client():
    with web_server.app.test_client() as client:
        yield client


def test_homepage(client):
    """Make sure the correct homepage is returned."""
    response = client.get("/")
    assert response.status_code == 200
    assert b"<title>GTFS API</title>" in response.data


@web_server.app.route('/test')
@web_server.format_response
def dummy_request():
    """A flask callback for testing that always return the same data. """

    return {
        "271":{
            "arrivals":[
                {"agency":"Bus \u00c1tha Cliath \u2013 Dublin Bus","headsign":"Drimnagh Road",
                 "real_time_arrival": None,"route":"122","scheduled_arrival":"2025-06-15T10:00:08"},
                {"agency":"Bus \u00c1tha Cliath \u2013 Dublin Bus","headsign":"Enniskerry",
                 "real_time_arrival": None,"route":"44","scheduled_arrival":"2025-06-15T10:26:18"},
                {"agency":"Bus \u00c1tha Cliath \u2013 Dublin Bus","headsign":"Shaw street",
                 "real_time_arrival":"2025-06-15T10:28:39","route":"1","scheduled_arrival":"2025-06-15T10:26:15"},
                {"agency":"Bus \u00c1tha Cliath \u2013 Dublin Bus","headsign":"Shaw street",
                 "real_time_arrival":"2025-06-15T11:04:42","route":"1","scheduled_arrival":"2025-06-15T10:56:15"}
            ],
            "stop_name": "O'Connell Street Lower"
        }
    }


def test_json_response(client):
    resp = client.get('/test', headers={'Accept': 'application/json'})
    # make sure the response is valid json
    assert isinstance(resp.json, dict)


ACCEPT_HEADERS = ['*/*', '', 'application/*']
def test_accept_mappings_are_json(client):
    for h in ACCEPT_HEADERS:
        resp = client.get('/test', headers={'Accept': h})
        # make sure the response is valid json
        assert isinstance(resp.json, dict)



def _test_accept_header_is_csv(client, header):
    resp = client.get('/test', headers={'Accept': header})
    df = pd.read_csv(io.StringIO(resp.text))

    line2 = df.iloc[1]
    assert line2.route == 44
    assert line2.headsign == 'Enniskerry'
    assert line2.scheduled_arrival == '2025-06-15T10:26:18'


def test_text_csv(client):
    _test_accept_header_is_csv(client, 'text/csv')

def test_text_plain(client):
    _test_accept_header_is_csv(client, 'text/plain')


@unittest.skip("debug only")
def test_webpage_in_browser(client):
    with tempfile.NamedTemporaryFile('w', delete=False, delete_on_close=False,
                                     suffix='.html') as my_file:
        resp = client.get('/test', headers={'Accept': 'text/html'})
        my_file.write(resp.text)
        webbrowser.open(f'file://{my_file.name}')


@unittest.skip("debug only")
def test_webpage_by_serving(client):
    """Start the server and open the test page with dummy data."""
    t = threading.Thread(target=web_server.serve_forever,
                         kwargs={'host': 'localhost', 'port': 10101},
                         daemon=True)
    t.start()

    webbrowser.open(f'http://localhost:10101/test')

    # wait for an interrupt, then exit, killing the thread
    while True:
        time.sleep(1)


