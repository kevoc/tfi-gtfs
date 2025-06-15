
import os
import io
import csv
import sys

import yaml
import datetime

from functools import wraps
from functools import lru_cache

from flask import jsonify
from flask import request
from flask import Response
from flask import render_template

from typing import Optional, List

from .utils import to_iso_date


HEADERS = ["stop_id", "stop_name", "route", "headsign",
           "agency", "scheduled_arrival", "estimated_arrival"]


def format_response(func):
    """Format the response based on the user's accept header."""

    @wraps(func)
    def _wrapper(*args, **kwargs):
        response_data = func(*args, **kwargs)
        accept_header = request.headers.get('Accept')
        mime_type = _mime_type_from_accept_header(accept_header)

        if mime_type == 'application/json':
            return jsonify(response_data)
        elif mime_type == 'application/yaml':
            return Response(yaml.dump(response_data, default_flow_style=False), mimetype=mime_type)

        table_data = _flatten_response_data(response_data)
        if mime_type in ('text/csv', 'text/plain'):
            return Response(csv_table(table_data, HEADERS), mimetype=mime_type)
        else:
            try:
                return Response(render_template('main.html',
                                                table=html_table(table_data, HEADERS),
                                                css=render_template('main.css'),
                                                script=render_template('main.js')),
                                mimetype=mime_type)
            except:
                print(sys.exc_info())
                raise
    return _wrapper



ACCEPT_MAPPING = {
    # accept header: mime type
    '*/*':           'application/json',
    '':              'application/json',
    'application/*': 'application/json',
    None:            'application/json'
}

VALID_MIME_TYPES = ['application/json', 'application/yaml',
                    'text/csv', 'text/plain', 'text/html']

def _mime_type_from_accept_header(accept: Optional[str]):
    """Sanitise the Accept header from the client."""

    if accept in ACCEPT_MAPPING:
        return ACCEPT_MAPPING[accept]
    else:
        for mime in VALID_MIME_TYPES:
            if mime in accept:
                return mime

    # if there's no clear Accept header value, the request is likely not
    # coming from a browser, so we should return json for an application.
    return 'application/json'


TEMPLATES = os.path.join(os.path.dirname(__file__), 'templates')
@lru_cache
def load_template(template_file, **kwargs):
    """Load the given template file, also filling in variables."""

    with open(os.path.join(TEMPLATES, template_file)) as f:
        template_content = f.read()

    return template_content.format(**kwargs)


def _flatten_response_data(response_data) -> List[dict]:
    """Reformat the data to a table-ready format"""

    # reformat the data provided into a list of dictionaries
    # one line of the table per dict.
    return [
        { 'stop_id': stop_number,
          'stop_name': stop_data['stop_name'],
          'route': stop['route'],
          'headsign':stop['headsign'],
          'agency': stop['agency'],
          'scheduled_arrival': to_iso_date(stop['scheduled_arrival']),
          'estimated_arrival': to_iso_date(stop['real_time_arrival'])
        }
        for stop_number, stop_data in response_data.items()
        for stop in stop_data['arrivals']
    ]


def csv_table(table: List[dict], headers: Optional[List[str]] = None) -> str:
    """convert a list of dictionaries to the text of a CSV file."""

    # if `headers` weren't supplied, use the keys of the first dictionary
    # if there's at least one element in the `table`
    headers = headers or (list(table[0].keys()) if table else [])

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(table)

    return output.getvalue()



TABLE_TEMPLATE = """
<table>
  <thead>{headers}</thead>
  <tbody>{rows}</tbody>
</table>
"""

def _table_row(row: dict, headers: List[str], tag='td'):
    """Format a HTML row in the same order as the headers."""
    return '<tr>' + ('\n'.join([f'<{tag}>{row.get(h)}</{tag}>' for h in headers])) + '</tr>'

def html_table(table: List[dict], headers: Optional[List[str]] = None) -> str:
    """convert a list of dictionaries to the text of a HTML table."""

    headers = headers or (list(table[0].keys()) if table else [])

    header_str = _table_row({h:h for h in headers}, headers, tag='th')
    rows_str = '\n'.join([_table_row(r, headers) for r in table])

    return TABLE_TEMPLATE.format(headers=header_str, rows=rows_str)
