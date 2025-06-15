
# this is an untested set of functions that were abandoned
# in favour of a pandas data storage option.

import csv
import zipfile

import pandas as pd


def _file_in_zip(zip_file, filename):
    """Return a file handle to an unzipped file inside a zip file."""
    zf = zipfile.ZipFile(zip_file, 'r')
    return zf.open(filename, 'r')


def agencies_to_store(zip_file, store):
    """Extract all agency info and add to store."""

    _store_str_by_index_in_namespace(zip_file, 'agency.txt', store, 'agency')


def routes_to_store(zip_file, store):
    """Extract all info regarding routes and add to store."""

    _store_dict_by_index_in_namespace(zip_file, 'routes.txt', store,
                                     'route',
                                     'agency', 'name')


def _store_dict_by_index_in_namespace(zip_file, filename, store, namespace, *cols):
    """Take the CSV `filename` from the `zip_file`, and store the first
    column as the key, and all other columns as a dict under that key."""

    row_end = len(cols) + 1

    with _file_in_zip(zip_file, filename) as f:
        reader = csv.reader(f)
        next(reader)  # skip headers

        for row in reader:
            # use col1 as the key, and all other cols get zipped together with
            # the columns passed in, stored as a dict.
            store.set(namespace, row[0], dict(zip(cols, row[1:row_end])))


def _store_str_by_index_in_namespace(zip_file, filename, store, namespace):
    """Take the CSV `filename` from the `zip_file`, and store the first
    column as the key, and the second col as the value."""

    with _file_in_zip(zip_file, filename) as f:
        df = pd.read_csv(f)

        for index, row in df.iterrows():
            # use col1 as the key, col2 as value
            store.set(namespace, index, row.iloc[0])
