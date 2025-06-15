# a test suite to make the sure the re-write of the redis
# and local backends are functionally equivalent.

import unittest

from typing import Union

from tfi_gtfs import LocalStore
from tfi_gtfs import RedisStore


class _StorageTests(unittest.TestCase):
    """Parent class for common code on both storage mechanisms."""

    store: Union[LocalStore, RedisStore]

    def _test_get_set(self):
        self.store.set('my_namespace', 'my_key', 'my_value123')
        self.assertEqual(self.store.get('my_namespace', 'my_key'), 'my_value123',
                         msg='returned value was not consistent')

    def _test_set_operations(self):
        self.store.add('my_set', 'key1')
        self.store.add('my_set', 'key1')
        self.store.add('my_set', 'key2')
        self.store.add('my_set', 'key2')

        self.assertEqual(self.store.cardinality('my_set'), 2)

        self.assertTrue(self.store.has('my_set', 'key2'))
        self.store.remove('my_set', 'key2')
        self.assertFalse(self.store.has('my_set', 'key2'))

        self.assertEqual(self.store.cardinality('my_set'), 1)


class LocalStorageTestCase(_StorageTests):
    """Testing local storage"""

    test_get_set = _StorageTests._test_get_set
    test_set_operations = _StorageTests._test_set_operations

    def __init__(self, *args, **kwargs):
        _StorageTests.__init__(self, *args, **kwargs)
        self.store = LocalStore()


# This test case requires a running redis instance if launched outside
# the docker container environment:
#      docker run -p 6379:6379 redis
@unittest.skip("skip these tests so we don't need to launch redis.")
class RedisStorageTestCase(_StorageTests):
    """Testing redis storage"""

    test_get_set = _StorageTests._test_get_set
    test_set_operations = _StorageTests._test_set_operations

    def __init__(self, *args, **kwargs):
        _StorageTests.__init__(self, *args, **kwargs)
        self.store = RedisStore()

