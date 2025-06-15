
import os
import time
import redis
import pickle
import logging

from collections import defaultdict
from typing import Optional, Dict

from tfi_gtfs.settings import data_dir_file

log = logging.getLogger(__name__)


# used to store the LocalStore() cache
CACHE_FILE = data_dir_file('cache.pkl')

DEFAULT_REDIS_URL = 'redis://localhost:6379'


class RedisStore:
    """A container to wrap Redis as the backend storage."""

    def __init__(self, url=DEFAULT_REDIS_URL):
        self._backend = redis.from_url(url)

        self._load_cache()

    def _load_cache(self):
        """If a cache file exists on disk, load it."""
        # do nothing, redis manages this itself.

    def _save_cache(self):
        """Dump the cache to disk."""
        self._backend.save()

    def _flush_cache(self):
        """Delete the entire cache"""
        self._backend.flushdb()

    @property
    def memory_usage(self):
        return self._backend.info('memory')['used_memory']

    def get(self, namespace, key, default=None):
        value = self._backend.hget(namespace, key)
        if value is not None:
            return pickle.loads(value)
        else:
            return default

    def set(self, namespace, key, value):
        self._backend.hset(namespace, key, pickle.dumps(value))

    def delete(self, namespace, key):
        self._backend.hdel(namespace, key)

    def add(self, namespace, value):
        self._backend.sadd(namespace, value)

    def remove(self, namespace, value):
        self._backend.srem(namespace, value)

    def has(self, namespace, value):
        return self._backend.sismember(namespace, value) == 1

    def cardinality(self, namespace):
        return self._backend.scard(namespace)


class LocalStore:
    """A container to mimic redis storage by using nested dictionaries."""

    def __init__(self, expiry=None):
        self._expiry = expiry
        self._backend = defaultdict(dict)

        self._load_cache()

    def _load_cache(self):
        """If a cache file exists on disk, load it."""

        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "rb") as f:
                self._backend = pickle.load(f)

    def _save_cache(self):
        """Dump the cache to disk."""

        with open(CACHE_FILE, "wb") as f:
            pickle.dump(self._backend, f)

    def _flush_cache(self):
        """Delete the entire cache"""

        self._backend = defaultdict(dict)
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)

    @property
    def memory_usage(self):
        return -1

    def get(self, namespace, key, default=None):
        """Return the value of the cached namespace/key if it exists and
        hasn't expired yet."""

        now = int(time.time())

        cached_item = self._backend.get(namespace, {}).get(key)
        if cached_item:
            if self._expiry is None:
                return cached_item
            else:
                timestamp, value = cached_item
                if (now - timestamp) < self._expiry:
                    return value
                else:
                    del self._backend[namespace][key]

        return default

    def set(self, namespace, key, value):
        """Set the value of the cached item."""

        if self._expiry is None:
            self._backend[namespace][key] = value
        else:
            self._backend[namespace][key] = (int(time.time()), value)

    def delete(self, namespace, key):
        ns = self._backend.get(namespace, {})
        if key in ns:
            del ns[key]

    # set operations including add, remove and has
    def add(self, namespace, value):
        if namespace not in self._backend:
            self._backend[namespace] = set()
        self._backend[namespace].add(value)

    def remove(self, namespace, value):
        self._backend[namespace].discard(value)

    def has(self, namespace, value):
        return value in self._backend[namespace]

    def cardinality(self, namespace):
        return len(self._backend[namespace])

