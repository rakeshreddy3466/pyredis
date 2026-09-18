"""
The actual database: an in-memory dict wrapped with

  1. TTL / expiry (lazy expiry check on access, like real Redis)
  2. LRU eviction once we exceed a configured max number of keys

We use collections.OrderedDict as the backbone: every read or write moves
the touched key to the end ("most recently used"). When we're over
capacity we pop from the front (least recently used) until we're back
under the limit.
"""

import time
import fnmatch
from collections import OrderedDict


class LRUStore:
    def __init__(self, max_keys: int = 10_000):
        self.max_keys = max_keys
        # key -> (value, expire_at_epoch_seconds_or_None)
        self.data: "OrderedDict[str, tuple]" = OrderedDict()
        self.evictions = 0
        self.hits = 0
        self.misses = 0

    # ---- internal helpers ----

    def _is_expired(self, key) -> bool:
        entry = self.data.get(key)
        if entry is None:
            return False
        _, expire_at = entry
        return expire_at is not None and expire_at <= time.time()

    def _purge_if_expired(self, key) -> bool:
        if key in self.data and self._is_expired(key):
            del self.data[key]
            return True
        return False

    def _evict_if_needed(self):
        while len(self.data) > self.max_keys:
            self.data.popitem(last=False)  # least recently used
            self.evictions += 1

    # ---- public API ----

    def set(self, key: str, value: str, ttl: float | None = None):
        expire_at = (time.time() + ttl) if ttl is not None else None
        if key in self.data:
            del self.data[key]  # so re-insert goes to the end
        self.data[key] = (value, expire_at)
        self._evict_if_needed()

    def get(self, key: str):
        self._purge_if_expired(key)
        if key not in self.data:
            self.misses += 1
            return None
        value, _ = self.data[key]
        self.data.move_to_end(key)  # mark as most recently used
        self.hits += 1
        return value

    def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                count += 1
        return count

    def exists(self, key: str) -> bool:
        self._purge_if_expired(key)
        return key in self.data

    def expire(self, key: str, seconds: float) -> int:
        if self._purge_if_expired(key) or key not in self.data:
            return 0
        value, _ = self.data[key]
        self.data[key] = (value, time.time() + seconds)
        return 1

    def ttl(self, key: str) -> int:
        """Seconds remaining, -1 if no expiry set, -2 if key doesn't exist."""
        self._purge_if_expired(key)
        if key not in self.data:
            return -2
        _, expire_at = self.data[key]
        if expire_at is None:
            return -1
        return max(0, int(expire_at - time.time()))

    def persist(self, key: str) -> int:
        """Remove the expiry from a key. Returns 1 if it had one, 0 otherwise."""
        if self._purge_if_expired(key) or key not in self.data:
            return 0
        value, expire_at = self.data[key]
        if expire_at is None:
            return 0
        self.data[key] = (value, None)
        return 1

    def keys(self, pattern: str = "*"):
        result = []
        for k in list(self.data.keys()):
            if self._purge_if_expired(k):
                continue
            if fnmatch.fnmatch(k, pattern):
                result.append(k)
        return result

    def flushall(self):
        self.data.clear()

    def dbsize(self) -> int:
        # cheap lazy purge pass so DBSIZE doesn't count dead keys
        for k in list(self.data.keys()):
            self._purge_if_expired(k)
        return len(self.data)

    def dump(self):
        """Snapshot of live (non-expired) data as (key, value, ttl_remaining_or_None).
        Used for AOF rewrite and for full-sync when a replica connects."""
        items = []
        for k in list(self.data.keys()):
            if self._purge_if_expired(k):
                continue
            value, expire_at = self.data[k]
            remaining = None
            if expire_at is not None:
                remaining = max(0.0, expire_at - time.time())
            items.append((k, value, remaining))
        return items
