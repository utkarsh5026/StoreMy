import threading
from typing import Optional
from dataclasses import dataclass
from collections import OrderedDict
from app.storage.buffer_pool.primitives import PageEntry
from app.primitives import PageId


class CacheManager:
    """
    Manages the core page cache with configurable capacity.

    Responsibilities:
    - Store and retrieve pages from cache
    - Track cache statistics
    - Coordinate with eviction policy
    - Thread-safe cache operations
    """

    @dataclass
    class CacheStatistics:
        """Statistics for cache operations."""
        hits: int = 0
        misses: int = 0
        evictions: int = 0

        @property
        def hit_rate(self) -> float:
            total = self.hits + self.misses
            return self.hits / total if total > 0 else 0.0

    @dataclass
    class CacheInfo:
        size: int
        capacity: int
        utilization: float
        dirty_pages: int
        pinned_pages: int

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict[PageId, PageEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = self.CacheStatistics()

    def get(self, page_id: PageId) -> Optional[PageEntry]:
        """Get page entry from cache."""
        with self._lock:
            entry = self._cache.get(page_id)
            if entry:
                # Move to end (most recently used)
                self._cache.move_to_end(page_id)
                entry.touch()
                self._stats.hits += 1
                return entry
            else:
                self._stats.misses += 1
                return None

    def put(self, page_id: PageId, entry: PageEntry) -> None:
        """Put page entry into cache."""
        with self._lock:
            self._cache[page_id] = entry
            # Move to end (most recently used)
            self._cache.move_to_end(page_id)

    def remove(self, page_id: PageId) -> Optional[PageEntry]:
        """Remove page from cache."""
        with self._lock:
            return self._cache.pop(page_id, None)

    def is_full(self) -> bool:
        """Check if cache is at capacity."""
        with self._lock:
            return len(self._cache) >= self.capacity

    def get_eviction_candidates(self) -> list[tuple[PageId, PageEntry]]:
        """Get pages that can be evicted (in LRU order)."""
        with self._lock:
            candidates = []
            for page_id, entry in self._cache.items():
                if entry.can_evict():
                    candidates.append((page_id, entry))
            return candidates

    def get_statistics(self) -> CacheStatistics:
        """Get cache statistics."""
        with self._lock:
            return self.CacheStatistics(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions
            )

    def get_cache_info(self) -> CacheInfo:
        """Get detailed cache information."""
        with self._lock:
            return self.CacheInfo(
                size=len(self._cache),
                capacity=self.capacity,
                utilization=len(self._cache) / self.capacity,
                dirty_pages=sum(1 for entry in self._cache.values()
                                if entry.page.is_dirty()),
                pinned_pages=sum(
                    1 for entry in self._cache.values() if entry.is_pinned)
            )
