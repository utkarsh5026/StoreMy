from abc import ABC, abstractmethod
from .primitives import  PageEntry
from app.primitives import PageId
from typing import Optional


class EvictionPolicy(ABC):
    """Abstract base class for eviction policies."""

    @abstractmethod
    def select_victim(self, candidates: list[tuple[PageId, PageEntry]]) -> Optional[tuple[PageId, PageEntry]]:
        """Select a page to evict from candidates."""
        pass

    @abstractmethod
    def on_page_access(self, page_id: PageId, entry: PageEntry) -> None:
        """Called when a page is accessed."""
        pass


class LRUEvictionPolicy(EvictionPolicy):
    """Least Recently Used eviction policy."""

    def select_victim(self, candidates: list[tuple[PageId, PageEntry]]) -> Optional[tuple[PageId, PageEntry]]:
        """Select the least recently used page."""
        if not candidates:
            return None

        # Candidates are already in LRU order from cache manager
        return candidates[0]

    def on_page_access(self, page_id: PageId, entry: PageEntry) -> None:
        """LRU doesn't need special handling on access."""
        pass


class LFUEvictionPolicy(EvictionPolicy):
    """Least Frequently Used eviction policy."""

    def select_victim(self, candidates: list[tuple[PageId, PageEntry]]) -> Optional[tuple[PageId, PageEntry]]:
        """Select the least frequently used page."""
        if not candidates:
            return None

        # Sort by access count (ascending)
        candidates.sort(key=lambda x: x[1].access_count)
        return candidates[0]

    def on_page_access(self, page_id: PageId, entry: PageEntry) -> None:
        """LFU updates are handled in PageEntry.touch()."""
        pass


class ClockEvictionPolicy(EvictionPolicy):
    """Clock (Second Chance) eviction policy."""

    def __init__(self):
        self._reference_bits: dict[PageId, bool] = {}
        self._clock_hand = 0

    def select_victim(self, candidates: list[tuple[PageId, PageEntry]]) -> Optional[tuple[PageId, PageEntry]]:
        """Select a victim using clock algorithm."""
        if not candidates:
            return None

        for page_id, entry in candidates:
            if not self._reference_bits.get(page_id, False):
                return page_id, entry
            else:
                self._reference_bits[page_id] = False

        return candidates[0]

    def on_page_access(self, page_id: PageId, entry: PageEntry) -> None:
        """Set reference a bit on access."""
        self._reference_bits[page_id] = True
