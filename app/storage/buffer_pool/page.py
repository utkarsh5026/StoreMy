from typing import Dict, Any
from dataclasses import dataclass
from copy import deepcopy
import threading
import time
from app.primitives import PageId
from app.storage.interfaces import Page
from app.core.exceptions import DbException


class PageLoader:
    """
    Handles loading pages from storage using appropriate handlers.

    Responsibilities:
    - Determine correct page handler
    - Load page data from disk
    - Handle loading errors
    - Track loading statistics
    """

    @dataclass
    class LoadStatistics:
        successful_loads: int = 0
        failed_loads: int = 0
        total_load_time: float = 0.0

        @property
        def avg_load_time(self) -> float:
            return self.total_load_time / self.successful_loads if self.successful_loads > 0 else 0.0

        @property
        def success_rate(self) -> float:
            total_loads = self.successful_loads + self.failed_loads
            return self.successful_loads / total_loads if total_loads > 0 else 0.0

    def __init__(self):
        self._handlers: Dict[str, Any] = {}
        self._load_stats = self.LoadStatistics()
        self._lock = threading.RLock()

    def register_handler(self, page_type: str, handler: Any) -> None:
        """Register a page handler for a specific type."""
        self._handlers[page_type] = handler

    def load_page(self, page_id: PageId, page_type: str) -> Page:
        """Load a page from storage."""
        start_time = time.time()

        try:
            handler = self._handlers.get(page_type)
            if not handler:
                raise DbException(
                    f"No handler registered for page type: {page_type}")

            page = handler.load_page(page_id)

            with self._lock:
                self._load_stats.successful_loads += 1
                self._load_stats.total_load_time += time.time() - start_time

            return page

        except Exception as e:
            with self._lock:
                self._load_stats.failed_loads += 1
            raise DbException(f"Failed to load page {page_id}: {e}")

    def get_load_statistics(self) -> LoadStatistics:
        """Get loading statistics."""
        with self._lock:
            return deepcopy(self._load_stats)


class PageWriter:
    """
    Handles writing pages to storage.

    Responsibilities:
    - Write pages to disk using appropriate handlers
    - Handle write errors
    - Track writing statistics
    - Coordinate with transaction system
    """

    @dataclass
    class WriteStatistics:
        successful_writes: int = 0
        failed_writes: int = 0
        total_write_time: float = 0.0

        @property
        def avg_write_time(self) -> float:
            return self.total_write_time / self.successful_writes if self.successful_writes > 0 else 0.0

        @property
        def success_rate(self) -> float:
            total_writes = self.successful_writes + self.failed_writes
            return self.successful_writes / total_writes if total_writes > 0 else 0.0

    def __init__(self):
        self._handlers: Dict[str, Any] = {}
        self._write_stats = self.WriteStatistics()
        self._lock = threading.RLock()

    def register_handler(self, page_type: str, handler: Any) -> None:
        """Register a page handler for a specific type."""
        self._handlers[page_type] = handler

    def write_page(self, page: Page, page_type: str) -> None:
        """Write a page to storage."""
        start_time = time.time()

        try:
            handler = self._handlers.get(page_type)
            if not handler:
                raise DbException(
                    f"No handler registered for page type: {page_type}")

            handler.write_page(page)

            with self._lock:
                self._write_stats.successful_writes += 1
                self._write_stats.total_write_time += time.time() - \
                    start_time

        except Exception as e:
            with self._lock:
                self._write_stats.failed_writes += 1
            raise DbException(f"Failed to write page {page.get_id()}: {e}")

    def get_write_statistics(self) -> Dict[str, Any]:
        """Get writing statistics."""
        with self._lock:
            return deepcopy(self._write_stats)
