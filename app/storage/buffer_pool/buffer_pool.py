"""
Modular Buffer Pool Architecture

This design breaks down the buffer pool into focused, composable components:

1. CacheManager - Core caching logic and storage
2. EvictionPolicy - Pluggable eviction strategies
3. PageLoader - Loading pages from storage
4. PageWriter - Writing pages to storage
5. LockCoordinator - Lock management integration
6. StatisticsCollector - Performance monitoring
7. TransactionCoordinator - Transaction management
8. PageTypeManager - Page type handling
9. BufferPoolOrchestrator - Coordinates all components

Each component has a single responsibility and can be tested/modified independently.
"""

import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Optional, Any, List, Set, Protocol
from enum import Enum

from app.primitives import TransactionId, PageId
from app.storage.permissions import Permissions
from app.storage.interfaces import Page
from app.core.exceptions import DbException
from .cache_manager import CacheManager
from .primitives import PageEntry
from .eviction_policy import EvictionPolicy, LRUEvictionPolicy, LFUEvictionPolicy, ClockEvictionPolicy
from .page import PageLoader, PageWriter


# ============================================================================
# 2. Eviction Policy - Pluggable eviction strategies
# ============================================================================


# ============================================================================
# 5. Lock Coordinator - Lock management integration
# ============================================================================

class LockCoordinator:
    """
    Coordinates page access with the lock manager.

    Responsibilities:
    - Acquire and release page locks
    - Handle deadlock detection
    - Track lock statistics
    - Coordinate with transaction system
    """

    def __init__(self, lock_manager: Any):
        self.lock_manager = lock_manager
        self._lock_stats = {'locks_acquired': 0,
                            'locks_denied': 0, 'deadlocks': 0}
        self._lock = threading.Lock()

    def acquire_page_lock(self, tid: TransactionId, page_id: PageId,
                          permissions: Permissions) -> bool:
        """Acquire a lock on a page."""
        from enhanced_lock_manager import LockType

        # Determine lock type
        lock_type = LockType.EXCLUSIVE if permissions.is_write() else LockType.SHARED

        try:
            success = self.lock_manager.acquire_lock(tid, page_id, lock_type)

            with self._lock:
                if success:
                    self._lock_stats['locks_acquired'] += 1
                else:
                    self._lock_stats['locks_denied'] += 1
                    # Check if denial was due to deadlock
                    if self.lock_manager.has_deadlock(tid):
                        self._lock_stats['deadlocks'] += 1

            return success

        except Exception as e:
            with self._lock:
                self._lock_stats['locks_denied'] += 1
            raise DbException(f"Failed to acquire lock on page {page_id}: {e}")

    def release_page_lock(self, tid: TransactionId, page_id: PageId) -> None:
        """Release a lock on a page."""
        try:
            self.lock_manager.release_lock(tid, page_id)
        except Exception as e:
            # Log but don't fail - lock might already be released
            pass

    def release_all_locks(self, tid: TransactionId) -> None:
        """Release all locks held by a transaction."""
        try:
            self.lock_manager.release_all_locks(tid)
        except Exception as e:
            # Log but don't fail
            pass

    def get_lock_statistics(self) -> Dict[str, Any]:
        """Get lock statistics."""
        with self._lock:
            return self._lock_stats.copy()


# ============================================================================
# 6. Statistics Collector - Performance monitoring
# ============================================================================

class StatisticsCollector:
    """
    Collects and aggregates performance statistics.

    Responsibilities:
    - Aggregate statistics from all components
    - Provide performance metrics
    - Track trends over time
    - Generate reports
    """

    def __init__(self):
        self._operation_times: Dict[str, List[float]] = {}
        self._counters: Dict[str, int] = {}
        self._lock = threading.Lock()

    def record_operation(self, operation: str, duration: float) -> None:
        """Record the duration of an operation."""
        with self._lock:
            if operation not in self._operation_times:
                self._operation_times[operation] = []
            self._operation_times[operation].append(duration)

            # Keep only recent measurements (last 1000)
            if len(self._operation_times[operation]) > 1000:
                self._operation_times[operation] = self._operation_times[operation][-1000:]

    def increment_counter(self, counter: str, amount: int = 1) -> None:
        """Increment a counter."""
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + amount

    def get_aggregate_statistics(self, cache_mgr: CacheManager,
                                 page_loader: PageLoader, page_writer: PageWriter,
                                 lock_coord: LockCoordinator) -> Dict[str, Any]:
        """Get aggregated statistics from all components."""
        with self._lock:
            stats = {
                'cache': cache_mgr.get_statistics().__dict__,
                'cache_info': cache_mgr.get_cache_info(),
                'page_loading': page_loader.get_load_statistics(),
                'page_writing': page_writer.get_write_statistics(),
                'locking': lock_coord.get_lock_statistics(),
                'operations': {},
                'counters': self._counters.copy()
            }

            # Calculate operation statistics
            for operation, times in self._operation_times.items():
                if times:
                    import statistics
                    stats['operations'][operation] = {
                        'count': len(times),
                        'avg_time': statistics.mean(times),
                        'min_time': min(times),
                        'max_time': max(times),
                        'median_time': statistics.median(times)
                    }

            return stats


# ============================================================================
# 7. Transaction Coordinator - Transaction management
# ============================================================================

class TransactionCoordinator:
    """
    Coordinates buffer pool operations with transactions.

    Responsibilities:
    - Track pages accessed by transactions
    - Handle transaction commit/abort
    - Coordinate with lock manager
    - Manage dirty page handling
    """

    def __init__(self):
        self._transaction_pages: Dict[TransactionId, Set[PageId]] = {}
        self._page_transactions: Dict[PageId, Set[TransactionId]] = {}
        self._lock = threading.Lock()

    def record_page_access(self, tid: TransactionId, page_id: PageId) -> None:
        """Record that a transaction accessed a page."""
        with self._lock:
            if tid not in self._transaction_pages:
                self._transaction_pages[tid] = set()
            self._transaction_pages[tid].add(page_id)

            if page_id not in self._page_transactions:
                self._page_transactions[page_id] = set()
            self._page_transactions[page_id].add(tid)

    def get_transaction_pages(self, tid: TransactionId) -> Set[PageId]:
        """Get all pages accessed by a transaction."""
        with self._lock:
            return self._transaction_pages.get(tid, set()).copy()

    def get_page_transactions(self, page_id: PageId) -> Set[TransactionId]:
        """Get all transactions that accessed a page."""
        with self._lock:
            return self._page_transactions.get(page_id, set()).copy()

    def handle_transaction_commit(self, tid: TransactionId, cache_mgr: CacheManager,
                                  page_writer: PageWriter) -> None:
        """Handle transaction commit - flush dirty pages."""
        pages = self.get_transaction_pages(tid)

        for page_id in pages:
            entry = cache_mgr.get(page_id)
            if entry and entry.page.is_dirty():
                # Write dirty page to disk
                page_writer.write_page(entry.page, entry.page_type)
                entry.page.mark_dirty(False, None)

        self._cleanup_transaction(tid)

    def handle_transaction_abort(self, tid: TransactionId, cache_mgr: CacheManager) -> None:
        """Handle transaction abort - discard dirty pages."""
        pages = self.get_transaction_pages(tid)

        for page_id in pages:
            entry = cache_mgr.get(page_id)
            if entry and entry.page.is_dirty():
                # Remove dirty page from cache (discard changes)
                cache_mgr.remove(page_id)

        self._cleanup_transaction(tid)

    def _cleanup_transaction(self, tid: TransactionId) -> None:
        """Clean up transaction state."""
        with self._lock:
            pages = self._transaction_pages.pop(tid, set())
            for page_id in pages:
                if page_id in self._page_transactions:
                    self._page_transactions[page_id].discard(tid)
                    if not self._page_transactions[page_id]:
                        del self._page_transactions[page_id]


# ============================================================================
# 8. Page Type Manager - Page type handling
# ============================================================================

class PageTypeManager:
    """
    Manages different page types and their characteristics.

    Responsibilities:
    - Determine page types from page IDs
    - Manage page type metadata
    - Coordinate with appropriate handlers
    """

    def __init__(self):
        self._type_mappings: Dict[str, str] = {}
        self._type_handlers: Dict[str, Any] = {}

    def register_page_type(self, pattern: str, page_type: str, handler: Any) -> None:
        """Register a page type with its handler."""
        self._type_mappings[pattern] = page_type
        self._type_handlers[page_type] = handler

    def determine_page_type(self, page_id: PageId) -> str:
        """Determine the type of a page from its ID."""
        page_id_str = str(page_id)

        # Check for explicit type attribute
        if hasattr(page_id, 'index_type'):
            return page_id.index_type
        elif hasattr(page_id, 'page_type'):
            return page_id.page_type

        # Check pattern mappings
        for pattern, page_type in self._type_mappings.items():
            if pattern in page_id_str:
                return page_type

        # Default to heap page
        return 'heap'

    def get_handler(self, page_type: str) -> Any:
        """Get the handler for a page type."""
        return self._type_handlers.get(page_type)


# ============================================================================
# 9. Buffer Pool Orchestrator - Coordinates all components
# ============================================================================

class BufferPool:
    """
    Orchestrates all buffer pool components.

    This is the main interface that clients interact with.
    It coordinates between all the specialized components.
    """

    def __init__(self, capacity: int, lock_manager: Any,
                 eviction_policy: Optional[EvictionPolicy] = None):
        # Initialize all components
        self.cache_manager = CacheManager(capacity)
        self.eviction_policy = eviction_policy or LRUEvictionPolicy()
        self.page_loader = PageLoader()
        self.page_writer = PageWriter()
        self.lock_coordinator = LockCoordinator(lock_manager)
        self.statistics_collector = StatisticsCollector()
        self.transaction_coordinator = TransactionCoordinator()
        self.page_type_manager = PageTypeManager()

        # Configuration
        self.capacity = capacity

        # Initialize page type handlers
        self._initialize_page_handlers()

    def _initialize_page_handlers(self) -> None:
        """Initialize page handlers for different types."""
        from page_handlers_implementation import PageHandlerFactory

        # Register handlers with both loader and writer
        for page_type in ['heap', 'btree', 'hash', 'metadata']:
            handler = PageHandlerFactory.get_handler(page_type)
            self.page_loader.register_handler(page_type, handler)
            self.page_writer.register_handler(page_type, handler)
            self.page_type_manager.register_page_type(
                page_type, page_type, handler)

    def get_page(self, tid: TransactionId, page_id: PageId,
                 permissions: Permissions) -> Page:
        """Main entry point for page access."""
        start_time = time.time()

        try:
            # 1. Acquire lock
            if not self.lock_coordinator.acquire_page_lock(tid, page_id, permissions):
                raise DbException(f"Could not acquire lock on page {page_id}")

            # 2. Check cache
            entry = self.cache_manager.get(page_id)
            if entry:
                # Cache hit
                self.eviction_policy.on_page_access(page_id, entry)
                self.transaction_coordinator.record_page_access(tid, page_id)
                self.statistics_collector.record_operation(
                    'get_page_hit', time.time() - start_time)
                return entry.page

            # 3. Cache miss - need to load from disk
            page_type = self.page_type_manager.determine_page_type(page_id)

            # 4. Make room if cache is full
            if self.cache_manager.is_full():
                self._evict_page()

            # 5. Load page from disk
            page = self.page_loader.load_page(page_id, page_type)

            # 6. Add to cache
            entry = PageEntry(page, page_type)
            self.cache_manager.put(page_id, entry)

            # 7. Record access
            self.eviction_policy.on_page_access(page_id, entry)
            self.transaction_coordinator.record_page_access(tid, page_id)

            self.statistics_collector.record_operation(
                'get_page_miss', time.time() - start_time)
            return page

        except Exception as e:
            # Release lock on error
            self.lock_coordinator.release_page_lock(tid, page_id)
            raise e

    def _evict_page(self) -> None:
        """Evict a page from the cache."""
        candidates = self.cache_manager.get_eviction_candidates()

        if not candidates:
            raise DbException(
                "Cannot evict page: all pages are dirty or pinned")

        victim = self.eviction_policy.select_victim(candidates)
        if victim:
            page_id, entry = victim
            self.cache_manager.remove(page_id)
            self.statistics_collector.increment_counter('evictions')

    def pin_page(self, page_id: PageId) -> None:
        """Pin a page in memory."""
        entry = self.cache_manager.get(page_id)
        if entry:
            entry.pin()

    def unpin_page(self, page_id: PageId) -> None:
        """Unpin a page."""
        entry = self.cache_manager.get(page_id)
        if entry:
            entry.unpin()

    def flush_page(self, page_id: PageId) -> None:
        """Flush a specific page to disk."""
        entry = self.cache_manager.get(page_id)
        if entry and entry.page.is_dirty():
            self.page_writer.write_page(entry.page, entry.page_type)
            entry.page.mark_dirty(False, None)

    def flush_all_pages(self) -> None:
        """Flush all dirty pages to disk."""
        # Get all cache entries
        for page_id, entry in self.cache_manager._cache.items():
            if entry.page.is_dirty():
                self.page_writer.write_page(entry.page, entry.page_type)
                entry.page.mark_dirty(False, None)

    def transaction_complete(self, tid: TransactionId, commit: bool) -> None:
        """Handle transaction completion."""
        if commit:
            self.transaction_coordinator.handle_transaction_commit(
                tid, self.cache_manager, self.page_writer
            )
        else:
            self.transaction_coordinator.handle_transaction_abort(
                tid, self.cache_manager
            )

        # Release all locks
        self.lock_coordinator.release_all_locks(tid)

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return self.statistics_collector.get_aggregate_statistics(
            self.cache_manager, self.page_loader, self.page_writer, self.lock_coordinator
        )

    def get_cache_info(self) -> Dict[str, Any]:
        """Get cache information."""
        return self.cache_manager.get_cache_info()

    def set_eviction_policy(self, policy: EvictionPolicy) -> None:
        """Change the eviction policy."""
        self.eviction_policy = policy


# ============================================================================
# Factory for creating configured buffer pools
# ============================================================================

class BufferPoolFactory:
    """Factory for creating configured buffer pools."""

    @staticmethod
    def create_buffer_pool(capacity: int, lock_manager: Any,
                           eviction_strategy: str = 'lru') -> ModularBufferPool:
        """Create a buffer pool with specified configuration."""

        # Create eviction policy
        if eviction_strategy == 'lru':
            eviction_policy = LRUEvictionPolicy()
        elif eviction_strategy == 'lfu':
            eviction_policy = LFUEvictionPolicy()
        elif eviction_strategy == 'clock':
            eviction_policy = ClockEvictionPolicy()
        else:
            raise ValueError(f"Unknown eviction strategy: {eviction_strategy}")

        return ModularBufferPool(capacity, lock_manager, eviction_policy)

    @staticmethod
    def create_high_performance_buffer_pool(capacity: int, lock_manager: Any) -> ModularBufferPool:
        """Create a buffer pool optimized for high performance."""
        # Use LRU with larger capacity
        return BufferPoolFactory.create_buffer_pool(capacity * 2, lock_manager, 'lru')

    @staticmethod
    def create_memory_constrained_buffer_pool(capacity: int, lock_manager: Any) -> ModularBufferPool:
        """Create a buffer pool optimized for memory-constrained environments."""
        # Use Clock algorithm for better memory efficiency
        return BufferPoolFactory.create_buffer_pool(capacity, lock_manager, 'clock')
