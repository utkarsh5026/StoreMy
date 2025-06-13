# app/storage/buffer_pool/buffer_pool.py (Enhanced)
import time
import threading
from typing import Dict, Optional, Any, Set

from app.primitives import TransactionId, PageId
from app.storage.permissions import Permissions
from app.storage.interfaces import Page
from app.core.exceptions import DbException, TransactionAbortedException
from .cache_manager import CacheManager
from .primitives import PageEntry
from .eviction_policy import EvictionPolicy, LRUEvictionPolicy, LFUEvictionPolicy, ClockEvictionPolicy
from .page import PageLoader, PageWriter
from .transactions import LockCoordinator, TransactionCoordinator
from .stats_collector import StatisticsCollector


class PageTypeManager:
    """
    Enhanced page type manager with better error handling and extensibility.
    """

    def __init__(self):
        self._type_mappings: Dict[str, str] = {}
        self._type_handlers: Dict[str, Any] = {}
        self._lock = threading.RLock()

    def register_page_type(self, pattern: str, page_type: str, handler: Any) -> None:
        """Register a page type with its handler."""
        with self._lock:
            self._type_mappings[pattern] = page_type
            self._type_handlers[page_type] = handler

    def determine_page_type(self, page_id: PageId) -> str:
        """Determine the type of a page from its ID with better logic."""
        with self._lock:
            page_id_str = str(page_id)

            # Check for explicit type attributes
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
        with self._lock:
            return self._type_handlers.get(page_type)

    def get_registered_types(self) -> Set[str]:
        """Get all registered page types."""
        with self._lock:
            return set(self._type_handlers.keys())


class BufferPool:
    """
    Enhanced buffer pool orchestrator with robust transaction integration.

    This is the main interface that coordinates all buffer pool components
    with comprehensive transaction support, error handling, and monitoring.

    Key Features:
    - Integrated transaction management with proper ACID guarantees
    - Deadlock detection and resolution
    - Comprehensive statistics and monitoring
    - Pluggable eviction policies
    - Thread-safe operations
    - Proper error handling and recovery
    """

    # Default configuration
    DEFAULT_PAGES = 50
    DEFAULT_TIMEOUT = 30.0
    MAX_EVICTION_ATTEMPTS = 5

    def __init__(self, capacity: int, eviction_policy: Optional[EvictionPolicy] = None):
        """
        Initialize buffer pool with all components.

        Args:
            capacity: Maximum number of pages to cache
            eviction_policy: Eviction strategy (defaults to LRU)
        """
        # Core configuration
        self.capacity = capacity

        # Initialize all components
        self.cache_manager = CacheManager(capacity)
        self.eviction_policy = eviction_policy or LRUEvictionPolicy()
        self.page_loader = PageLoader()
        self.page_writer = PageWriter()
        self.lock_coordinator = LockCoordinator()  # Will lazy-load lock manager
        self.statistics_collector = StatisticsCollector()
        self.transaction_coordinator = TransactionCoordinator()
        self.page_type_manager = PageTypeManager()

        # Thread safety
        self._lock = threading.RLock()

        # Configuration
        self.enable_deadlock_detection = True
        self.default_timeout = self.DEFAULT_TIMEOUT

        # Initialize page type handlers
        self._initialize_page_handlers()

    def _initialize_page_handlers(self) -> None:
        """Initialize page handlers for different types."""
        try:
            from .page_handler import PageHandlerFactory

            # Register handlers with both loader and writer
            for page_type in ['heap', 'btree', 'hash', 'metadata']:
                try:
                    handler = PageHandlerFactory.get_handler(page_type)
                    self.page_loader.register_handler(page_type, handler)
                    self.page_writer.register_handler(page_type, handler)
                    self.page_type_manager.register_page_type(
                        page_type, page_type, handler)
                except Exception as e:
                    print(
                        f"Warning: Failed to register handler for {page_type}: {e}")

        except ImportError as e:
            print(f"Warning: Page handlers not available: {e}")

    def get_page(self, tid: TransactionId, page_id: PageId,
                 permissions: Permissions, timeout: Optional[float] = None) -> Page:
        """
        Main entry point for page access with robust transaction integration.

        Args:
            tid: Transaction requesting the page
            page_id: Page identifier
            permissions: Read/write permissions
            timeout: Optional timeout for lock acquisition

        Returns:
            The requested page

        Raises:
            TransactionAbortedException: If deadlock detected
            DbException: If page cannot be retrieved
        """
        if timeout is None:
            timeout = self.default_timeout

        start_time = time.time()
        page_acquired = False

        try:
            with self._lock:
                # 1. Acquire lock with proper blocking
                if not self.lock_coordinator.acquire_page_lock(tid, page_id, permissions, timeout):
                    raise DbException(
                        f"Could not acquire lock on page {page_id} within {timeout}s")

                page_acquired = True

                # 2. Check cache first
                entry = self.cache_manager.get(page_id)
                if entry:
                    # Cache hit - update access patterns and record transaction access
                    self.eviction_policy.on_page_access(page_id, entry)
                    self.transaction_coordinator.record_page_access(
                        tid, page_id, permissions.is_write())

                    # Mark page as dirty if this is a write access
                    if permissions.is_write():
                        entry.page.mark_dirty(True, tid)
                        self.transaction_coordinator.mark_page_dirty(
                            tid, page_id)

                    self.statistics_collector.record_operation(
                        'get_page_hit', time.time() - start_time)
                    return entry.page

                # 3. Cache miss - need to load from disk
                page_type = self.page_type_manager.determine_page_type(page_id)

                # 4. Make room if cache is full
                if self.cache_manager.is_full():
                    self._evict_page_for_transaction(tid)

                # 5. Load page from disk
                page = self.page_loader.load_page(page_id, page_type)

                # 6. Add to cache
                entry = PageEntry(page, page_type)
                self.cache_manager.put(page_id, entry)

                # 7. Record access and update patterns
                self.eviction_policy.on_page_access(page_id, entry)
                self.transaction_coordinator.record_page_access(
                    tid, page_id, permissions.is_write())

                # 8. Mark as dirty if write access
                if permissions.is_write():
                    page.mark_dirty(True, tid)
                    self.transaction_coordinator.mark_page_dirty(tid, page_id)

                self.statistics_collector.record_operation(
                    'get_page_miss', time.time() - start_time)
                return page

        except TransactionAbortedException:
            # Deadlock - release lock and re-raise
            if page_acquired:
                self.lock_coordinator.release_page_lock(tid, page_id)
            raise

        except Exception as e:
            # Other error - release lock and wrap in DbException
            if page_acquired:
                self.lock_coordinator.release_page_lock(tid, page_id)
            raise DbException(f"Failed to get page {page_id}: {e}")

    def _evict_page_for_transaction(self, requesting_tid: TransactionId) -> None:
        """
        Evict a page with proper handling of no-steal policy.

        Args:
            requesting_tid: Transaction requesting space

        Raises:
            TransactionAbortedException: If no pages can be evicted (no-steal policy)
        """
        attempts = 0
        while attempts < self.MAX_EVICTION_ATTEMPTS:
            candidates = self.cache_manager.get_eviction_candidates()

            if not candidates:
                # No evictable pages - implement no-steal policy
                self.statistics_collector.increment_counter('no_steal_aborts')
                raise TransactionAbortedException(
                    f"Transaction {requesting_tid} aborted: buffer pool full with dirty/pinned pages")

            victim = self.eviction_policy.select_victim(candidates)
            if victim:
                page_id, entry = victim

                # Double-check that page can still be evicted
                if entry.can_evict():
                    # If page is dirty, it should have been flushed by transaction commit
                    # If we reach here with a dirty page, it indicates a bug
                    if entry.page.is_dirty():
                        print(
                            f"Warning: Evicting dirty page {page_id} - possible bug")

                    self.cache_manager.remove(page_id)
                    self.statistics_collector.increment_counter('evictions')
                    return

            attempts += 1

        # If we get here, we couldn't evict after multiple attempts
        raise DbException("Failed to evict page after multiple attempts")

    def pin_page(self, page_id: PageId) -> None:
        """Pin a page in memory to prevent eviction."""
        with self._lock:
            entry = self.cache_manager.get(page_id)
            if entry:
                entry.pin()
                self.statistics_collector.increment_counter('pins')

    def unpin_page(self, page_id: PageId) -> None:
        """Unpin a page to allow eviction."""
        with self._lock:
            entry = self.cache_manager.get(page_id)
            if entry:
                entry.unpin()
                self.statistics_collector.increment_counter('unpins')

    def flush_page(self, page_id: PageId) -> None:
        """Flush a specific page to disk."""
        with self._lock:
            entry = self.cache_manager.get(page_id)
            if entry and entry.page.is_dirty():
                try:
                    self.page_writer.write_page(entry.page, entry.page_type)
                    entry.page.mark_dirty(False, None)
                    self.statistics_collector.increment_counter(
                        'manual_flushes')
                except Exception as e:
                    raise DbException(f"Failed to flush page {page_id}: {e}")

    def flush_all_pages(self) -> None:
        """Flush all dirty pages to disk (used for checkpoints)."""
        with self._lock:
            flushed_count = 0
            error_count = 0

            # Get all cache entries
            for page_id, entry in self.cache_manager._cache.items():
                if entry.page.is_dirty():
                    try:
                        self.page_writer.write_page(
                            entry.page, entry.page_type)
                        entry.page.mark_dirty(False, None)
                        flushed_count += 1
                    except Exception as e:
                        error_count += 1
                        print(f"Warning: Failed to flush page {page_id}: {e}")

            self.statistics_collector.increment_counter(
                'checkpoint_flushes', flushed_count)
            if error_count > 0:
                print(
                    f"Warning: {error_count} pages failed to flush during checkpoint")

    def transaction_complete(self, tid: TransactionId, commit: bool) -> None:
        """
        Handle transaction completion with comprehensive cleanup.

        Args:
            tid: Transaction that completed
            commit: True if committed, False if aborted
        """
        start_time = time.time()

        try:
            with self._lock:
                if commit:
                    self.transaction_coordinator.handle_transaction_commit(
                        tid, self.cache_manager, self.page_writer
                    )
                    self.statistics_collector.increment_counter('commits')
                else:
                    self.transaction_coordinator.handle_transaction_abort(
                        tid, self.cache_manager
                    )
                    self.statistics_collector.increment_counter('aborts')

                # Release all locks held by this transaction
                self.lock_coordinator.release_all_locks(tid)

                completion_time = time.time() - start_time
                operation = 'transaction_commit' if commit else 'transaction_abort'
                self.statistics_collector.record_operation(
                    operation, completion_time)

        except Exception as e:
            print(f"Error during transaction completion for {tid}: {e}")
            # Still try to release locks
            try:
                self.lock_coordinator.release_all_locks(tid)
            except:
                pass

    def force_abort_transaction(self, tid: TransactionId, reason: str = "Forced abort") -> None:
        """Force abort a transaction (used by deadlock detection)."""
        try:
            self.transaction_complete(tid, commit=False)
            print(f"Force aborted transaction {tid}: {reason}")
        except Exception as e:
            print(f"Error force aborting transaction {tid}: {e}")

    def get_page_if_cached(self, page_id: PageId) -> Optional[Page]:
        """Get page if it's in cache, without acquiring locks."""
        with self._lock:
            entry = self.cache_manager.get(page_id)
            return entry.page if entry else None

    def is_page_dirty(self, page_id: PageId) -> bool:
        """Check if a page is dirty."""
        with self._lock:
            entry = self.cache_manager.get(page_id)
            return entry.page.is_dirty() if entry else False

    def get_dirty_pages(self) -> Set[PageId]:
        """Get all dirty pages in the buffer pool."""
        with self._lock:
            dirty_pages = set()
            for page_id, entry in self.cache_manager._cache.items():
                if entry.page.is_dirty():
                    dirty_pages.add(page_id)
            return dirty_pages

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics from all components."""
        with self._lock:
            stats = self.statistics_collector.get_aggregate_statistics(
                self.cache_manager, self.page_loader, self.page_writer, self.lock_coordinator
            )

            # Add buffer pool specific stats
            stats['buffer_pool'] = {
                'capacity': self.capacity,
                'utilization': len(self.cache_manager._cache) / self.capacity,
                'dirty_pages': len(self.get_dirty_pages()),
                'eviction_policy': type(self.eviction_policy).__name__,
                'active_transactions': len(self.transaction_coordinator.get_active_transactions())
            }

            # Add transaction coordinator stats
            stats['transaction_coordinator'] = self.transaction_coordinator.get_statistics(
            ).__dict__

            return stats

    def get_cache_info(self) -> Dict[str, Any]:
        """Get detailed cache information."""
        with self._lock:
            return self.cache_manager.get_cache_info().__dict__

    def get_transaction_info(self, tid: TransactionId) -> Dict[str, Any]:
        """Get detailed information about a transaction."""
        return self.transaction_coordinator.get_transaction_info(tid)

    def get_debug_info(self) -> Dict[str, Any]:
        """Get comprehensive debug information."""
        with self._lock:
            return {
                'cache_info': self.get_cache_info(),
                'statistics': self.get_statistics(),
                'lock_info': self.lock_coordinator.get_debug_info(),
                'active_transactions': [str(tid) for tid in self.transaction_coordinator.get_active_transactions()],
                'dirty_pages': [str(pid) for pid in self.get_dirty_pages()],
                'registered_page_types': list(self.page_type_manager.get_registered_types())
            }

    def set_eviction_policy(self, policy: EvictionPolicy) -> None:
        """Change the eviction policy."""
        with self._lock:
            self.eviction_policy = policy

    def set_default_timeout(self, timeout: float) -> None:
        """Set default timeout for lock acquisition."""
        self.default_timeout = timeout

    def enable_monitoring(self, enabled: bool) -> None:
        """Enable or disable detailed monitoring."""
        # Could control whether to collect detailed statistics
        pass

    def reset_statistics(self) -> None:
        """Reset all statistics (useful for testing)."""
        with self._lock:
            self.statistics_collector = StatisticsCollector()
            self.lock_coordinator.reset_statistics()
            self.transaction_coordinator.reset_statistics()


class BufferPoolFactory:
    """Enhanced factory for creating configured buffer pools."""

    @staticmethod
    def create_buffer_pool(capacity: int, eviction_strategy: str = 'lru',
                           **kwargs) -> BufferPool:
        """
        Create a buffer pool with specified configuration.

        Args:
            capacity: Buffer pool capacity
            eviction_strategy: 'lru', 'lfu', or 'clock'
            **kwargs: Additional configuration options
        """
        # Create eviction policy
        if eviction_strategy == 'lru':
            eviction_policy = LRUEvictionPolicy()
        elif eviction_strategy == 'lfu':
            eviction_policy = LFUEvictionPolicy()
        elif eviction_strategy == 'clock':
            eviction_policy = ClockEvictionPolicy()
        else:
            raise ValueError(f"Unknown eviction strategy: {eviction_strategy}")

        buffer_pool = BufferPool(capacity, eviction_policy)

        # Apply additional configuration
        if 'default_timeout' in kwargs:
            buffer_pool.set_default_timeout(kwargs['default_timeout'])

        return buffer_pool

    @staticmethod
    def create_high_performance_buffer_pool(capacity: int) -> BufferPool:
        """Create a buffer pool optimized for high performance."""
        return BufferPoolFactory.create_buffer_pool(
            capacity * 2,  # Larger capacity
            'lru',         # LRU for good hit rates
            default_timeout=60.0  # Longer timeout for better success rates
        )

    @staticmethod
    def create_memory_constrained_buffer_pool(capacity: int) -> BufferPool:
        """Create a buffer pool optimized for memory-constrained environments."""
        return BufferPoolFactory.create_buffer_pool(
            capacity,      # Normal capacity
            'clock',       # Clock for memory efficiency
            default_timeout=10.0  # Shorter timeout to fail fast
        )

    @staticmethod
    def create_high_concurrency_buffer_pool(capacity: int) -> BufferPool:
        """Create a buffer pool optimized for high concurrency."""
        return BufferPoolFactory.create_buffer_pool(
            capacity,      # Normal capacity
            'lru',         # LRU for simplicity
            default_timeout=5.0   # Short timeout to avoid long waits
        )
