import time
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Dict, Set

from ..primitives import TransactionId, PageId
from ..storage.permissions import Permissions
from ..core.exceptions import DbException, TransactionAbortedException
from ..concurrency.locks import LockManager, LockType

if TYPE_CHECKING:
    from .page import Page


@dataclass
class BufferPoolStats:
    """Statistics for buffer pool monitoring and performance tuning."""
    hits: int = 0
    misses: int = 0
    hit_rate: float = 0.0
    evictions: int = 0
    disk_reads: int = 0
    disk_writes: int = 0
    deadlocks_detected: int = 0
    lock_waits: int = 0
    lock_timeouts: int = 0


class BufferPool:
    """
    Enhanced BufferPool with robust concurrency control.
    
    The BufferPool is the central coordinator for:
    1. Page caching and memory management
    2. Lock acquisition and coordination
    3. Transaction integration
    4. Deadlock detection and resolution
    5. Write-ahead logging coordination
    
    Key Design Principles:
    1. **No-Steal Policy**: Dirty pages cannot be evicted until transaction commits
    2. **Force Policy**: Dirty pages are written to disk on transaction commit
    3. **Page-Level Locking**: All locking is at page granularity
    4. **Strict 2PL**: Locks held until transaction completion
    5. **Deadlock Detection**: Proactive cycle detection in lock dependencies
    
    Cache Management:
    - LRU eviction for clean pages
    - Fixed-size cache with configurable capacity
    - Thread-safe operations with fine-grained locking
    """
    
    DEFAULT_PAGES = 50
    LOCK_TIMEOUT = 5.0  # seconds to wait for a lock before checking for deadlock
    
    def __init__(self, num_pages: int = DEFAULT_PAGES):
        """
        Create a BufferPool that caches up to num_pages pages.
        
        Args:
            num_pages: Maximum number of pages in buffer cache
        """
        self.num_pages = num_pages
        
        # LRU cache: PageId -> Page
        # OrderedDict maintains insertion order, most recent at end
        self._pages: 'OrderedDict[PageId, Page]' = OrderedDict()
        
        # Enhanced lock manager with deadlock detection
        self.lock_manager = LockManager()
        
        # Thread synchronization
        self._cache_lock = threading.RLock()
        
        # Statistics and monitoring
        self._stats = BufferPoolStats()
        
        # Configuration
        self.enable_deadlock_detection = True
        self.deadlock_check_interval = 1.0  # seconds
        
    def get_page(self, tid: TransactionId, page_id: 'PageId', 
                 permissions: Permissions) -> 'Page':
        """
        Retrieve a page with proper locking and caching.
        
        This is the main entry point for page access. The algorithm:
        1. Determine required lock type based on permissions
        2. Acquire appropriate lock (may block or abort on deadlock)
        3. Check cache for page
        4. If cache miss, load from disk and add to cache
        5. Update LRU order and return page
        
        Args:
            tid: Transaction requesting the page
            page_id: ID of the page to retrieve
            permissions: READ_ONLY or READ_WRITE
            
        Returns:
            The requested page
            
        Raises:
            TransactionAbortedException: If deadlock detected
            DbException: If page cannot be loaded or cache is full of dirty pages
        """
        # Determine a lock type from permissions
        lock_type = LockType.EXCLUSIVE if permissions == Permissions.READ_WRITE else LockType.SHARED
        
        # Record the access attempt
        self._stats.lock_waits += 1
        
        # Acquire lock with timeout and deadlock detection
        if not self._acquire_lock_with_timeout(tid, page_id, lock_type):
            self._stats.lock_timeouts += 1
            raise TransactionAbortedException(
                f"Could not acquire {lock_type.value} lock on page {page_id} - deadlock detected"
            )
        
        try:
            # Now we have the lock - access the page
            with self._cache_lock:
                # Check cache first
                if page_id in self._pages:
                    # Cache hit - move to end (most recently used)
                    page = self._pages.pop(page_id)
                    self._pages[page_id] = page
                    self._stats.hits += 1
                    
                    # Record page access for transaction
                    self._record_page_access(tid, page_id, permissions)
                    
                    return page
                
                # Cache miss - need to load from disk
                self._stats.misses += 1
                
                # Make room in cache if needed
                if len(self._pages) >= self.num_pages:
                    self._evict_page()
                
                # Load page from disk
                page = self._load_page_from_disk(page_id)
                
                # Add to cache
                self._pages[page_id] = page
                
                # Record page access for transaction
                self._record_page_access(tid, page_id, permissions)
                
                return page
                
        except Exception as e:
            # If anything goes wrong, release the lock
            self.lock_manager.release_lock(tid, page_id)
            raise e
    
    def _acquire_lock_with_timeout(self, tid: TransactionId, page_id: 'PageId', 
                                 lock_type: LockType) -> bool:
        """
        Acquire a lock with timeout and deadlock detection.
        
        This implements the core concurrency control logic:
        1. Try to acquire lock immediately
        2. If blocked, wait with periodic deadlock checks
        3. Abort transaction if deadlock detected
        
        Returns:
            True if lock acquired, False if deadlock detected
        """
        start_time = time.time()
        
        while True:
            try:
                # Try to acquire lock
                if self.lock_manager.acquire_lock(tid, page_id, lock_type):
                    return True
                
                # Lock not available - check for deadlock
                if self.enable_deadlock_detection:
                    cycle = self.lock_manager._dependency_graph.has_cycle()
                    if cycle and tid in cycle:
                        # We're part of a deadlock cycle
                        self._stats.deadlocks_detected += 1
                        return False
                
                # Check timeout
                if time.time() - start_time > self.LOCK_TIMEOUT:
                    # Timeout - assume deadlock
                    return False
                
                # Wait a bit before retrying
                time.sleep(0.01)
                
            except TransactionAbortedException:
                # Deadlock detected by lock manager
                self._stats.deadlocks_detected += 1
                return False
    
    def _record_page_access(self, tid: TransactionId, page_id: 'PageId', 
                          permissions: Permissions) -> None:
        """Record page access for transaction tracking."""
        # Get the transaction and record the access
        # This helps with deadlock detection and statistics
        pass  # Implementation depends on transaction management integration
    
    def _load_page_from_disk(self, page_id: 'PageId') -> 'Page':
        """Load a page from disk using the appropriate DbFile."""
        from app.database import Database
        
        table_id = page_id.get_table_id()
        db_file = Database.get_catalog().get_db_file(table_id)
        
        self._stats.disk_reads += 1
        return db_file.read_page(page_id)
    
    def _evict_page(self) -> None:
        """
        Evict a page from the buffer pool using the no-steal policy.
        
        The no-steal policy means we can only evict clean (non-dirty) pages.
        If all pages are dirty, we must abort the requesting transaction
        or wait for pages to be cleaned.
        """
        # Look for a clean page to evict (LRU order)
        for page_id, page in list(self._pages.items()):
            if not page.is_dirty():
                # Found a clean page - evict it
                del self._pages[page_id]
                self._stats.evictions += 1
                return
        
        # No clean pages available - all pages are dirty
        # This violates the no-steal policy
        raise DbException(
            "Cannot evict page: all pages in buffer pool are dirty. "
            "This may indicate transactions holding too many locks "
            "or insufficient buffer pool size. "
            f"Current capacity: {self.num_pages} pages"
        )
    
    def get_page_if_cached(self, page_id: 'PageId') -> Optional['Page']:
        """
        Get a page if it's currently in the cache, without acquiring locks.
        
        This is used internally for transaction management and logging.
        
        Args:
            page_id: ID of the page to check
            
        Returns:
            The page if cached, None otherwise
        """
        with self._cache_lock:
            return self._pages.get(page_id)
    
    def release_page(self, tid: TransactionId, page_id: 'PageId') -> None:
        """
        Release the lock on a page.
        
        WARNING: This should only be called when you're certain the transaction
        no longer needs the page. In most cases, locks should be held until
        transaction completion (strict 2PL).
        """
        self.lock_manager.release_lock(tid, page_id)
    
    def holds_lock(self, tid: TransactionId, page_id: 'PageId') -> bool:
        """Check if a transaction holds a lock on a page."""
        return self.lock_manager.holds_lock(tid, page_id)
    
    def insert_tuple(self, tid: TransactionId, table_id: int, tuple_data) -> None:
        """
        Add a tuple to the specified table.
        
        This coordinates with the DbFile to find/create pages with space.
        """
        from app.database import Database
        
        heap_file = Database.get_catalog().get_db_file(table_id)
        modified_pages = heap_file.add_tuple(tid, tuple_data)
        
        # Mark pages as dirty and ensure they're in cache
        for page in modified_pages:
            page.mark_dirty(True, tid)
            # Ensure page is in cache
            with self._cache_lock:
                self._pages[page.get_id()] = page
    
    def delete_tuple(self, tid: TransactionId, tuple_data) -> None:
        """
        Remove a tuple from its table.
        
        This coordinates with the DbFile to modify the appropriate page.
        """
        from app.database import Database
        
        record_id = tuple_data.get_record_id()
        if record_id is None:
            raise DbException("Cannot delete tuple without RecordId")
        
        table_id = record_id.get_page_id().get_table_id()
        heap_file = Database.get_catalog().get_db_file(table_id)
        modified_page = heap_file.delete_tuple(tid, tuple_data)
        
        # Mark page as dirty
        modified_page.mark_dirty(True, tid)
    
    def flush_all_pages(self) -> None:
        """
        Flush all dirty pages to disk.
        
        WARNING: This breaks the no-steal policy and should only be used
        during shutdown or checkpoints when no transactions are active.
        """
        with self._cache_lock:
            pages_to_flush = list(self._pages.keys())
            
            for page_id in pages_to_flush:
                if page_id in self._pages:
                    self._flush_page(page_id)
    
    def flush_pages(self, tid: TransactionId) -> None:
        """
        Flush all pages modified by a specific transaction.
        
        This is called during transaction commit to ensure durability.
        """
        pages_to_flush = self.lock_manager.get_pages_locked_by_transaction(tid)
        if not pages_to_flush:
            return
        
        with self._cache_lock:
            for page_id in pages_to_flush:
                if page_id in self._pages:
                    page = self._pages[page_id]
                    if page.is_dirty() == tid:
                        self._flush_page(page_id)
    
    def _flush_page(self, page_id: 'PageId') -> None:
        """Flush a specific page to disk if it's dirty."""
        if page_id not in self._pages:
            return
        
        page = self._pages[page_id]
        
        if page.is_dirty():
            # Write page to disk
            from app.database import Database
            
            table_id = page_id.get_table_id()
            db_file = Database.get_catalog().get_db_file(table_id)
            db_file.write_page(page)
            
            # Mark page as clean
            page.mark_dirty(False, None)
            
            self._stats.disk_writes += 1
    
    def discard_page(self, page_id: 'PageId') -> None:
        """
        Remove a page from the buffer pool.
        
        Used by recovery to ensure rolled-back pages aren't in cache.
        """
        with self._cache_lock:
            if page_id in self._pages:
                del self._pages[page_id]
    
    def transaction_complete(self, tid: TransactionId, commit: bool = True) -> None:
        """
        Handle transaction completion (commit or abort).
        
        This implements the transaction completion protocol:
        1. For commit: flush dirty pages then release locks
        2. For abort: discard dirty pages then release locks
        
        Args:
            tid: Transaction that is completing
            commit: True for commit, False for abort
        """
        # Get pages locked by this transaction
        locked_pages = self.lock_manager.get_pages_locked_by_transaction(tid)
        if not locked_pages:
            return
        
        with self._cache_lock:
            for page_id in locked_pages:
                if page_id not in self._pages:
                    continue
                
                page = self._pages[page_id]
                
                if page.is_dirty() == tid:
                    if commit:
                        # Commit: write dirty pages to disk
                        self._flush_page(page_id)
                    else:
                        # Abort: discard dirty pages
                        self.discard_page(page_id)
        
        # Release all locks held by this transaction
        self.lock_manager.release_all_locks(tid)
    
    def get_stats(self) -> BufferPoolStats:
        """Get buffer pool statistics for monitoring."""
        total_requests = self._stats.hits + self._stats.misses
        hit_rate = self._stats.hits / total_requests if total_requests > 0 else 0
        
        self._stats.hit_rate = hit_rate
        return self._stats
    
    def get_debug_info(self) -> Dict:
        """Get comprehensive debugging information."""
        with self._cache_lock:
            dirty_pages = sum(1 for page in self._pages.values() if page.is_dirty())
            
            return {
                'cache_size': len(self._pages),
                'max_cache_size': self.num_pages,
                'dirty_pages': dirty_pages,
                'clean_pages': len(self._pages) - dirty_pages,
                'stats': self.get_stats(),
                'lock_info': self.lock_manager.get_debug_info(),
                'cached_pages': list(self._pages.keys())
            }
    
    def __str__(self) -> str:
        """Return a string representation for debugging."""
        stats = self.get_stats()
        with self._cache_lock:
            dirty_pages = sum(1 for p in self._pages.values() if p.is_dirty())
            
        return (f"BufferPool(pages={len(self._pages)}/{self.num_pages}, "
                f"hit_rate={stats.hit_rate:.2%}, "
                f"dirty_pages={dirty_pages}, "
                f"deadlocks={stats.deadlocks_detected})")