import time
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..primitives import TransactionId

if TYPE_CHECKING:
    from .page import Page
    from ..primitives import PageId

# Import at runtime what we need
from .permissions import Permissions
from ..concurrency.locks import LockManager
from ..core.exceptions import DbException


@dataclass
class BufferPoolStats:
    hits: int = 0
    misses: int = 0
    hit_rate: float = 0.0
    evictions: int = 0
    disk_reads: int = 0
    disk_writes: int = 0


class BufferPool:
    """
    BufferPool manages the reading and writing of pages into memory from disk.

    The BufferPool is responsible for:
    1. **Caching pages in memory** - Keep frequently accessed pages in RAM
    2. **Managing memory limits** - Evict pages when cache is full
    3. **Coordinating with locks** - Ensure proper concurrency control
    4. **Tracking dirty pages** - Know which pages need to be written back
    5. **Providing ACID guarantees** - Work with transaction system

    Key Design Decisions:
    1. **Fixed-size cache**: Maximum number of pages in memory is set at creation
    2. **LRU eviction**: Least recently used pages are evicted first
    3. **No-steal policy**: Dirty pages cannot be evicted (prevents partial writes)
    4. **Page-level locking**: Each page can be locked independently
    5. **Write-through on commit**: Dirty pages written to disk on transaction commit

    The BufferPool acts as the single point of coordination between:
    - File I/O operations
    - Lock management  
    - Transaction management
    - Memory management
    """

    # The Default number of pages - can be overridden in constructor
    DEFAULT_PAGES = 50

    def __init__(self, num_pages: int = DEFAULT_PAGES):
        """
        Create a BufferPool that caches up to num_pages pages.

        Args:
            num_pages: Maximum number of pages in this buffer pool
        """
        self.num_pages = num_pages

        # LRU cache implementation using OrderedDict
        # Key: PageId, Value: Page
        # Most recently used pages are at the end
        self._pages: 'OrderedDict[PageId, Page]' = OrderedDict()
        self.lock_manager = LockManager()
        self._lock = threading.RLock()

        self._stats = BufferPoolStats()

    def get_page(self, tid: TransactionId, page_id: 'PageId',
                 permissions: Permissions) -> 'Page':
        """
        Retrieve the specified page with the associated permissions.

        This method will acquire a lock and may block if that lock is held
        by another transaction.

        Algorithm:
        1. Acquire the appropriate lock (may block)
        2. Check if page is in buffer pool cache
        3. If not in cache, read from disk and add to cache
        4. If cache is full, evict a page first
        5. Return the page

        Args:
            tid: The ID of the transaction requesting the page
            page_id: The ID of the requested page  
            permissions: The requested permissions (READ_ONLY or READ_WRITE)

        Returns:
            The requested page

        Raises:
            TransactionAbortedException: If deadlock detected or other concurrency issue
            DbException: If page cannot be read or buffer pool is full of dirty pages
        """
        with self._lock:
            # Step 1: Acquire lock (may block until available)
            exclusive_lock = permissions == Permissions.READ_WRITE

            # This call may block or throw TransactionAbortedException
            while not self.lock_manager.acquire_lock(tid, page_id, exclusive_lock):
                # Lock acquisition failed, probably due to conflict
                # The lock manager will handle deadlock detection
                time.sleep(0.001)  # Small sleep to prevent busy waiting

            try:
                # Step 2: Check if page is already in buffer pool
                if page_id in self._pages:
                    # Cache hit! Move to end (mark as most recently used)
                    page = self._pages.pop(page_id)
                    self._pages[page_id] = page
                    self._stats.hits += 1
                    return page

                # Step 3: Cache miss - need to load from disk
                self._stats.misses += 1

                # Step 4: Make room if buffer pool is full
                if len(self._pages) >= self.num_pages:
                    self._evict_page()

                # Step 5: Load page from disk
                page = self._load_page_from_disk(page_id)

                # Step 6: Add to cache
                self._pages[page_id] = page

                return page

            except Exception as e:
                # If anything goes wrong, release the lock
                self.lock_manager.release_lock(tid, page_id)
                raise e

    def _load_page_from_disk(self, page_id: 'PageId') -> 'Page':
        """
        Load a page from disk using the appropriate DbFile.
        """
        from app.database import Database

        table_id = page_id.get_table_id()
        db_file = Database.get_catalog().get_db_file(table_id)

        self._stats.disk_reads += 1
        return db_file.read_page(page_id)

    def _evict_page(self) -> None:
        """
        Evict a page from the buffer pool to make room for new pages.

        Uses the no-steal policy: only evict clean (non-dirty) pages.
        If all pages are dirty, raises an exception.

        Algorithm:
        1. Find the least recently used clean page
        2. Remove it from the buffer pool
        3. If no clean pages exist, raise exception
        """
        # Find a clean page to evict (LRU order)
        for page_id, page in self._pages.items():
            if not page.is_dirty():
                # Found a clean page - evict it
                del self._pages[page_id]
                self._stats.evictions += 1
                return

        # No clean pages available - all pages are dirty
        # This violates the no-steal policy
        raise DbException(
            "Cannot evict page: all pages in buffer pool are dirty. "
            "This may indicate a transaction holding too many locks "
            "or insufficient buffer pool size."
        )

    def release_page(self, tid: TransactionId, page_id: 'PageId') -> None:
        """
        Release the lock on a page.

        WARNING: This is very risky and should only be called when you're
        certain the transaction no longer needs the page.

        Args:
            tid: The ID of the transaction releasing the lock
            page_id: The ID of the page to unlock
        """
        self.lock_manager.release_lock(tid, page_id)

    def holds_lock(self, tid: TransactionId, page_id: 'PageId') -> bool:
        """
        Return True if the specified transaction has a lock on the specified page.

        Args:
            tid: The transaction to check
            page_id: The page to check

        Returns:
            True if the transaction holds a lock on the page
        """
        return self.lock_manager.holds_lock(tid, page_id)

    def insert_tuple(self, tid: TransactionId, table_id: int, tuple_data) -> None:
        """
        Add a tuple to the specified table on behalf of transaction tid.

        This method:
        1. Gets the DbFile for the table
        2. Calls DbFile.add_tuple() which handles page allocation
        3. The DbFile will call back to get_page() for any pages it needs

        Args:
            tid: The transaction adding the tuple
            table_id: The table to add the tuple to
            tuple_data: The tuple to add
        """
        from app.database import Database

        heap_file = Database.get_catalog().get_db_file(table_id)
        heap_file.add_tuple(tid, tuple_data)

    def delete_tuple(self, tid: TransactionId, tuple_data) -> None:
        """
        Remove the specified tuple from the buffer pool.

        This method:
        1. Gets the table ID from the tuple's record ID
        2. Gets the DbFile for the table  
        3. Calls DbFile.delete_tuple() to remove the tuple

        Args:
            tid: The transaction deleting the tuple
            tuple_data: The tuple to delete
        """
        from app.database import Database

        record_id = tuple_data.get_record_id()
        if record_id is None:
            raise DbException("Cannot delete tuple without RecordId")

        table_id = record_id.get_page_id().get_table_id()
        heap_file = Database.get_catalog().get_db_file(table_id)
        heap_file.delete_tuple(tid, tuple_data)

    def flush_all_pages(self) -> None:
        """
        Flush all dirty pages to disk.

        WARNING: This writes dirty data to disk and will break ACID properties
        if used incorrectly. Should only be called during shutdown or checkpoints.
        """
        with self._lock:
            # Copy to avoid modification during iteration
            pages_to_flush = list(self._pages.keys())

            for page_id in pages_to_flush:
                if page_id in self._pages:  # Page might have been evicted
                    self._flush_page(page_id)

    def _flush_page(self, page_id: 'PageId') -> None:
        """
        Flush a specific page to disk if it's dirty.

        Args:
            page_id: The ID of the page to flush
        """
        if page_id not in self._pages:
            return

        page = self._pages[page_id]

        if page.is_dirty():
            # Write the page to disk
            from app.database import Database

            table_id = page_id.get_table_id()
            db_file = Database.get_catalog().get_db_file(table_id)
            db_file.write_page(page)

            # Mark page as clean
            page.mark_dirty(False, None)

            self._stats.disk_writes += 1

    def discard_page(self, page_id: 'PageId') -> None:
        """
        Remove the specific page from the buffer pool.

        This is used by the recovery manager to ensure that the buffer pool
        doesn't keep a rolled-back page in its cache.

        Args:
            page_id: The ID of the page to discard
        """
        with self._lock:
            if page_id in self._pages:
                del self._pages[page_id]

    def transaction_complete(self, tid: TransactionId, commit: bool = True) -> None:
        """
        Commit or abort a given transaction; release all locks associated
        with the transaction.

        Args:
            tid: The ID of the transaction to complete
            commit: True to commit, False to abort
        """
        # Get all pages locked by this transaction
        locked_page_ids = self.lock_manager.get_pages_locked_by_transaction(
            tid)

        if locked_page_ids is None:
            # Transaction has no locks
            return

        with self._lock:
            for page_id in locked_page_ids:
                # Only process pages that are still in the buffer pool
                if page_id not in self._pages:
                    continue

                page = self._pages[page_id]

                if page.is_dirty():
                    if commit:
                        # Commit: write dirty pages to disk
                        self._flush_page(page_id)
                    else:
                        # Abort: discard dirty pages
                        self.discard_page(page_id)

        # Release all locks held by this transaction
        self.lock_manager.release_all_locks(tid)

    def get_stats(self) -> BufferPoolStats:
        """
        Get buffer pool statistics for monitoring and debugging.

        Returns:
            Dictionary containing hit rate, miss rate, etc.
        """
        total_requests = self._stats.hits + self._stats.misses
        hit_rate = self._stats.hits / \
            total_requests if total_requests > 0 else 0

        self._stats.hit_rate = hit_rate
        return self._stats

    def __str__(self) -> str:
        """Return a string representation for debugging."""
        stats = self.get_stats()
        return (f"BufferPool(pages={len(self._pages)}/{self.num_pages}, "
                f"hit_rate={stats.hit_rate:.2%}, "
                f"dirty_pages={sum(1 for p in self._pages.values() if p.is_dirty())})")
