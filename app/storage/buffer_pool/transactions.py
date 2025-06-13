from app.primitives import TransactionId, PageId
from app.storage.permissions import Permissions
import threading
from dataclasses import dataclass
from copy import deepcopy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.concurrency.locks import LockManager, LockType
from app.core.exceptions import DbException
from .cache_manager import CacheManager
from .page import PageWriter


class LockCoordinator:
    """
    Coordinates page access with the lock manager.

    Responsibilities:
    - Acquire and release page locks
    - Handle deadlock detection
    - Track lock statistics
    - Coordinate with transaction system
    """

    @dataclass
    class LockStatistics:
        locks_acquired: int = 0
        locks_denied: int = 0
        deadlocks: int = 0

    def __init__(self, lock_manager: LockManager):
        self.lock_manager = lock_manager
        self._lock_stats = self.LockStatistics()
        self._lock = threading.RLock()

    def acquire_page_lock(self, tid: TransactionId, page_id: PageId,
                          permissions: Permissions) -> bool:
        """Acquire a lock on a page."""

        # Determine lock type
        lock_type = LockType.EXCLUSIVE if permissions.is_write() else LockType.SHARED

        try:
            success = self.lock_manager.acquire_lock(tid, page_id, lock_type)

            with self._lock:
                if success:
                    self._lock_stats.locks_acquired += 1
                else:
                    self._lock_stats.locks_denied += 1
                    # Check if denial was due to deadlock
                    if self.lock_manager.has_deadlock(tid):
                        self._lock_stats.deadlocks += 1

            return success

        except Exception as e:
            with self._lock:
                self._lock_stats.locks_denied += 1
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

    def get_lock_statistics(self) -> LockStatistics:
        """Get lock statistics."""
        with self._lock:
            return deepcopy(self._lock_stats)


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
        self._transaction_pages: dict[TransactionId, set[PageId]] = {}
        self._page_transactions: dict[PageId, set[TransactionId]] = {}
        self._lock = threading.RLock()

    def record_page_access(self, tid: TransactionId, page_id: PageId) -> None:
        """Record that a transaction accessed a page."""
        with self._lock:
            if tid not in self._transaction_pages:
                self._transaction_pages[tid] = set()
            self._transaction_pages[tid].add(page_id)

            if page_id not in self._page_transactions:
                self._page_transactions[page_id] = set()
            self._page_transactions[page_id].add(tid)

    def get_transaction_pages(self, tid: TransactionId) -> set[PageId]:
        """Get all pages accessed by a transaction."""
        with self._lock:
            return self._transaction_pages.get(tid, set()).copy()

    def get_page_transactions(self, page_id: PageId) -> set[TransactionId]:
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
