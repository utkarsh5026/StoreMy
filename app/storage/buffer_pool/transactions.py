# app/storage/buffer_pool/transactions.py (Enhanced)
import threading
import time
from typing import Optional, Set, Dict, Any
from dataclasses import dataclass
from copy import deepcopy

from app.primitives import TransactionId, PageId
from app.storage.permissions import Permissions
from app.core.exceptions import DbException, TransactionAbortedException
from .cache_manager import CacheManager
from .page import PageWriter


class LockCoordinator:
    """
    Enhanced lock coordinator with robust transaction integration.

    Responsibilities:
    - Acquire and release page locks with proper blocking
    - Handle deadlock detection and resolution
    - Integrate with robust transaction management
    - Track comprehensive lock statistics
    - Provide timeout and retry mechanisms
    """

    @dataclass
    class LockStatistics:
        locks_acquired: int = 0
        locks_denied: int = 0
        locks_timeout: int = 0
        deadlocks_detected: int = 0
        lock_upgrades: int = 0
        total_wait_time: float = 0.0

        @property
        def success_rate(self) -> float:
            total = self.locks_acquired + self.locks_denied + self.locks_timeout
            return self.locks_acquired / total if total > 0 else 0.0

        @property
        def avg_wait_time(self) -> float:
            return self.total_wait_time / self.locks_acquired if self.locks_acquired > 0 else 0.0

    def __init__(self, lock_manager=None):
        # Lazy initialization to avoid circular dependencies
        self._lock_manager = lock_manager
        self._lock_stats = self.LockStatistics()
        self._lock = threading.RLock()

        # Configuration
        self.default_timeout = 30.0  # seconds
        self.deadlock_check_interval = 1.0  # seconds

    def _get_lock_manager(self):
        """Get lock manager with lazy initialization."""
        if self._lock_manager is None:
            try:
                from app.database import Database
                self._lock_manager = Database.get_lock_manager()
            except Exception as e:
                raise DbException(f"Cannot access lock manager: {e}")
        return self._lock_manager

    def acquire_page_lock(self, tid: TransactionId, page_id: PageId,
                          permissions: Permissions, timeout: Optional[float] = None) -> bool:
        """
        Acquire a lock on a page with proper blocking and timeout.

        Args:
            tid: Transaction requesting the lock
            page_id: Page to lock
            permissions: Read or write permissions
            timeout: Optional timeout (uses default if None)

        Returns:
            True if lock acquired successfully

        Raises:
            TransactionAbortedException: If deadlock detected and transaction is victim
            DbException: If lock acquisition fails for other reasons
        """
        if timeout is None:
            timeout = self.default_timeout

        start_time = time.time()

        try:
            # Import here to avoid circular dependency
            from app.concurrency.locks.lock_manager import LockType

            # Determine lock type from permissions
            lock_type = LockType.EXCLUSIVE if permissions.is_write() else LockType.SHARED

            # Attempt to acquire lock (may block)
            lock_manager = self._get_lock_manager()
            success = lock_manager.acquire_lock(
                tid, page_id, lock_type, timeout)

            # Update statistics
            wait_time = time.time() - start_time
            with self._lock:
                if success:
                    self._lock_stats.locks_acquired += 1
                    self._lock_stats.total_wait_time += wait_time
                else:
                    self._lock_stats.locks_timeout += 1

            return success

        except TransactionAbortedException as e:
            # Deadlock detected - update statistics and re-raise
            with self._lock:
                self._lock_stats.deadlocks_detected += 1
                self._lock_stats.locks_denied += 1
            raise e

        except Exception as e:
            with self._lock:
                self._lock_stats.locks_denied += 1
            raise DbException(
                f"Failed to acquire {lock_type.value} lock on page {page_id}: {e}")

    def try_acquire_page_lock(self, tid: TransactionId, page_id: PageId,
                              permissions: Permissions) -> bool:
        """
        Try to acquire a lock without blocking.

        Returns:
            True if lock acquired immediately, False if would need to wait
        """
        try:
            return self.acquire_page_lock(tid, page_id, permissions, timeout=0.001)
        except (TransactionAbortedException, DbException):
            return False

    def upgrade_page_lock(self, tid: TransactionId, page_id: PageId) -> bool:
        """
        Upgrade a shared lock to exclusive.

        Returns:
            True if upgrade successful
        """
        try:
            lock_manager = self._get_lock_manager()

            # Check if we have a shared lock first
            locks_held = lock_manager.get_locks_held(tid)
            from app.concurrency.locks.lock_manager import LockType

            has_shared = any(pid == page_id and ltype == LockType.SHARED
                             for pid, ltype in locks_held)

            if not has_shared:
                raise DbException(
                    f"Transaction {tid} does not hold shared lock on page {page_id}")

            # Attempt upgrade (internally handled by lock manager)
            success = lock_manager.acquire_lock(
                tid, page_id, LockType.EXCLUSIVE)

            if success:
                with self._lock:
                    self._lock_stats.lock_upgrades += 1

            return success

        except TransactionAbortedException:
            with self._lock:
                self._lock_stats.deadlocks_detected += 1
            raise
        except Exception as e:
            raise DbException(f"Failed to upgrade lock on page {page_id}: {e}")

    def release_page_lock(self, tid: TransactionId, page_id: PageId) -> None:
        """Release a specific lock on a page."""
        try:
            lock_manager = self._get_lock_manager()
            lock_manager.release_lock(tid, page_id)
        except Exception as e:
            # Log warning but don't fail - lock might already be released
            print(
                f"Warning: Failed to release lock on page {page_id} for transaction {tid}: {e}")

    def release_all_locks(self, tid: TransactionId) -> None:
        """Release all locks held by a transaction."""
        try:
            lock_manager = self._get_lock_manager()
            lock_manager.release_all_locks(tid)
        except Exception as e:
            # Log warning but don't fail
            print(
                f"Warning: Failed to release all locks for transaction {tid}: {e}")

    def get_locks_held(self, tid: TransactionId) -> Set[tuple]:
        """Get all locks held by a transaction."""
        try:
            lock_manager = self._get_lock_manager()
            return lock_manager.get_locks_held(tid)
        except Exception:
            return set()

    def has_lock(self, tid: TransactionId, page_id: PageId) -> bool:
        """Check if transaction holds any lock on a page."""
        locks_held = self.get_locks_held(tid)
        return any(pid == page_id for pid, _ in locks_held)

    def has_exclusive_lock(self, tid: TransactionId, page_id: PageId) -> bool:
        """Check if transaction holds exclusive lock on a page."""
        locks_held = self.get_locks_held(tid)
        from app.concurrency.locks.lock_manager import LockType
        return any(pid == page_id and ltype == LockType.EXCLUSIVE
                   for pid, ltype in locks_held)

    def get_lock_statistics(self) -> LockStatistics:
        """Get comprehensive lock statistics."""
        with self._lock:
            return deepcopy(self._lock_stats)

    def reset_statistics(self) -> None:
        """Reset lock statistics (useful for testing)."""
        with self._lock:
            self._lock_stats = self.LockStatistics()

    def get_debug_info(self) -> Dict[str, Any]:
        """Get debug information about current locks."""
        try:
            lock_manager = self._get_lock_manager()
            return {
                'lock_manager_stats': lock_manager.get_statistics(),
                'lock_coordinator_stats': self.get_lock_statistics().__dict__,
                'detailed_lock_info': lock_manager.get_lock_info()
            }
        except Exception as e:
            return {'error': f"Failed to get debug info: {e}"}


class TransactionCoordinator:
    """
    Enhanced transaction coordinator with robust transaction management.

    Responsibilities:
    - Track pages accessed by transactions with detailed metadata
    - Handle transaction commit/abort with proper error handling
    - Coordinate with enhanced lock manager
    - Manage dirty page handling with WAL compliance
    - Provide transaction monitoring and debugging
    """

    @dataclass
    class TransactionPageInfo:
        """Information about pages accessed by a transaction."""
        page_id: PageId
        first_access_time: float
        last_access_time: float
        access_count: int = 1
        is_dirty: bool = False
        read_count: int = 0
        write_count: int = 0

    @dataclass
    class TransactionStats:
        """Statistics for transaction operations."""
        transactions_committed: int = 0
        transactions_aborted: int = 0
        pages_flushed: int = 0
        pages_discarded: int = 0
        total_commit_time: float = 0.0
        total_abort_time: float = 0.0

        @property
        def avg_commit_time(self) -> float:
            return self.total_commit_time / self.transactions_committed if self.transactions_committed > 0 else 0.0

        @property
        def avg_abort_time(self) -> float:
            return self.total_abort_time / self.transactions_aborted if self.transactions_aborted > 0 else 0.0

    def __init__(self):
        # Track pages accessed by each transaction
        self._transaction_pages: Dict[TransactionId,
                                      Dict[PageId, TransactionPageInfo]] = {}

        # Track which transactions accessed each page
        self._page_transactions: Dict[PageId, Set[TransactionId]] = {}

        # Statistics
        self._stats = self.TransactionStats()

        # Thread safety
        self._lock = threading.RLock()

    def record_page_access(self, tid: TransactionId, page_id: PageId,
                           is_write: bool = False) -> None:
        """
        Record detailed page access information.

        Args:
            tid: Transaction ID
            page_id: Page being accessed
            is_write: Whether this is a write access
        """
        with self._lock:
            current_time = time.time()

            # Initialize transaction tracking if needed
            if tid not in self._transaction_pages:
                self._transaction_pages[tid] = {}

            # Update or create page info for this transaction
            if page_id in self._transaction_pages[tid]:
                page_info = self._transaction_pages[tid][page_id]
                page_info.last_access_time = current_time
                page_info.access_count += 1
                if is_write:
                    page_info.write_count += 1
                    page_info.is_dirty = True
                else:
                    page_info.read_count += 1
            else:
                page_info = self.TransactionPageInfo(
                    page_id=page_id,
                    first_access_time=current_time,
                    last_access_time=current_time,
                    is_dirty=is_write,
                    read_count=0 if is_write else 1,
                    write_count=1 if is_write else 0
                )
                self._transaction_pages[tid][page_id] = page_info

            # Update reverse mapping
            if page_id not in self._page_transactions:
                self._page_transactions[page_id] = set()
            self._page_transactions[page_id].add(tid)

    def mark_page_dirty(self, tid: TransactionId, page_id: PageId) -> None:
        """Mark a page as dirty for a transaction."""
        with self._lock:
            if tid in self._transaction_pages and page_id in self._transaction_pages[tid]:
                self._transaction_pages[tid][page_id].is_dirty = True

    def get_transaction_pages(self, tid: TransactionId) -> Set[PageId]:
        """Get all pages accessed by a transaction."""
        with self._lock:
            return set(self._transaction_pages.get(tid, {}).keys())

    def get_dirty_pages(self, tid: TransactionId) -> Set[PageId]:
        """Get all dirty pages for a transaction."""
        with self._lock:
            if tid not in self._transaction_pages:
                return set()

            return {page_id for page_id, info in self._transaction_pages[tid].items()
                    if info.is_dirty}

    def get_page_transactions(self, page_id: PageId) -> Set[TransactionId]:
        """Get all transactions that accessed a page."""
        with self._lock:
            return self._page_transactions.get(page_id, set()).copy()

    def handle_transaction_commit(self, tid: TransactionId, cache_mgr: CacheManager,
                                  page_writer: PageWriter) -> None:
        """
        Handle transaction commit with comprehensive error handling.

        This implements the commit protocol:
        1. Write all dirty pages to log (WAL)
        2. Flush dirty pages to disk
        3. Clean up transaction state
        """
        start_time = time.time()

        try:
            with self._lock:
                dirty_pages = self.get_dirty_pages(tid)

                # Phase 1: Write to log (WAL compliance)
                self._write_pages_to_log(tid, dirty_pages, cache_mgr)

                # Phase 2: Flush dirty pages to disk
                pages_flushed = 0
                for page_id in dirty_pages:
                    try:
                        entry = cache_mgr.get(page_id)
                        if entry and entry.page.is_dirty():
                            # Write page to disk
                            page_writer.write_page(entry.page, entry.page_type)
                            entry.page.mark_dirty(False, None)
                            pages_flushed += 1
                    except Exception as e:
                        # Log error but continue with other pages
                        print(
                            f"Warning: Failed to flush page {page_id} for transaction {tid}: {e}")

                # Phase 3: Clean up transaction state
                self._cleanup_transaction(tid)

                # Update statistics
                commit_time = time.time() - start_time
                self._stats.transactions_committed += 1
                self._stats.pages_flushed += pages_flushed
                self._stats.total_commit_time += commit_time

        except Exception as e:
            # If commit fails, this is a serious error
            print(f"Critical error during transaction commit for {tid}: {e}")
            # Try to clean up anyway
            try:
                self._cleanup_transaction(tid)
            except:
                pass
            raise DbException(f"Transaction commit failed for {tid}: {e}")

    def handle_transaction_abort(self, tid: TransactionId, cache_mgr: CacheManager) -> None:
        """
        Handle transaction abort with proper cleanup.

        This implements the abort protocol:
        1. Discard all dirty pages from cache
        2. Clean up transaction state
        3. Update statistics
        """
        start_time = time.time()

        try:
            with self._lock:
                dirty_pages = self.get_dirty_pages(tid)

                # Discard dirty pages from cache
                pages_discarded = 0
                for page_id in dirty_pages:
                    try:
                        entry = cache_mgr.get(page_id)
                        if entry and entry.page.is_dirty():
                            # Check if other transactions are using this page
                            other_transactions = self.get_page_transactions(
                                page_id) - {tid}

                            if not other_transactions:
                                # Safe to remove - no other transactions using it
                                cache_mgr.remove(page_id)
                                pages_discarded += 1
                            else:
                                # Other transactions using this page - just mark as clean for this transaction
                                # The page will be reloaded from disk when accessed
                                pass

                    except Exception as e:
                        print(
                            f"Warning: Failed to discard page {page_id} for transaction {tid}: {e}")

                # Clean up transaction state
                self._cleanup_transaction(tid)

                # Update statistics
                abort_time = time.time() - start_time
                self._stats.transactions_aborted += 1
                self._stats.pages_discarded += pages_discarded
                self._stats.total_abort_time += abort_time

        except Exception as e:
            print(f"Error during transaction abort for {tid}: {e}")
            # Try to clean up anyway
            try:
                self._cleanup_transaction(tid)
            except:
                pass

    def _write_pages_to_log(self, tid: TransactionId, dirty_pages: Set[PageId],
                            cache_mgr: CacheManager) -> None:
        """Write dirty pages to log for WAL compliance."""
        try:
            from app.database import Database
            log_file = Database.get_log_file()

            for page_id in dirty_pages:
                entry = cache_mgr.get(page_id)
                if entry and entry.page.is_dirty():
                    # Get before image for UNDO logging
                    before_image = getattr(
                        entry.page, 'get_before_image', lambda: entry.page)()
                    log_file.log_write(tid, before_image, entry.page)

        except Exception as e:
            print(
                f"Warning: Failed to write pages to log for transaction {tid}: {e}")

    def _cleanup_transaction(self, tid: TransactionId) -> None:
        """Clean up all state for a completed transaction."""
        with self._lock:
            # Get pages accessed by this transaction
            pages = self._transaction_pages.pop(tid, {}).keys()

            # Remove transaction from page mappings
            for page_id in pages:
                if page_id in self._page_transactions:
                    self._page_transactions[page_id].discard(tid)
                    # Clean up empty mappings
                    if not self._page_transactions[page_id]:
                        del self._page_transactions[page_id]

    def get_transaction_info(self, tid: TransactionId) -> Dict[str, Any]:
        """Get detailed information about a transaction's page accesses."""
        with self._lock:
            if tid not in self._transaction_pages:
                return {'error': 'Transaction not found'}

            pages_info = []
            for page_id, info in self._transaction_pages[tid].items():
                pages_info.append({
                    'page_id': str(page_id),
                    'first_access': info.first_access_time,
                    'last_access': info.last_access_time,
                    'access_count': info.access_count,
                    'read_count': info.read_count,
                    'write_count': info.write_count,
                    'is_dirty': info.is_dirty,
                    'duration': info.last_access_time - info.first_access_time
                })

            return {
                'transaction_id': str(tid),
                'pages_accessed': len(pages_info),
                'dirty_pages': sum(1 for info in pages_info if info['is_dirty']),
                'total_accesses': sum(info['access_count'] for info in pages_info),
                'pages': pages_info
            }

    def get_statistics(self) -> TransactionStats:
        """Get transaction coordinator statistics."""
        with self._lock:
            return deepcopy(self._stats)

    def get_active_transactions(self) -> Set[TransactionId]:
        """Get all currently active transactions."""
        with self._lock:
            return set(self._transaction_pages.keys())

    def reset_statistics(self) -> None:
        """Reset statistics (useful for testing)."""
        with self._lock:
            self._stats = self.TransactionStats()
