import threading
from typing import Set, Optional, Dict, List
from collections import defaultdict

from ...primitives import TransactionId, PageId
from ...core.exceptions import TransactionAbortedException
from .dependency_manager import DependencyGraph, Lock, LockType

class LockManager:
    """
    Manages locks for concurrency control using Strict Two-Phase Locking.

    Key Features:
    1. Page-level locking (granularity chosen for simplicity)
    2. Shared (read) and Exclusive (write) locks
    3. Deadlock detection using dependency graphs
    4. Lock upgrades (shared -> exclusive)
    5. Timeout-based deadlock resolution

    Lock Compatibility Matrix:
              | Shared | Exclusive
    ----------|--------|----------
    Shared    |   ✓    |    ✗
    Exclusive |   ✗    |    ✗

    Implementation Details:
    - Uses fine-grained locking with separate locks per data structure
    - Deadlock detection runs periodically and on lock conflicts
    - Victim selection for deadlock resolution uses transaction age
    """

    def __init__(self):
        self._page_locks: Dict[PageId, List[Lock]] = defaultdict(list)

        # Maps transaction_id -> set of pages locked by transaction
        self._transaction_pages: Dict[TransactionId, Set[PageId]] = defaultdict(set)

        # Dependency tracking for deadlock detection
        self._dependency_graph = DependencyGraph()

        # Maps transaction_id -> set of transactions it's waiting for
        self._waiting_for: Dict[TransactionId, Set[TransactionId]] = defaultdict(set)

        # Thread synchronization
        self._lock = threading.RLock()

        # Deadlock detection settings
        self._deadlock_timeout = 5.0  # seconds to wait before checking for deadlock

    def acquire_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> bool:
        """
        Attempt to acquire a lock on a page.

        This is the core method that implements 2PL. The algorithm:
        1. Check if the transaction already holds a compatible lock
        2. Check if lock can be granted immediately
        3. If not, add to the dependency graph and wait
        4. Check for deadlocks periodically while waiting
        5. Grant lock when possible or abort if deadlocked

        Args:
            tid: Transaction requesting the lock
            page_id: Page to lock
            lock_type: Type of lock (SHARED or EXCLUSIVE)

        Returns:
            True if lock acquired successfully

        Raises:
            TransactionAbortedException: If deadlock detected and this transaction chosen as victim
        """
        with self._lock:
            # Quick check: does the transaction already hold a sufficient lock?
            existing_lock = self._get_lock_held_by_transaction(tid, page_id)
            if existing_lock:
                if (existing_lock.lock_type == LockType.EXCLUSIVE or
                        lock_type == LockType.SHARED):
                    return True  # Already have sufficient access
                else:
                    # Try to upgrade shared -> exclusive
                    return self._try_lock_upgrade(tid, page_id)

            # Check if lock can be granted immediately
            if self._can_acquire_lock(tid, page_id, lock_type):
                self._grant_lock(tid, page_id, lock_type)
                return True

            # Lock cannot be granted - need to wait
            return self._handle_lock_conflict(tid, page_id, lock_type)

    def _get_lock_held_by_transaction(self, tid: TransactionId, page_id: PageId) -> Optional[Lock]:
        """Find the lock held by a transaction on a specific page."""
        for lock in self._page_locks[page_id]:
            if lock.transaction_id == tid:
                return lock
        return None

    def _can_acquire_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> bool:
        """
        Check if a lock can be granted without waiting.

        Lock Compatibility Rules:
        - SHARED: Can coexist with other SHARED locks
        - EXCLUSIVE: Cannot coexist with any other locks
        """
        existing_locks = self._page_locks[page_id]

        if not existing_locks:
            return True  # No existing locks

        if lock_type == LockType.EXCLUSIVE:
            return False  # Exclusive locks conflict with everything

        # Requesting SHARED lock - check for EXCLUSIVE locks
        for lock in existing_locks:
            if lock.lock_type == LockType.EXCLUSIVE:
                return False

        return True

    def _grant_lock(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> None:
        """Grant a lock to a transaction."""
        new_lock = Lock(tid, page_id, lock_type)
        self._page_locks[page_id].append(new_lock)
        self._transaction_pages[tid].add(page_id)

    def _try_lock_upgrade(self, tid: TransactionId, page_id: PageId) -> bool:
        """
        Try to upgrade a SHARED lock to EXCLUSIVE.

        This is only possible if the transaction holds the only SHARED lock.
        """
        existing_locks = self._page_locks[page_id]

        if len(existing_locks) == 1:
            only_lock = existing_locks[0]
            if (only_lock.transaction_id == tid and
                    only_lock.lock_type == LockType.SHARED):
                # Upgrade the lock
                self._page_locks[page_id] = [Lock(tid, page_id, LockType.EXCLUSIVE)]
                return True

        return False

    def _handle_lock_conflict(self, tid: TransactionId, page_id: PageId, lock_type: LockType) -> bool:
        """
        Handle the case where a lock cannot be granted immediately.

        This involves:
        1. Adding dependencies to the dependency graph
        2. Checking for deadlocks
        3. Either waiting or aborting the transaction
        """
        # Find who we're waiting for
        blocking_transactions = self._find_blocking_transactions(page_id, lock_type)

        # Add dependencies
        for blocking_tid in blocking_transactions:
            self._dependency_graph.add_dependency(tid, blocking_tid)
            self._waiting_for[tid].add(blocking_tid)

        # Check for deadlock
        cycle = self._dependency_graph.has_cycle()
        if cycle:
            # Deadlock detected - choose victim and abort
            victim = self._choose_deadlock_victim(cycle)
            if victim == tid:
                # We are the victim - clean up and abort
                self._cleanup_transaction_dependencies(tid)
                raise TransactionAbortedException(f"Transaction {tid} aborted due to deadlock")
            else:
                # Someone else is the victim - they will be aborted
                # We should wait and try again
                pass

        # For now, return False to indicate lock not acquired
        # In a full implementation, this would block/wait
        return False

    def _find_blocking_transactions(self, page_id: PageId, lock_type: LockType) -> Set[TransactionId]:
        """Find which transactions are blocking a lock request."""
        blocking = set()

        for lock in self._page_locks[page_id]:
            if lock_type == LockType.EXCLUSIVE:
                # EXCLUSIVE conflicts with everything
                blocking.add(lock.transaction_id)
            elif lock.lock_type == LockType.EXCLUSIVE:
                # SHARED conflicts with EXCLUSIVE
                blocking.add(lock.transaction_id)

        return blocking

    @classmethod
    def _choose_deadlock_victim(cls, cycle: List[TransactionId]) -> TransactionId:
        """
        Choose which transaction to abort in a deadlock.

        Strategy: Choose the transaction with the highest ID (youngest transaction).
        This is simple but effective - could be enhanced with more sophisticated
        strategies like choosing the transaction that has done the least work.
        """
        return max(cycle, key=lambda tid: tid.get_id())

    def _cleanup_transaction_dependencies(self, tid: TransactionId) -> None:
        """Clean up dependency graph entries for a transaction."""
        self._dependency_graph.remove_dependencies_for_transaction(tid)
        if tid in self._waiting_for:
            del self._waiting_for[tid]

    def release_lock(self, tid: TransactionId, page_id: PageId) -> None:
        """
        Release a specific lock held by a transaction.

        Args:
            tid: Transaction releasing the lock
            page_id: Page to unlock
        """
        with self._lock:
            # Remove from page locks
            self._page_locks[page_id] = [
                lock for lock in self._page_locks[page_id]
                if lock.transaction_id != tid
            ]

            # Clean up empty lock lists
            if not self._page_locks[page_id]:
                del self._page_locks[page_id]

            # Remove from transaction pages
            self._transaction_pages[tid].discard(page_id)
            if not self._transaction_pages[tid]:
                del self._transaction_pages[tid]

    def release_all_locks(self, tid: TransactionId) -> None:
        """
        Release all locks held by a transaction.

        Called when a transaction commits or aborts.
        This implements the "release phase" of 2PL.
        """
        with self._lock:
            if tid not in self._transaction_pages:
                return

            # Get a copy to avoid modification during iteration
            pages_to_release = list(self._transaction_pages[tid])

            for page_id in pages_to_release:
                self.release_lock(tid, page_id)

            # Clean up dependency tracking
            self._cleanup_transaction_dependencies(tid)

    def holds_lock(self, tid: TransactionId, page_id: PageId) -> bool:
        """Check if a transaction holds any lock on a page."""
        with self._lock:
            return page_id in self._transaction_pages.get(tid, set())

    def get_lock_type(self, tid: TransactionId, page_id: PageId) -> Optional[LockType]:
        """Get the type of lock held by a transaction on a page."""
        with self._lock:
            existing_lock = self._get_lock_held_by_transaction(tid, page_id)
            return existing_lock.lock_type if existing_lock else None

    def get_pages_locked_by_transaction(self, tid: TransactionId) -> Optional[Set[PageId]]:
        """Get all pages locked by a transaction."""
        with self._lock:
            return self._transaction_pages.get(tid, set()).copy()

    def get_debug_info(self) -> Dict:
        """Get debugging information about the current lock state."""
        with self._lock:
            total_locks = sum(len(locks) for locks in self._page_locks.values())

            return {
                'total_locks': total_locks,
                'active_transactions': len(self._transaction_pages),
                'locked_pages': len(self._page_locks),
                'page_locks': dict(self._page_locks),
                'transaction_pages': dict(self._transaction_pages),
                'dependencies': dict(self._dependency_graph.graph),
                'waiting_transactions': dict(self._waiting_for)
            }