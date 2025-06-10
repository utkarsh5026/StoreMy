import threading
from dataclasses import dataclass
from typing import Dict, Set, Optional
import collections

from ...concurrency.transactions import TransactionId
from ...storage.page import PageId
from ...core.exceptions import TransactionAbortedException


class Lock:
    """
    Represents a lock held by a transaction on a page.

    Attributes:
        tid: Transaction holding the lock
        page_id: Page being locked
        exclusive: True for exclusive (write) lock, False for shared (read) lock
    """

    def __init__(self, tid: TransactionId, page_id: PageId, exclusive: bool):
        self.tid = tid
        self.page_id = page_id
        self.exclusive = exclusive

    def __eq__(self, other) -> bool:
        if not isinstance(other, Lock):
            return False
        return (self.tid == other.tid and
                self.page_id == other.page_id and
                self.exclusive == other.exclusive)

    def __hash__(self) -> int:
        return hash((self.tid, self.page_id, self.exclusive))

    def __str__(self) -> str:
        lock_type = "EXCLUSIVE" if self.exclusive else "SHARED"
        return f"Lock({self.tid}, {self.page_id}, {lock_type})"


@dataclass(frozen=True)
class LockInfo:
    """
    Information about the current locks in the database.

    This is used for debugging and monitoring purposes.
    """

    total_locks: int
    active_transactions: int
    locked_pages: int
    page_locks: dict[PageId, list[Lock]]
    transaction_pages: dict[TransactionId, set[PageId]]


class LockManager:
    """
    Manages locks for concurrency control in the database.

    This is a simplified version for Phase 2. The full implementation
    with deadlock detection will come in Phase 5.

    Key responsibilities:
    1. Grant and release locks on pages
    2. Enforce lock compatibility rules
    3. Track which transactions hold which locks
    4. (Later) Detect and resolve deadlocks

    Lock Compatibility Rules:
    - Multiple shared locks can coexist
    - Exclusive lock conflicts with any other lock
    - Lock upgrade (shared -> exclusive) is allowed if only one shared lock exists
    """

    def __init__(self):
        """Initialize the lock manager."""
        self.page_locks: dict[PageId, list[Lock]] = collections.defaultdict(list)
        self.transaction_pages: dict[TransactionId,
                                     set[PageId]] = collections.defaultdict(set)


        self._lock = threading.RLock()

    def acquire_lock(self, tid: TransactionId, page_id: PageId, exclusive: bool) -> bool:
        """
        Attempt to acquire a lock on a page.

        Args:
            tid: Transaction requesting the lock
            page_id: Page to lock
            exclusive: True for exclusive lock, False for shared lock

        Returns:
            True if lock was acquired, False if it would conflict

        Raises:
            TransactionAbortedException: If deadlock is detected (future implementation)
        """
        with self._lock:
            existing_lock = self._get_lock_held_by_transaction(tid, page_id)
            if existing_lock:
                if existing_lock.exclusive or not exclusive:
                    return True
                else:
                    return self._try_lock_upgrade(tid, page_id)

            # Check if we can acquire the new lock
            if self._can_acquire_lock(tid, page_id, exclusive):
                # Grant the lock
                new_lock = Lock(tid, page_id, exclusive)
                self.page_locks[page_id].append(new_lock)
                self.transaction_pages[tid].add(page_id)
                return True

            return False

    def _get_lock_held_by_transaction(self, tid: TransactionId, page_id: PageId) -> Optional[Lock]:
        """
        Get the lock held by a transaction on a specific page.

        Returns:
            The lock if found, None otherwise
        """
        for lock in self.page_locks[page_id]:
            if lock.tid == tid:
                return lock
        return None

    def _can_acquire_lock(self, tid: TransactionId, page_id: PageId, exclusive: bool) -> bool:
        """
        Check if a transaction can acquire a lock on a page.

        Lock compatibility rules:
        - Exclusive lock: No other locks allowed
        - Shared lock: Other shared locks allowed, exclusive locks not allowed
        """
        existing_locks = self.page_locks[page_id]

        if not existing_locks:
            # No existing locks, can always acquire
            return True

        if exclusive:
            # Requesting exclusive lock - no other locks allowed
            return False

        # Requesting shared lock - check if any exclusive locks exist
        for lock in existing_locks:
            if lock.exclusive:
                return False

        return True

    def _try_lock_upgrade(self, tid: TransactionId, page_id: PageId) -> bool:
        """
        Try to upgrade a shared lock to an exclusive lock.

        This is only possible if the transaction holds the only shared lock
        on the page.

        Returns:
            True if upgrade successful, False otherwise
        """
        existing_locks = self.page_locks[page_id]

        if len(existing_locks) == 1:
            # Only one lock exists
            only_lock = existing_locks[0]
            if only_lock.tid == tid and not only_lock.exclusive:
                # We hold the only shared lock - upgrade it
                only_lock.exclusive = True
                return True

        return False

    def release_lock(self, tid: TransactionId, page_id: PageId) -> None:
        """
        Release a lock held by a transaction on a page.

        Args:
            tid: Transaction releasing the lock
            page_id: Page to unlock
        """
        with self._lock:
            # Remove lock from page_locks
            locks = self.page_locks[page_id]
            locks[:] = [lock for lock in locks if lock.tid != tid]

            # Clean up empty lock lists
            if not locks:
                del self.page_locks[page_id]

            # Remove page from transaction_pages
            if tid in self.transaction_pages:
                self.transaction_pages[tid].discard(page_id)
                if not self.transaction_pages[tid]:
                    del self.transaction_pages[tid]

    def release_all_locks(self, tid: TransactionId) -> None:
        """
        Release all locks held by a transaction.

        This is called when a transaction commits or aborts.

        Args:
            tid: Transaction to release locks for
        """
        with self._lock:
            if tid not in self.transaction_pages:
                return

            # Get a copy of the pages to avoid modification during iteration
            pages_to_release = list(self.transaction_pages[tid])

            for page_id in pages_to_release:
                self.release_lock(tid, page_id)

    def holds_lock(self, tid: TransactionId, page_id: PageId) -> bool:
        """
        Check if a transaction holds a lock on a page.

        Args:
            tid: Transaction to check
            page_id: Page to check

        Returns:
            True if the transaction holds a lock on the page
        """
        with self._lock:
            return page_id in self.transaction_pages.get(tid, set())

    def get_pages_locked_by_transaction(self, tid: TransactionId) -> Optional[Set[PageId]]:
        """
        Get all pages locked by a transaction.

        Args:
            tid: Transaction to check

        Returns:
            Set of page IDs locked by the transaction, or None if no locks
        """
        with self._lock:
            return self.transaction_pages.get(tid)

    def get_lock_info(self) -> LockInfo:
        """
        Get information about current locks for debugging.

        Returns:
            LockInfo object with lock statistics and current state
        """
        with self._lock:
            total_locks = sum(len(locks) for locks in self.page_locks.values())
            active_transactions = len(self.transaction_pages)
            locked_pages = len(self.page_locks)

            return LockInfo(
                total_locks=total_locks,
                active_transactions=active_transactions,
                locked_pages=locked_pages,
                page_locks=dict(self.page_locks),
                transaction_pages=dict(self.transaction_pages)
            )
