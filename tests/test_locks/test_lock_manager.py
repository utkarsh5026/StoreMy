"""
Comprehensive tests for the lock manager module.

This module provides extensive testing of the LockManager class,
covering all aspects of lock acquisition, release, upgrade, and deadlock detection.
"""

import pytest
import threading

from app.concurrency.locks.lock_manager import LockManager
from app.concurrency.locks.dependency_manager import LockType
from app.primitives import TransactionId
from app.storage.heap import HeapPageId
from app.core.exceptions import TransactionAbortedException


class TestLockManager:
    """Test the LockManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.lock_manager = LockManager()
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.tid3 = TransactionId()
        self.page1 = HeapPageId(1, 0)
        self.page2 = HeapPageId(1, 1)
        self.page3 = HeapPageId(2, 0)

    def test_lock_manager_initialization(self):
        """Test that lock manager initializes correctly."""
        assert isinstance(self.lock_manager._page_locks, dict)
        assert isinstance(self.lock_manager._transaction_pages, dict)
        assert hasattr(self.lock_manager, '_dependency_graph')
        assert hasattr(self.lock_manager, '_waiting_for')
        assert self.lock_manager._deadlock_timeout == 5.0

    def test_acquire_shared_lock_empty_page(self):
        """Test acquiring a shared lock on an empty page."""
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)

        assert result is True
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.SHARED

        # Check internal state
        assert self.page1 in self.lock_manager._page_locks
        assert len(self.lock_manager._page_locks[self.page1]) == 1
        assert self.page1 in self.lock_manager._transaction_pages[self.tid1]

    def test_acquire_exclusive_lock_empty_page(self):
        """Test acquiring an exclusive lock on an empty page."""
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        assert result is True
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.EXCLUSIVE

    def test_multiple_shared_locks_compatible(self):
        """Test that multiple shared locks are compatible."""
        # First transaction acquires shared lock
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result1 is True

        # Second transaction acquires shared lock on same page
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert result2 is True

        # Both should hold locks
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.lock_manager.holds_lock(self.tid2, self.page1)
        assert len(self.lock_manager._page_locks[self.page1]) == 2

    def test_exclusive_lock_blocks_shared(self):
        """Test that exclusive lock blocks shared locks."""
        # First transaction acquires exclusive lock
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result1 is True

        # Second transaction tries to acquire shared lock - should fail
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert result2 is False

        # Only first transaction should hold lock
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert not self.lock_manager.holds_lock(self.tid2, self.page1)

    def test_shared_lock_blocks_exclusive(self):
        """Test that shared lock blocks exclusive locks."""
        # First transaction acquires shared lock
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result1 is True

        # Second transaction tries to acquire exclusive lock - should fail
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.EXCLUSIVE)
        assert result2 is False

        # Only first transaction should hold lock
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert not self.lock_manager.holds_lock(self.tid2, self.page1)

    def test_exclusive_lock_blocks_exclusive(self):
        """Test that exclusive lock blocks other exclusive locks."""
        # First transaction acquires exclusive lock
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result1 is True

        # Second transaction tries to acquire exclusive lock - should fail
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.EXCLUSIVE)
        assert result2 is False

        # Only first transaction should hold lock
        assert self.lock_manager.holds_lock(self.tid1, self.page1)
        assert not self.lock_manager.holds_lock(self.tid2, self.page1)

    def test_same_transaction_duplicate_shared_lock(self):
        """Test that same transaction can request same shared lock multiple times."""
        # First request
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result1 is True

        # Second request from same transaction
        result2 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result2 is True

        # Should still only have one lock entry
        assert len(self.lock_manager._page_locks[self.page1]) == 1

    def test_same_transaction_duplicate_exclusive_lock(self):
        """Test that same transaction can request same exclusive lock multiple times."""
        # First request
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result1 is True

        # Second request from same transaction
        result2 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result2 is True

        # Should still only have one lock entry
        assert len(self.lock_manager._page_locks[self.page1]) == 1

    def test_lock_upgrade_shared_to_exclusive_success(self):
        """Test successful upgrade from shared to exclusive lock."""
        # Acquire shared lock
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result1 is True

        # Upgrade to exclusive
        result2 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result2 is True

        # Should now have exclusive lock
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.EXCLUSIVE
        assert len(self.lock_manager._page_locks[self.page1]) == 1

    def test_lock_upgrade_shared_to_exclusive_failure(self):
        """Test failed upgrade when multiple shared locks exist."""
        # Two transactions acquire shared locks
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert result1 is True
        assert result2 is True

        # First transaction tries to upgrade - should fail
        result3 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result3 is False

        # Should still have shared lock
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.SHARED

    def test_release_lock_single(self):
        """Test releasing a single lock."""
        # Acquire lock
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result is True

        # Release lock
        self.lock_manager.release_lock(self.tid1, self.page1)

        # Should no longer hold lock
        assert not self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.page1 not in self.lock_manager._page_locks
        assert self.tid1 not in self.lock_manager._transaction_pages

    def test_release_lock_multiple_shared(self):
        """Test releasing one of multiple shared locks."""
        # Two transactions acquire shared locks
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert result1 is True
        assert result2 is True

        # Release first lock
        self.lock_manager.release_lock(self.tid1, self.page1)

        # First should not hold lock, second should
        assert not self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.lock_manager.holds_lock(self.tid2, self.page1)
        assert len(self.lock_manager._page_locks[self.page1]) == 1

    def test_release_all_locks(self):
        """Test releasing all locks for a transaction."""
        # Acquire locks on multiple pages
        self.lock_manager.acquire_lock(self.tid1, self.page1, LockType.SHARED)
        self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(self.tid1, self.page3, LockType.SHARED)

        # Release all locks
        self.lock_manager.release_all_locks(self.tid1)

        # Should not hold any locks
        assert not self.lock_manager.holds_lock(self.tid1, self.page1)
        assert not self.lock_manager.holds_lock(self.tid1, self.page2)
        assert not self.lock_manager.holds_lock(self.tid1, self.page3)
        assert self.tid1 not in self.lock_manager._transaction_pages

    def test_release_nonexistent_lock(self):
        """Test releasing a lock that doesn't exist."""
        # Should not raise an error
        self.lock_manager.release_lock(self.tid1, self.page1)

        # Should not affect empty state
        assert not self.lock_manager.holds_lock(self.tid1, self.page1)

    def test_get_pages_locked_by_transaction(self):
        """Test getting pages locked by a transaction."""
        # Acquire locks on multiple pages
        self.lock_manager.acquire_lock(self.tid1, self.page1, LockType.SHARED)
        self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        pages = self.lock_manager.get_pages_locked_by_transaction(self.tid1)
        assert pages is not None
        assert len(pages) == 2
        assert self.page1 in pages
        assert self.page2 in pages

    def test_get_pages_locked_by_nonexistent_transaction(self):
        """Test getting pages for a transaction that holds no locks."""
        pages = self.lock_manager.get_pages_locked_by_transaction(self.tid1)
        assert pages == set()

    def test_simple_deadlock_detection(self):
        """Test detection of a simple deadlock."""
        # Transaction 1 gets lock on page 1
        result1 = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result1 is True

        # Transaction 2 gets lock on page 2
        result2 = self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)
        assert result2 is True

        # Transaction 1 tries to get lock on page 2 (will conflict)
        result3 = self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)
        assert result3 is False

        # Transaction 2 tries to get lock on page 1 (creates deadlock)
        with pytest.raises(TransactionAbortedException):
            self.lock_manager.acquire_lock(
                self.tid2, self.page1, LockType.EXCLUSIVE)

    def test_deadlock_victim_selection(self):
        """Test that deadlock victim selection chooses the highest ID (youngest)."""
        # Create deadlock scenario
        self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)

        # Create the deadlock
        self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        # The transaction with higher ID should be chosen as victim
        higher_id_tid = self.tid1 if self.tid1.get_id() > self.tid2.get_id() else self.tid2
        lower_id_tid = self.tid2 if self.tid1.get_id() > self.tid2.get_id() else self.tid1

        if higher_id_tid == self.tid2:
            with pytest.raises(TransactionAbortedException):
                self.lock_manager.acquire_lock(
                    self.tid2, self.page1, LockType.EXCLUSIVE)
        else:
            # tid1 has higher ID, so tid2's request should not raise exception
            result = self.lock_manager.acquire_lock(
                self.tid2, self.page1, LockType.EXCLUSIVE)
            # Result will be False as it can't acquire the lock

    def test_no_false_deadlock_detection(self):
        """Test that no deadlock is detected when none exists."""
        # Chain of dependencies without cycle
        self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(
            self.tid3, self.page3, LockType.EXCLUSIVE)

        # Create dependencies but no cycle
        result1 = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.EXCLUSIVE)
        assert result1 is False  # Can't acquire, but no deadlock

        result2 = self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.EXCLUSIVE)
        assert result2 is False  # Can't acquire, but no deadlock

    def test_lock_conflict_dependency_tracking(self):
        """Test that lock conflicts are properly tracked in dependency graph."""
        # T1 gets exclusive lock on page1
        self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        # T2 tries to get lock on page1 - should create dependency
        result = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert result is False

        # Check that dependency was recorded
        deps = self.lock_manager._dependency_graph.graph
        assert self.tid2 in deps
        assert self.tid1 in deps[self.tid2]

    def test_cleanup_on_transaction_completion(self):
        """Test that dependencies are cleaned up when transaction completes."""
        # Create conflict and dependency
        self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(self.tid2, self.page1, LockType.SHARED)

        # Complete transaction 1
        self.lock_manager.release_all_locks(self.tid1)

        # Dependencies should be cleaned up
        assert self.tid1 not in self.lock_manager._dependency_graph.graph
        assert self.tid1 not in self.lock_manager._waiting_for

    def test_thread_safety_concurrent_acquisitions(self):
        """Test thread safety with concurrent lock acquisitions."""
        results = []

        def acquire_lock_worker(tid, page, lock_type):
            try:
                result = self.lock_manager.acquire_lock(tid, page, lock_type)
                results.append((tid, result))
            except TransactionAbortedException:
                results.append((tid, "aborted"))

        # Multiple threads trying to acquire exclusive locks on same page
        threads = []
        tids = [TransactionId() for _ in range(5)]

        for tid in tids:
            thread = threading.Thread(
                target=acquire_lock_worker,
                args=(tid, self.page1, LockType.EXCLUSIVE)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Only one should succeed
        successes = [r for r in results if r[1] is True]
        assert len(successes) == 1

    def test_thread_safety_concurrent_releases(self):
        """Test thread safety with concurrent lock releases."""
        # Set up locks for multiple transactions
        tids = [TransactionId() for _ in range(10)]
        pages = [HeapPageId(i, 0) for i in range(10)]

        # Each transaction gets a lock on its own page
        for tid, page in zip(tids, pages):
            self.lock_manager.acquire_lock(tid, page, LockType.EXCLUSIVE)

        def release_worker(tid):
            self.lock_manager.release_all_locks(tid)

        # Release all locks concurrently
        threads = []
        for tid in tids:
            thread = threading.Thread(target=release_worker, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All locks should be released
        for tid, page in zip(tids, pages):
            assert not self.lock_manager.holds_lock(tid, page)

    def test_complex_deadlock_scenario(self):
        """Test complex deadlock scenario with multiple transactions."""
        # Create a more complex deadlock scenario
        tids = [TransactionId() for _ in range(4)]
        pages = [HeapPageId(i, 0) for i in range(4)]

        # Each transaction gets a lock on one page
        for i, (tid, page) in enumerate(zip(tids, pages)):
            self.lock_manager.acquire_lock(tid, page, LockType.EXCLUSIVE)

        # Create circular wait: T0->P1, T1->P2, T2->P3, T3->P0
        self.lock_manager.acquire_lock(tids[0], pages[1], LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(tids[1], pages[2], LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(tids[2], pages[3], LockType.EXCLUSIVE)

        # This should create a deadlock
        with pytest.raises(TransactionAbortedException):
            self.lock_manager.acquire_lock(
                tids[3], pages[0], LockType.EXCLUSIVE)

    def test_get_debug_info(self):
        """Test the debug info functionality."""
        # Set up some locks
        self.lock_manager.acquire_lock(self.tid1, self.page1, LockType.SHARED)
        self.lock_manager.acquire_lock(self.tid2, self.page1, LockType.SHARED)
        self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        debug_info = self.lock_manager.get_debug_info()

        assert 'total_locks' in debug_info
        assert 'active_transactions' in debug_info
        assert 'locked_pages' in debug_info
        assert 'page_locks' in debug_info
        assert 'transaction_pages' in debug_info
        assert 'dependencies' in debug_info
        assert 'waiting_transactions' in debug_info

        assert debug_info['total_locks'] == 3
        assert debug_info['active_transactions'] == 2
        assert debug_info['locked_pages'] == 2

    def test_lock_type_transitions(self):
        """Test various lock type transitions and their validity."""
        # Start with shared lock
        self.lock_manager.acquire_lock(self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.SHARED

        # Upgrade to exclusive (should work if alone)
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result is True
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.EXCLUSIVE

        # Requesting shared on exclusive should return True (already have stronger)
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert result is True
        assert self.lock_manager.get_lock_type(
            self.tid1, self.page1) == LockType.EXCLUSIVE

    def test_edge_case_empty_operations(self):
        """Test edge cases with empty data structures."""
        # Operations on empty lock manager
        assert not self.lock_manager.holds_lock(self.tid1, self.page1)
        assert self.lock_manager.get_lock_type(self.tid1, self.page1) is None
        assert self.lock_manager.get_pages_locked_by_transaction(
            self.tid1) == set()

        # Release operations on empty manager
        self.lock_manager.release_lock(self.tid1, self.page1)
        self.lock_manager.release_all_locks(self.tid1)

        # Debug info on empty manager
        debug_info = self.lock_manager.get_debug_info()
        assert debug_info['total_locks'] == 0
        assert debug_info['active_transactions'] == 0
        assert debug_info['locked_pages'] == 0

    def test_stress_test_many_transactions(self):
        """Stress test with many transactions and locks."""
        num_transactions = 50
        num_pages = 20

        tids = [TransactionId() for _ in range(num_transactions)]
        pages = [HeapPageId(i, 0) for i in range(num_pages)]

        # Each transaction tries to acquire shared locks on random pages
        import random
        random.seed(42)  # For reproducible results

        for tid in tids:
            selected_pages = random.sample(pages, 3)
            for page in selected_pages:
                result = self.lock_manager.acquire_lock(
                    tid, page, LockType.SHARED)
                # Result might be True or False depending on conflicts

        # Check that internal state is consistent
        debug_info = self.lock_manager.get_debug_info()
        assert debug_info['total_locks'] >= 0
        assert debug_info['active_transactions'] >= 0
        assert debug_info['locked_pages'] >= 0

        # Clean up
        for tid in tids:
            self.lock_manager.release_all_locks(tid)

    def test_lock_manager_find_blocking_transactions(self):
        """Test the internal _find_blocking_transactions method."""
        # Set up blocking scenario
        self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)  # Won't succeed

        # Test finding blocking transactions for shared lock request
        blocking = self.lock_manager._find_blocking_transactions(
            self.page1, LockType.SHARED)
        assert self.tid1 in blocking

        # Test finding blocking transactions for exclusive lock request
        blocking = self.lock_manager._find_blocking_transactions(
            self.page1, LockType.EXCLUSIVE)
        assert self.tid1 in blocking

    def test_lock_manager_cleanup_dependencies(self):
        """Test the internal dependency cleanup method."""
        # Create some dependencies
        self.lock_manager._dependency_graph.add_dependency(
            self.tid1, self.tid2)
        self.lock_manager._waiting_for[self.tid1].add(self.tid2)

        # Clean up
        self.lock_manager._cleanup_transaction_dependencies(self.tid1)

        # Should be cleaned up
        assert self.tid1 not in self.lock_manager._dependency_graph.graph
        assert self.tid1 not in self.lock_manager._waiting_for
