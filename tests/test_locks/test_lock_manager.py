"""
Comprehensive tests for the LockManager implementation covering all edge cases.

This test suite provides exhaustive coverage of:
- Basic lock operations (acquire/release)
- Lock compatibility and conflicts
- Lock upgrades and downgrades
- Deadlock detection and resolution
- Timeout handling and race conditions
- Concurrent operations and thread safety
- Statistics and monitoring
- Error conditions and edge cases
"""

import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.concurrency.locks.lock_manager import LockManager, LockRequest
from app.concurrency.locks.dependency_manager import LockType
from app.primitives import TransactionId
from app.core.exceptions import TransactionAbortedException
from app.storage.heap import HeapPageId


class TestLockManagerComprehensive:
    """Comprehensive test suite for LockManager covering all edge cases."""

    def setup_method(self):
        """Set up test fixtures with multiple transactions and pages."""
        self.lock_manager = LockManager()

        # Create test transactions
        self.tid1 = TransactionId()
        self.tid2 = TransactionId()
        self.tid3 = TransactionId()
        self.tid4 = TransactionId()
        self.tid5 = TransactionId()

        # Create test pages
        self.page1 = HeapPageId(1, 1)
        self.page2 = HeapPageId(1, 2)
        self.page3 = HeapPageId(1, 3)
        self.page4 = HeapPageId(1, 4)
        self.page5 = HeapPageId(1, 5)

    # =================== BASIC LOCK OPERATIONS ===================

    def test_acquire_lock_shared_on_empty_page(self):
        """Test acquiring shared lock on empty page."""
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)

        assert result is True
        assert len(self.lock_manager._page_locks[self.page1]) == 1
        assert self.page1 in self.lock_manager._transaction_locks[self.tid1]
        assert self.lock_manager.locks_granted == 1

    def test_acquire_lock_exclusive_on_empty_page(self):
        """Test acquiring exclusive lock on empty page."""
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        assert result is True
        assert len(self.lock_manager._page_locks[self.page1]) == 1
        locks = self.lock_manager.get_locks_held(self.tid1)
        assert (self.page1, LockType.EXCLUSIVE) in locks

    def test_multiple_shared_locks_same_page(self):
        """Test multiple transactions acquiring shared locks on the same page."""
        # Multiple shared locks should succeed
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.SHARED)

        assert len(self.lock_manager._page_locks[self.page1]) == 3
        assert self.lock_manager.locks_granted == 3

    def test_exclusive_lock_blocks_all_others(self):
        """Test that exclusive lock blocks all other lock requests."""
        # Get exclusive lock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        # All other requests should be blocked
        assert not self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert not self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.EXCLUSIVE)

        assert len(self.lock_manager._page_locks[self.page1]) == 1
        assert self.lock_manager.locks_granted == 1

    def test_shared_lock_blocks_exclusive(self):
        """Test that shared locks block exclusive lock requests."""
        # Get shared locks
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)

        # Exclusive request should be blocked
        assert not self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.EXCLUSIVE)

        assert len(self.lock_manager._page_locks[self.page1]) == 2

    # =================== LOCK COMPATIBILITY ===================

    def test_same_transaction_duplicate_requests(self):
        """Test duplicate lock requests from the same transaction."""
        # Same transaction requesting same lock multiple times
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)


        assert len(self.lock_manager._page_locks[self.page1]) == 1

    def test_same_transaction_upgrade_attempt(self):
        """Test lock upgrade from same transaction."""
        # Start with shared lock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)

        # Upgrade to exclusive (should succeed when no other shared locks)
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        # Should now have exclusive lock
        locks = self.lock_manager.get_locks_held(self.tid1)
        assert (self.page1, LockType.EXCLUSIVE) in locks

    def test_lock_upgrade_blocked_by_other_shared_locks(self):
        """Test lock upgrade blocked by other shared locks."""
        # Multiple shared locks
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)

        # Upgrade attempt should fail due to other shared locks
        assert not self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

    # =================== LOCK RELEASE ===================

    def test_release_single_lock(self):
        """Test releasing a single lock."""
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)

        self.lock_manager.release_lock(self.tid1, self.page1)

        assert len(self.lock_manager._page_locks.get(self.page1, [])) == 0
        assert self.page1 not in self.lock_manager._transaction_locks.get(
            self.tid1, set())

    def test_release_one_of_multiple_shared_locks(self):
        """Test releasing one lock when multiple shared locks exist."""
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)

        self.lock_manager.release_lock(self.tid1, self.page1)

        assert len(self.lock_manager._page_locks[self.page1]) == 1
        # tid2 should still hold the lock
        locks = self.lock_manager.get_locks_held(self.tid2)
        assert (self.page1, LockType.SHARED) in locks

    def test_release_all_locks_for_transaction(self):
        """Test releasing all locks held by a transaction."""
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page3, LockType.SHARED)

        self.lock_manager.release_all_locks(self.tid1)

        # All locks should be released
        assert len(self.lock_manager.get_locks_held(self.tid1)) == 0
        assert self.tid1 not in self.lock_manager._transaction_locks

    def test_release_nonexistent_lock(self):
        """Test releasing a lock that doesn't exist."""
        # Should not raise an error
        self.lock_manager.release_lock(self.tid1, self.page1)

        # Should be no-op
        assert len(self.lock_manager._page_locks.get(self.page1, [])) == 0

    # =================== LOCK WAITING AND TIMEOUTS ===================

    def test_lock_timeout_behavior(self):
        """Test lock request timeout behavior."""
        # Get exclusive lock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        # Request with short timeout should fail
        start_time = time.time()
        result = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED, timeout=0.1)
        end_time = time.time()

        assert result is False
        assert end_time - start_time >= 0.1  # Should have waited
        assert self.lock_manager.lock_timeouts >= 1

    def test_lock_granted_after_release(self):
        """Test lock granted after blocking lock is released."""
        def release_after_delay():
            time.sleep(0.2)
            self.lock_manager.release_lock(self.tid1, self.page1)

        # Get exclusive lock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        release_thread = threading.Thread(target=release_after_delay)
        release_thread.start()

        start_time = time.time()
        result = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED, timeout=1.0)
        end_time = time.time()

        release_thread.join()

        assert result is True
        assert 0.2 <= end_time - start_time <= 0.5  # Should have waited for release

    # =================== DEADLOCK DETECTION ===================

    def test_simple_deadlock_detection(self):
        """Test detection of simple two-transaction deadlock."""
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)

        assert not self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        # T2 tries to get page1 (creates deadlock)
        with pytest.raises(TransactionAbortedException):
            self.lock_manager.acquire_lock(
                self.tid2, self.page1, LockType.EXCLUSIVE)

        assert self.lock_manager.deadlocks_detected >= 1

    def test_complex_deadlock_cycle(self):
        """Test detection of multi-transaction deadlock cycle."""
        # Create circular dependency: T1->T2->T3->T1
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid3, self.page3, LockType.EXCLUSIVE)

        # Create dependencies
        assert not self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)  # T1->T2
        assert not self.lock_manager.acquire_lock(
            self.tid2, self.page3, LockType.EXCLUSIVE)  # T2->T3

        # Complete the cycle
        with pytest.raises(TransactionAbortedException):
            self.lock_manager.acquire_lock(
                self.tid3, self.page1, LockType.EXCLUSIVE)  # T3->T1

    def test_deadlock_victim_selection(self):
        """Test that deadlock victim is chosen correctly (the youngest transaction)."""
        # Ensure tid2 has higher ID (younger)
        if self.tid1.get_id() > self.tid2.get_id():
            self.tid1, self.tid2 = self.tid2, self.tid1

        # Create deadlock scenario
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)

        assert not self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        # tid2 (younger) should be chosen as victim
        with pytest.raises(TransactionAbortedException):
            self.lock_manager.acquire_lock(
                self.tid2, self.page1, LockType.EXCLUSIVE)

    def test_no_false_deadlock_detection(self):
        """Test that deadlock is not detected when none exists."""
        # Create a chain without a cycle: T1->T2->T3
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid3, self.page3, LockType.EXCLUSIVE)

        # Create dependencies but no cycle
        assert not self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.EXCLUSIVE)  # T2->T1
        assert not self.lock_manager.acquire_lock(
            self.tid3, self.page2, LockType.EXCLUSIVE)  # T3->T2

        # No deadlock should be detected
        assert self.lock_manager.deadlocks_detected == 0

    # =================== CONCURRENCY AND THREAD SAFETY ===================

    def test_concurrent_shared_lock_acquisition(self):
        """Test concurrent acquisition of shared locks."""
        results = []

        def acquire_shared_lock(_tid: TransactionId):
            result = self.lock_manager.acquire_lock(
                _tid, self.page1, LockType.SHARED)
            results.append((_tid, result))


        threads = []
        tids = [TransactionId() for _ in range(10)]

        for tid in tids:
            thread = threading.Thread(target=acquire_shared_lock, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        successful = [r for r in results if r[1] is True]
        assert len(successful) == 10

    def test_concurrent_exclusive_lock_acquisition(self):
        """Test concurrent acquisition of exclusive locks (only one should succeed)."""
        results = []

        def acquire_exclusive_lock(_tid: TransactionId):
            result = self.lock_manager.acquire_lock(
                _tid, self.page1, LockType.EXCLUSIVE, 0.1)
            results.append((_tid, result))


        threads = []
        tids = [TransactionId() for _ in range(5)]

        for tid in tids:
            thread = threading.Thread(
                target=acquire_exclusive_lock, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        successful = [r for r in results if r[1] is True]
        failed = [r for r in results if r[1] is False]

        assert len(successful) == 1
        assert len(failed) == 4

    def test_concurrent_lock_upgrade(self):
        """Test concurrent lock upgrades."""
        tids = [TransactionId() for _ in range(3)]
        for tid in tids:
            assert self.lock_manager.acquire_lock(
                tid, self.page1, LockType.SHARED, 0.5)

        results = []

        def try_upgrade(_tid: TransactionId):
            result = self.lock_manager.acquire_lock(
                _tid, self.page1, LockType.EXCLUSIVE)
            results.append((_tid, result))

        threads = []
        for tid in tids:
            thread = threading.Thread(target=try_upgrade, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        successful = [r for r in results if r[1] is True]
        assert len(successful) == 0

    def test_concurrent_release_operations(self):
        """Test concurrent lock releases."""
        # Set up locks for multiple transactions
        tids = [TransactionId() for _ in range(10)]
        pages = [HeapPageId(1, i) for i in range(10)]

        for tid, page in zip(tids, pages):
            assert self.lock_manager.acquire_lock(
                tid, page, LockType.EXCLUSIVE)

        def release_all_locks(_tid: TransactionId):
            self.lock_manager.release_all_locks(_tid)

        threads = []
        for tid in tids:
            thread = threading.Thread(target=release_all_locks, args=(tid,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All locks should be released
        for tid in tids:
            assert len(self.lock_manager.get_locks_held(tid)) == 0

    # =================== STATISTICS AND MONITORING ===================

    def test_statistics_tracking(self):
        """Test that statistics are tracked correctly."""
        initial_stats = self.lock_manager.get_statistics()

        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert not self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)  # upgrade

        final_stats = self.lock_manager.get_statistics()

        assert final_stats['locks_granted'] > initial_stats['locks_granted']
        assert final_stats['lock_upgrades'] > initial_stats['lock_upgrades']
        assert final_stats['active_locks'] >= 1
        assert final_stats['active_transactions'] >= 1

    def test_lock_info_debugging(self):
        """Test detailed lock information for debugging."""
        # Set up some locks
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)
        assert not self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.EXCLUSIVE)  # pending

        lock_info = self.lock_manager.get_lock_info()

        assert 'active_locks' in lock_info
        assert 'pending_requests' in lock_info
        assert 'dependency_graph' in lock_info

        # Check active locks
        page1_str = str(self.page1)
        assert page1_str in lock_info['active_locks']
        assert len(lock_info['active_locks'][page1_str]) == 2

        # Check pending requests
        if page1_str in lock_info['pending_requests']:
            assert len(lock_info['pending_requests'][page1_str]) >= 1

    # =================== EDGE CASES AND ERROR CONDITIONS ===================

    def test_lock_request_abort_handling(self):
        """Test handling of aborted lock requests."""
        # Get exclusive lock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)

        # Create pending request
        request = LockRequest(self.tid2, self.page1, LockType.SHARED)
        self.lock_manager._pending_requests[self.page1].append(request)

        # Abort the transaction
        self.lock_manager.release_all_locks(self.tid2)

        # Request should be marked as aborted
        assert request.aborted or request not in self.lock_manager._pending_requests[
            self.page1]

    def test_cleanup_empty_data_structures(self):
        """Test cleanup of empty data structures."""
        # Create and release locks
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        self.lock_manager.release_all_locks(self.tid1)

        # Data structures should be cleaned up
        assert self.page1 not in self.lock_manager._page_locks
        assert self.tid1 not in self.lock_manager._transaction_locks

    def test_dependency_graph_cleanup(self):
        """Test dependency graph cleanup after deadlock resolution."""
        # Create potential deadlock
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page2, LockType.EXCLUSIVE)

        assert not self.lock_manager.acquire_lock(
            self.tid1, self.page2, LockType.EXCLUSIVE)

        try:
            self.lock_manager.acquire_lock(
                self.tid2, self.page1, LockType.EXCLUSIVE)
        except TransactionAbortedException:
            pass

        # Release remaining locks
        self.lock_manager.release_all_locks(self.tid1)

        # Dependency graph should be cleaned up
        cycles = self.lock_manager.detect_all_deadlocks()
        assert len(cycles) == 0

    def test_lock_timeout_edge_cases(self):
        """Test edge cases with lock timeouts."""
        # Test zero timeout
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        result = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED, timeout=0)
        assert result is False

        # Test very small timeout
        result = self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED, timeout=0.001)
        assert result is False

    def test_lock_upgrade_with_pending_requests(self):
        """Test lock upgrade when there are pending requests."""
        # Get shared locks
        assert self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.SHARED)
        assert self.lock_manager.acquire_lock(
            self.tid2, self.page1, LockType.SHARED)

        # Create pending exclusive request
        assert not self.lock_manager.acquire_lock(
            self.tid3, self.page1, LockType.EXCLUSIVE)

        # Try to upgrade one of the shared locks
        result = self.lock_manager.acquire_lock(
            self.tid1, self.page1, LockType.EXCLUSIVE)
        assert result is False  # Should fail due to pending request and other shared lock

    # =================== STRESS TESTS ===================

    def test_high_concurrency_stress(self):
        """Stress test with high concurrency."""
        num_transactions = 50
        num_pages = 10

        def random_operations(tid):
            pages = [HeapPageId(1, i) for i in range(num_pages)]
            for _ in range(10):
                page = pages[tid.get_id() % num_pages]
                lock_type = LockType.SHARED if tid.get_id() % 2 == 0 else LockType.EXCLUSIVE

                try:
                    result = self.lock_manager.acquire_lock(
                        tid, page, lock_type, timeout=0.1)
                    if result:
                        time.sleep(0.001)  # Hold lock briefly
                        self.lock_manager.release_lock(tid, page)
                except TransactionAbortedException:
                    pass  # Expected in high contention

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=20) as executor:
            tids = [TransactionId() for _ in range(num_transactions)]
            futures = [executor.submit(random_operations, tid) for tid in tids]

            for future in as_completed(futures):
                future.result()  # Wait for completion

        # Clean up any remaining locks
        for tid in [TransactionId() for _ in range(num_transactions)]:
            self.lock_manager.release_all_locks(tid)

        # System should be in clean state
        stats = self.lock_manager.get_statistics()
        assert stats['active_locks'] == 0
        assert stats['pending_requests'] == 0

    def test_memory_cleanup_stress(self):
        """Test memory cleanup under stress."""
        # Create and destroy many transactions
        for i in range(100):
            tid = TransactionId()
            page = HeapPageId(1, i % 10)

            if self.lock_manager.acquire_lock(tid, page, LockType.SHARED):
                self.lock_manager.release_all_locks(tid)

        # Memory usage should be minimal
        stats = self.lock_manager.get_statistics()
        assert stats['active_locks'] == 0
        assert stats['active_transactions'] == 0
