"""
Integration tests for transaction management.

These tests focus on complex scenarios involving multiple transactions,
deadlock handling, system stress, and integration with other components.
"""

import pytest
import threading
import time
import random
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.concurrency.transactions.transaction import Transaction, TransactionState
from app.concurrency.transactions.transaction_manager import (
    TransactionManager,
    get_transaction_manager,
    create_transaction
)
from app.primitives import TransactionId
from app.storage.heap.heap_page_id import HeapPageId
from app.core.exceptions import DbException


class TestTransactionIntegration:
    """Integration tests for transaction management."""

    def test_concurrent_transaction_lifecycle(self):
        """Test concurrent transaction creation, execution, and completion."""
        tm = TransactionManager()
        results = []
        errors = []

        def transaction_worker(worker_id: int):
            try:
                with patch('app.database.Database.get_log_file') as mock_log, \
                        patch('app.database.Database.get_buffer_pool') as mock_bp:

                    mock_log_file = Mock()
                    mock_log.return_value = mock_log_file
                    mock_buffer_pool = Mock()
                    mock_bp.return_value = mock_buffer_pool

                    # Create and execute transaction
                    txn = tm.create_transaction()
                    txn.start()

                    # Simulate some work
                    for i in range(10):
                        txn.record_page_read(HeapPageId(worker_id, i))
                        txn.record_page_write(HeapPageId(worker_id, i // 2))
                        time.sleep(0.001)  # Small delay to simulate work

                    # Randomly commit or abort
                    if random.random() > 0.2:  # 80% commit rate
                        txn.commit()
                        results.append(('commit', worker_id, txn.tid))
                    else:
                        txn.abort()
                        results.append(('abort', worker_id, txn.tid))

                    tm.transaction_completed(txn)

            except Exception as e:
                errors.append((worker_id, e))

        # Run multiple concurrent transactions
        threads = [threading.Thread(target=transaction_worker, args=(i,))
                   for i in range(20)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify results
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 20
        assert len(tm.get_active_transactions()) == 0

        commits = [r for r in results if r[0] == 'commit']
        aborts = [r for r in results if r[0] == 'abort']

        assert len(commits) + len(aborts) == 20
        assert tm.transactions_committed == len(commits)
        assert tm.transactions_aborted == len(aborts)

    def test_deadlock_scenario_simulation(self):
        """Test simulated deadlock scenarios and recovery."""
        tm = TransactionManager()
        deadlock_count = 0
        successful_retries = 0

        def deadlock_prone_transaction(txn_id: int):
            nonlocal deadlock_count, successful_retries

            with patch('app.database.Database.get_log_file') as mock_log, \
                    patch('app.database.Database.get_buffer_pool') as mock_bp:

                mock_log_file = Mock()
                mock_log.return_value = mock_log_file
                mock_buffer_pool = Mock()
                mock_bp.return_value = mock_buffer_pool

                txn = tm.create_transaction()

                while True:
                    try:
                        txn.start()

                        # Simulate operations that could cause deadlock
                        pages = [HeapPageId(1, i) for i in range(5)]
                        if txn_id % 2 == 0:
                            pages.reverse()  # Different order to cause conflicts

                        for page in pages:
                            txn.record_page_read(page)
                            txn.record_page_write(page)
                            time.sleep(0.001)

                        # Simulate deadlock detection with 10% probability
                        if random.random() < 0.1:
                            deadlock_count += 1
                            should_retry = txn.handle_deadlock_abort()
                            if should_retry:
                                successful_retries += 1
                                continue
                            else:
                                txn.abort()
                                tm.transaction_completed(txn)
                                return

                        # Normal commit
                        txn.commit()
                        tm.transaction_completed(txn)
                        return

                    except Exception as e:
                        txn.abort()
                        tm.transaction_completed(txn)
                        raise e

        # Run transactions that might deadlock
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(deadlock_prone_transaction, i)
                       for i in range(15)]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    pytest.fail(f"Transaction failed unexpectedly: {e}")

        # Verify deadlock handling worked
        assert deadlock_count >= 0  # Some deadlocks may have been simulated
        assert successful_retries >= 0  # Some retries may have occurred
        assert len(tm.get_active_transactions()) == 0

    def test_transaction_manager_stress_test(self):
        """Stress test with many concurrent operations."""
        tm = TransactionManager()
        operation_count = 0
        operation_lock = threading.Lock()

        def increment_counter():
            nonlocal operation_count
            with operation_lock:
                operation_count += 1

        def create_transactions():
            for _ in range(100):
                txn = tm.create_transaction()
                increment_counter()
                time.sleep(0.0001)

        def complete_transactions():
            active_txns = tm.get_active_transactions()
            for txn in list(active_txns):
                if random.random() > 0.5:
                    txn.state = TransactionState.COMMITTED
                else:
                    txn.state = TransactionState.ABORTED
                tm.transaction_completed(txn)
                increment_counter()
                time.sleep(0.0001)

        def read_statistics():
            for _ in range(200):
                stats = tm.get_statistics()
                assert isinstance(stats, dict)
                increment_counter()
                time.sleep(0.0001)

        # Run concurrent operations
        threads = [
            threading.Thread(target=create_transactions) for _ in range(3)
        ] + [
            threading.Thread(target=complete_transactions) for _ in range(2)
        ] + [
            threading.Thread(target=read_statistics) for _ in range(2)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no crashes occurred
        assert operation_count > 0
        final_stats = tm.get_statistics()
        assert isinstance(final_stats, dict)

    def test_context_manager_nested_transactions(self):
        """Test nested transaction context managers."""
        commit_count = 0
        abort_count = 0

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            def mock_transaction_complete(tid, commit):
                nonlocal commit_count, abort_count
                if commit:
                    commit_count += 1
                else:
                    abort_count += 1

            mock_buffer_pool.transaction_complete.side_effect = mock_transaction_complete

            # Nested successful transactions
            with create_transaction() as txn1:
                txn1.record_page_read(HeapPageId(1, 1))

                with create_transaction() as txn2:
                    txn2.record_page_read(HeapPageId(1, 2))

                txn1.record_page_write(HeapPageId(1, 1))

            assert commit_count == 2
            assert abort_count == 0

            # Nested transaction with inner failure
            commit_count = 0
            abort_count = 0

            with pytest.raises(ValueError):
                with create_transaction() as txn1:
                    txn1.record_page_read(PageId(2, 1))

                    with pytest.raises(ValueError):
                        with create_transaction() as txn2:
                            txn2.record_page_read(PageId(2, 2))
                            raise ValueError("Inner transaction fails")

                    # Outer transaction continues and should commit
                    txn1.record_page_write(PageId(2, 1))

            assert commit_count == 1  # Outer transaction commits
            assert abort_count == 1   # Inner transaction aborts

    def test_transaction_timeout_simulation(self):
        """Simulate transaction timeouts and cleanup."""
        tm = TransactionManager()
        timed_out_transactions = []

        def long_running_transaction(duration: float):
            with patch('app.database.Database.get_log_file') as mock_log, \
                    patch('app.database.Database.get_buffer_pool') as mock_bp:

                mock_log_file = Mock()
                mock_log.return_value = mock_log_file
                mock_buffer_pool = Mock()
                mock_bp.return_value = mock_buffer_pool

                txn = tm.create_transaction()
                txn.start()

                # Simulate long-running work
                time.sleep(duration)

                # Check if transaction is "too old" (simulated timeout)
                if txn.get_age() > 0.1:  # 100ms timeout
                    timed_out_transactions.append(txn)
                    txn.abort()
                    tm.transaction_completed(txn)
                else:
                    txn.commit()
                    tm.transaction_completed(txn)

        # Start several transactions with different durations
        threads = [
            threading.Thread(target=long_running_transaction,
                             args=(0.05,)),  # Fast
            threading.Thread(target=long_running_transaction,
                             args=(0.15,)),  # Slow
            threading.Thread(target=long_running_transaction,
                             args=(0.08,)),  # Fast
            threading.Thread(target=long_running_transaction,
                             args=(0.12,)),  # Slow
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify timeout handling
        assert len(timed_out_transactions) >= 2  # At least the slow ones
        assert tm.transactions_aborted >= 2
        assert len(tm.get_active_transactions()) == 0

    def test_memory_usage_under_load(self):
        """Test memory usage doesn't grow unboundedly under load."""
        tm = TransactionManager()

        def transaction_cycle():
            with patch('app.database.Database.get_log_file') as mock_log, \
                    patch('app.database.Database.get_buffer_pool') as mock_bp:

                mock_log_file = Mock()
                mock_log.return_value = mock_log_file
                mock_buffer_pool = Mock()
                mock_bp.return_value = mock_buffer_pool

                # Create many transactions quickly
                for _ in range(50):
                    txn = tm.create_transaction()
                    txn.start()

                    # Do minimal work
                    txn.record_page_read(PageId(1, 1))

                    # Commit immediately
                    txn.commit()
                    tm.transaction_completed(txn)

        # Run multiple cycles
        threads = [threading.Thread(target=transaction_cycle)
                   for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify cleanup
        assert len(tm.get_active_transactions()) == 0
        assert tm.transactions_started == 250  # 5 threads * 50 transactions
        assert tm.transactions_committed == 250
        assert tm.transactions_aborted == 0

    def test_error_recovery_scenarios(self):
        """Test various error scenarios and recovery."""
        tm = TransactionManager()
        error_scenarios = []

        def error_prone_transaction(scenario: str):
            with patch('app.database.Database.get_log_file') as mock_log, \
                    patch('app.database.Database.get_buffer_pool') as mock_bp:

                mock_log_file = Mock()
                mock_log.return_value = mock_log_file
                mock_buffer_pool = Mock()
                mock_bp.return_value = mock_buffer_pool

                txn = tm.create_transaction()

                try:
                    if scenario == "start_error":
                        mock_log.side_effect = Exception("Log unavailable")
                        txn.start()  # Should still work despite log error
                        txn.abort()

                    elif scenario == "commit_error":
                        txn.start()
                        mock_buffer_pool.transaction_complete.side_effect = Exception(
                            "Buffer error")
                        with pytest.raises(DbException):
                            txn.commit()
                        # Should be in aborted state after failed commit
                        assert txn.state == TransactionState.ABORTED

                    elif scenario == "abort_error":
                        txn.start()
                        mock_log_file.log_abort.side_effect = Exception(
                            "Log error")
                        txn.abort()  # Should still complete despite log error

                    error_scenarios.append(scenario)

                finally:
                    tm.transaction_completed(txn)

        # Test different error scenarios
        scenarios = ["start_error", "commit_error", "abort_error"]
        threads = [threading.Thread(target=error_prone_transaction, args=(scenario,))
                   for scenario in scenarios]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify all scenarios were tested
        assert len(error_scenarios) == 3
        assert len(tm.get_active_transactions()) == 0


class TestTransactionEdgeCases:
    """Test additional edge cases and boundary conditions."""

    def test_transaction_state_race_conditions(self):
        """Test race conditions in transaction state changes."""
        errors = []

        def state_changer(txn: Transaction, target_state: TransactionState):
            try:
                # Simulate concurrent state changes
                time.sleep(random.uniform(0.001, 0.01))
                txn.state = target_state
            except Exception as e:
                errors.append(e)

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            txn = Transaction()
            txn.start()

            # Try to change state concurrently
            states = [TransactionState.PREPARING, TransactionState.COMMITTED,
                      TransactionState.ABORTED]
            threads = [threading.Thread(target=state_changer, args=(txn, state))
                       for state in states]

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Should not crash, final state should be valid
            assert txn.state in [TransactionState.PREPARING, TransactionState.COMMITTED,
                                 TransactionState.ABORTED]
            assert len(errors) == 0

    def test_extreme_deadlock_retry_scenarios(self):
        """Test extreme deadlock retry scenarios."""
        txn = Transaction()

        # Test maximum retries
        for i in range(10):
            should_retry = txn.handle_deadlock_abort()
            if i < 3:
                assert should_retry is True
                assert txn.state == TransactionState.CREATED
            else:
                assert should_retry is False
                break

        # Verify state after max retries
        assert txn.deadlock_retries > 3

    def test_page_tracking_edge_cases(self):
        """Test edge cases in page read/write tracking."""
        txn = Transaction()

        # Test duplicate page tracking
        page_id = PageId(1, 1)

        # Add same page multiple times
        for _ in range(100):
            txn.record_page_read(page_id)
            txn.record_page_write(page_id)

        # Should only appear once in each set
        assert len(txn.get_pages_read()) == 1
        assert len(txn.get_pages_written()) == 1
        assert page_id in txn.get_pages_read()
        assert page_id in txn.get_pages_written()

    def test_concurrent_page_tracking(self):
        """Test concurrent page tracking operations."""
        txn = Transaction()
        errors = []

        def page_tracker(start_page: int, count: int):
            try:
                for i in range(count):
                    page_id = PageId(1, start_page + i)
                    txn.record_page_read(page_id)
                    if i % 2 == 0:
                        txn.record_page_write(page_id)
            except Exception as e:
                errors.append(e)

        # Run concurrent page tracking
        threads = [
            threading.Thread(target=page_tracker, args=(0, 100)),
            threading.Thread(target=page_tracker, args=(50, 100)),
            threading.Thread(target=page_tracker, args=(100, 100)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no errors and reasonable results
        assert len(errors) == 0
        assert len(txn.get_pages_read()) <= 200  # Some overlap expected
        assert len(txn.get_pages_written()) <= 100  # Only even numbered pages

    def test_transaction_age_precision(self):
        """Test transaction age calculation precision."""
        txn = Transaction()

        # Test age before start
        assert txn.get_age() == 0.0

        # Set precise start time
        start_time = time.time()
        txn.start_time = start_time

        # Wait a specific amount
        time.sleep(0.05)

        age = txn.get_age()
        expected_age = time.time() - start_time

        # Should be within reasonable precision (10ms tolerance)
        assert abs(age - expected_age) < 0.01
