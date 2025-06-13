"""
Tests for the Transaction class.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock

from app.concurrency.transactions.transaction import Transaction, TransactionState
from app.primitives import TransactionId
from app.storage.heap.heap_page_id import HeapPageId
from app.core.exceptions import DbException


class TestTransaction:
    """Test cases for Transaction class."""

    def test_transaction_initialization(self):
        """Test that a transaction is properly initialized."""
        txn = Transaction()

        assert isinstance(txn.tid, TransactionId)
        assert txn.state == TransactionState.CREATED
        assert txn.start_time is None
        assert txn.end_time is None
        assert txn.deadlock_retries == 0
        assert txn.max_deadlock_retries == 3
        assert len(txn._pages_read) == 0
        assert len(txn._pages_written) == 0

    def test_transaction_start_success(self):
        """Test successful transaction start."""
        txn = Transaction()

        with patch('app.database.Database.get_log_file') as mock_log:
            mock_log_file = Mock()
            mock_log.return_value = mock_log_file

            txn.start()

            assert txn.state == TransactionState.ACTIVE
            assert txn.start_time is not None
            assert txn.is_active()
            mock_log_file.log_transaction_begin.assert_called_once_with(
                txn.tid)

    def test_transaction_start_invalid_state(self):
        """Test that starting an already started transaction raises exception."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE

        with pytest.raises(DbException, match="Cannot start transaction in state"):
            txn.start()

    def test_transaction_start_log_failure(self):
        """Test transaction start when logging fails."""
        txn = Transaction()

        with patch('app.database.Database.get_log_file') as mock_log:
            mock_log.side_effect = Exception("Log error")

            # Should still start successfully despite log error
            txn.start()
            assert txn.state == TransactionState.ACTIVE

    def test_get_id(self):
        """Test getting transaction ID."""
        txn = Transaction()
        assert txn.get_id() == txn.tid

    def test_get_state(self):
        """Test getting transaction state."""
        txn = Transaction()
        assert txn.get_state() == TransactionState.CREATED

        txn.state = TransactionState.ACTIVE
        assert txn.get_state() == TransactionState.ACTIVE

    def test_is_active(self):
        """Test checking if transaction is active."""
        txn = Transaction()
        assert not txn.is_active()

        txn.state = TransactionState.ACTIVE
        assert txn.is_active()

        txn.state = TransactionState.COMMITTED
        assert not txn.is_active()

    def test_get_age(self):
        """Test getting transaction age."""
        txn = Transaction()

        # Age should be 0 before starting
        assert txn.get_age() == pytest.approx(0.0)

        # Start transaction and check age
        with patch('app.database.Database.get_log_file'):
            txn.start()
            time.sleep(0.1)
            age = txn.get_age()
            assert age >= 0.1
            assert age < 1.0  # Should be reasonable

    def test_record_page_operations(self):
        """Test recording page read and write operations."""
        txn = Transaction()
        page_id1 = HeapPageId(1, 1)
        page_id2 = HeapPageId(1, 2)

        # Test recording reads
        txn.record_page_read(page_id1)
        txn.record_page_read(page_id2)
        assert page_id1 in txn.get_pages_read()
        assert page_id2 in txn.get_pages_read()
        assert len(txn.get_pages_read()) == 2

        # Test recording writes
        txn.record_page_write(page_id1)
        assert page_id1 in txn.get_pages_written()
        assert len(txn.get_pages_written()) == 1

    def test_commit_success(self):
        """Test successful transaction commit."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            txn.commit()

            assert txn.state == TransactionState.COMMITTED
            assert txn.end_time is not None
            mock_log_file.log_commit.assert_called_once_with(txn.tid)
            mock_log_file.force.assert_called_once()
            mock_buffer_pool.transaction_complete.assert_called_once_with(
                txn.tid, commit=True)

    def test_commit_invalid_state(self):
        """Test commit with invalid state."""
        txn = Transaction()

        with pytest.raises(DbException, match="Cannot commit transaction in state"):
            txn.commit()

    def test_commit_failure(self):
        """Test commit failure handling."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            # Make commit preparation fail
            mock_buffer_pool.get_page_if_cached.side_effect = Exception(
                "Buffer error")

            with pytest.raises(DbException, match="Transaction commit failed"):
                txn.commit()

            assert txn.state == TransactionState.ABORTED

    def test_abort_success(self):
        """Test successful transaction abort."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            txn.abort()

            assert txn.state == TransactionState.ABORTED
            assert txn.end_time is not None
            mock_log_file.log_abort.assert_called_once_with(txn.tid)
            mock_buffer_pool.transaction_complete.assert_called_once_with(
                txn.tid, commit=False)

    def test_abort_already_completed(self):
        """Test aborting already completed transactions."""
        txn = Transaction()

        # Test aborting committed transaction
        txn.state = TransactionState.COMMITTED
        txn.abort()  # Should not raise exception
        assert txn.state == TransactionState.COMMITTED

        # Test aborting already aborted transaction
        txn.state = TransactionState.ABORTED
        txn.abort()  # Should not raise exception
        assert txn.state == TransactionState.ABORTED

    def test_abort_with_errors(self):
        """Test abort with various error conditions."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log.side_effect = Exception("Log error")
            mock_bp.side_effect = Exception("Buffer error")

            # Should still complete abort despite errors
            txn.abort()
            assert txn.state == TransactionState.ABORTED

    def test_prepare_commit_with_dirty_pages(self):
        """Test preparing commit with dirty pages."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE
        page_id = HeapPageId(1, 1)
        txn.record_page_write(page_id)

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            # Mock a dirty page
            mock_page = Mock()
            mock_page.is_dirty.return_value = txn.tid
            mock_page.get_before_image.return_value = b"before"
            mock_buffer_pool.get_page_if_cached.return_value = mock_page

            txn._prepare_commit()

            mock_log_file.log_write.assert_called_once_with(
                txn.tid, b"before", mock_page)

    def test_handle_deadlock_abort_retry_success(self):
        """Test successful deadlock retry."""
        txn = Transaction()
        txn.state = TransactionState.ACTIVE
        txn.start_time = time.time()
        txn.record_page_read(HeapPageId(1, 1))

        should_retry = txn.handle_deadlock_abort()

        assert should_retry is True
        assert txn.deadlock_retries == 1
        assert txn.state == TransactionState.CREATED
        assert txn.start_time is None
        assert len(txn._pages_read) == 0

    def test_handle_deadlock_abort_max_retries(self):
        """Test deadlock handling when max retries reached."""
        txn = Transaction()
        txn.deadlock_retries = 3  # At max retries

        should_retry = txn.handle_deadlock_abort()

        assert should_retry is False
        assert txn.deadlock_retries == 4

    def test_context_manager_success(self):
        """Test transaction as context manager with successful completion."""
        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            with Transaction() as txn:
                assert txn.state == TransactionState.ACTIVE
                # Do some work
                txn.record_page_read(HeapPageId(1, 1))

            assert txn.state == TransactionState.COMMITTED

    def test_context_manager_exception(self):
        """Test transaction as context manager with exception."""
        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            with pytest.raises(ValueError):
                with Transaction() as txn:
                    assert txn.state == TransactionState.ACTIVE
                    raise ValueError("Test exception")

            assert txn.state == TransactionState.ABORTED

    def test_string_representation(self):
        """Test string representation of transaction."""
        txn = Transaction()

        str_repr = str(txn)
        assert "Transaction(" in str_repr
        assert str(txn.tid) in str_repr
        assert "CREATED" in str_repr

        repr_str = repr(txn)
        assert repr_str == str_repr

    def test_thread_safety(self):
        """Test thread safety of transaction operations."""
        txn = Transaction()
        errors = []

        def worker():
            try:
                for i in range(100):
                    txn.record_page_read(HeapPageId(1, i))
                    txn.record_page_write(HeapPageId(2, i))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(txn.get_pages_read()) == 500  # 5 threads * 100 pages
        assert len(txn.get_pages_written()) == 500

    def test_edge_case_state_transitions(self):
        """Test edge cases in state transitions."""
        txn = Transaction()

        # Test all invalid start states
        for state in [TransactionState.ACTIVE, TransactionState.PREPARING,
                      TransactionState.COMMITTED, TransactionState.ABORTED]:
            txn.state = state
            with pytest.raises(DbException):
                txn.start()

        # Test invalid commit states
        for state in [TransactionState.CREATED, TransactionState.PREPARING,
                      TransactionState.COMMITTED, TransactionState.ABORTED]:
            txn.state = state
            if state != TransactionState.ACTIVE:
                with pytest.raises(DbException):
                    txn.commit()

    def test_age_calculation_edge_cases(self):
        """Test edge cases in age calculation."""
        txn = Transaction()

        # Age should be 0 when start_time is None
        assert txn.get_age() == 0.0

        # Test with very small time differences
        txn.start_time = time.time() - 0.001
        age = txn.get_age()
        assert age >= 0.001
        assert age < 0.1

    def test_concurrent_state_changes(self):
        """Test concurrent state changes."""
        txn = Transaction()
        errors = []

        with patch('app.database.Database.get_log_file') as mock_log, \
                patch('app.database.Database.get_buffer_pool') as mock_bp:

            mock_log_file = Mock()
            mock_log.return_value = mock_log_file
            mock_buffer_pool = Mock()
            mock_bp.return_value = mock_buffer_pool

            def start_worker():
                try:
                    txn.start()
                except Exception as e:
                    errors.append(e)

            def abort_worker():
                try:
                    time.sleep(0.01)  # Let start complete first
                    txn.abort()
                except Exception as e:
                    errors.append(e)

            # Try concurrent start and abort
            t1 = threading.Thread(target=start_worker)
            t2 = threading.Thread(target=abort_worker)

            t1.start()
            t2.start()
            t1.join()
            t2.join()

            # Should have at most one error (trying to start already active)
            assert len(errors) <= 1
            assert txn.state in [
                TransactionState.ACTIVE, TransactionState.ABORTED]
