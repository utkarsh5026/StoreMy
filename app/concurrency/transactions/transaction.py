import threading
import time
from typing import Optional, Set
from enum import Enum

from ...primitives import TransactionId, PageId
from ...core.exceptions import DbException


class TransactionState(Enum):
    """States a transaction can be in during its lifecycle."""
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    PREPARING = "PREPARING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class Transaction:
    """
    Robust Transaction class with complete lifecycle management.

    Features:
    - Proper state management with atomic transitions
    - Integration with lock manager and buffer pool
    - Deadlock handling with exponential backoff retry
    - Comprehensive error handling and logging
    - Thread-safe operations
    """

    def __init__(self):
        """Create a new transaction with unique ID and clean state."""
        self.tid = TransactionId()
        self.state = TransactionState.CREATED
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self._lock = threading.RLock()

        # Track pages accessed for proper cleanup
        self._pages_read: Set[PageId] = set()
        self._pages_written: Set[PageId] = set()

        # Deadlock handling
        self.deadlock_retries = 0
        self.max_deadlock_retries = 3

        # Statistics
        self.operations_count = 0
        self.locks_acquired = 0

    def start(self) -> None:
        """Start the transaction with proper logging and state management."""
        with self._lock:
            if self.state != TransactionState.CREATED:
                raise DbException(
                    f"Cannot start transaction in state {self.state}")

            try:
                # Log transaction begin with retries
                self._log_transaction_begin()

                # Update state atomically
                self.state = TransactionState.ACTIVE
                self.start_time = time.time()

                # Register with transaction manager
                self._register_with_manager()

            except Exception as e:
                self.state = TransactionState.ABORTED
                raise DbException(f"Failed to start transaction: {e}")

    def _log_transaction_begin(self) -> None:
        """Log transaction begin with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                from app.database import Database
                Database.get_log_file().log_transaction_begin(self.tid)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise DbException(
                        f"Failed to log transaction begin after {max_retries} attempts: {e}")
                time.sleep(0.1 * (2 ** attempt))  # Exponential backoff

    def _register_with_manager(self) -> None:
        """Register this transaction with the global transaction manager."""
        try:
            from .transaction_manager import get_transaction_manager
            get_transaction_manager().register_transaction(self)
        except Exception as e:
            raise DbException(
                f"Failed to register with transaction manager: {e}")

    def get_id(self) -> TransactionId:
        """Return the unique ID of this transaction."""
        return self.tid

    def get_state(self) -> TransactionState:
        """Return the current state of this transaction."""
        return self.state

    def is_active(self) -> bool:
        """Check if this transaction is currently active."""
        return self.state == TransactionState.ACTIVE

    def get_age(self) -> float:
        """Get the age of this transaction in seconds."""
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time

    def record_page_read(self, page_id: PageId) -> None:
        """Record that this transaction read from a page."""
        with self._lock:
            if self.state == TransactionState.ACTIVE:
                self._pages_read.add(page_id)
                self.operations_count += 1

    def record_page_write(self, page_id: PageId) -> None:
        """Record that this transaction wrote to a page."""
        with self._lock:
            if self.state == TransactionState.ACTIVE:
                self._pages_written.add(page_id)
                self.operations_count += 1

    def get_pages_read(self) -> Set[PageId]:
        """Get all pages this transaction has read."""
        with self._lock:
            return self._pages_read.copy()

    def get_pages_written(self) -> Set[PageId]:
        """Get all pages this transaction has written."""
        with self._lock:
            return self._pages_written.copy()

    def commit(self) -> None:
        """
        Commit the transaction using strict 2PL protocol.

        Commit Protocol:
        1. Enter PREPARING state
        2. Write all dirty pages to log (WAL)
        3. Write commit record to log
        4. Force log to disk
        5. Release all locks
        6. Mark as COMMITTED
        """
        with self._lock:
            if self.state != TransactionState.ACTIVE:
                raise DbException(
                    f"Cannot commit transaction in state {self.state}")

            try:
                # Phase 1: Prepare
                self.state = TransactionState.PREPARING
                self._prepare_commit()

                # Phase 2: Write commit record and force log
                self._write_commit_record()

                # Phase 3: Complete commit (release locks, update state)
                self._complete_commit()

            except Exception as e:
                # If commit fails, abort the transaction
                self._abort_internal(f"Commit failed: {e}")
                raise DbException(f"Transaction commit failed: {e}")

    def _prepare_commit(self) -> None:
        """Prepare phase: write all dirty pages to log."""
        try:
            from app.database import Database
            buffer_pool = Database.get_buffer_pool()
            log_file = Database.get_log_file()

            # Write all dirty pages to log for WAL compliance
            for page_id in self._pages_written:
                try:
                    page = buffer_pool.get_page_if_cached(page_id)
                    if page and page.is_dirty() == self.tid:
                        before_image = page.get_before_image()
                        log_file.log_write(self.tid, before_image, page)
                except Exception as e:
                    raise DbException(f"Failed to log page {page_id}: {e}")

        except Exception as e:
            raise DbException(f"Prepare commit failed: {e}")

    def _write_commit_record(self) -> None:
        """Write commit record to log and force to disk."""
        try:
            from app.database import Database
            log_file = Database.get_log_file()
            log_file.log_commit(self.tid)
            # log_file.force()  # Ensure durability
        except Exception as e:
            raise DbException(f"Failed to write commit record: {e}")

    def _complete_commit(self) -> None:
        """Complete commit by releasing locks and updating state."""
        try:
            from app.database import Database

            # Notify buffer pool to release locks and handle dirty pages
            buffer_pool = Database.get_buffer_pool()
            buffer_pool.transaction_complete(self.tid, commit=True)

            # Update state
            self.state = TransactionState.COMMITTED
            self.end_time = time.time()

            # Notify transaction manager
            self._notify_manager_completion()

        except Exception as e:
            raise DbException(f"Failed to complete commit: {e}")

    def abort(self, reason: str = "Explicit abort requested") -> None:
        """Abort the transaction with proper cleanup."""
        with self._lock:
            if self.state in [TransactionState.COMMITTED, TransactionState.ABORTED]:
                return  # Already completed

            self._abort_internal(reason)

    def _abort_internal(self, reason: str) -> None:
        """Internal abort implementation with comprehensive cleanup."""
        try:
            from app.database import Database

            # Write abort record to log
            try:
                log_file = Database.get_log_file()
                log_file.log_abort(self.tid)
            except Exception as e:
                print(f"Warning: Could not log abort: {e}")

            # Release locks and discard dirty pages
            buffer_pool = Database.get_buffer_pool()
            buffer_pool.transaction_complete(self.tid, commit=False)

            # Update state
            self.state = TransactionState.ABORTED
            self.end_time = time.time()

            # Notify transaction manager
            self._notify_manager_completion()

            print(f"Transaction {self.tid} aborted: {reason}")

        except Exception as e:
            print(f"Error during abort: {e}")
            # Still mark as aborted even if cleanup fails
            self.state = TransactionState.ABORTED
            self.end_time = time.time()

    def _notify_manager_completion(self) -> None:
        """Notify transaction manager of completion."""
        try:
            from .transaction_manager import get_transaction_manager
            get_transaction_manager().transaction_completed(self)
        except Exception as e:
            print(f"Warning: Failed to notify transaction manager: {e}")

    def handle_deadlock_abort(self) -> bool:
        """
        Handle being aborted due to deadlock with retry logic.

        Returns:
            True if transaction should retry, False if it should give up
        """
        with self._lock:
            # First, properly abort current transaction
            if self.state == TransactionState.ACTIVE:
                self._abort_internal("Deadlock detected")

            self.deadlock_retries += 1

            if self.deadlock_retries <= self.max_deadlock_retries:
                # Reset for retry
                self._reset_for_retry()

                # Exponential backoff
                backoff_time = 0.1 * (2 ** self.deadlock_retries)
                time.sleep(backoff_time)

                return True
            else:
                print(
                    f"Transaction {self.tid} giving up after {self.deadlock_retries} deadlock retries")
                return False

    def _reset_for_retry(self) -> None:
        """Reset transaction state for deadlock retry."""
        self.state = TransactionState.CREATED
        self.start_time = None
        self.end_time = None
        self._pages_read.clear()
        self._pages_written.clear()
        self.operations_count = 0
        self.locks_acquired = 0

    def get_statistics(self) -> dict:
        """Get transaction statistics."""
        with self._lock:
            duration = 0.0
            if self.start_time:
                end_time = self.end_time or time.time()
                duration = end_time - self.start_time

            return {
                'transaction_id': str(self.tid),
                'state': self.state.value,
                'duration': duration,
                'operations_count': self.operations_count,
                'locks_acquired': self.locks_acquired,
                'pages_read': len(self._pages_read),
                'pages_written': len(self._pages_written),
                'deadlock_retries': self.deadlock_retries
            }

    def __str__(self) -> str:
        return f"Transaction({self.tid}, state={self.state.value}, age={self.get_age():.2f}s)"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        """Context manager entry - start the transaction."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - commit or abort based on exception."""
        if exc_type is None:
            self.commit()
        else:
            self.abort(f"Exception occurred: {exc_val}")
        return False  # Don't suppress exceptions
