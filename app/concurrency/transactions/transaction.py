import threading
import time
from typing import Optional
from enum import Enum

from ...primitives import TransactionId, PageId
from ...core.exceptions import DbException


class TransactionState(Enum):
    """States a transaction can be in during its lifecycle."""
    CREATED = "CREATED"  # Transaction created but not started
    ACTIVE = "ACTIVE"  # Transaction is running
    PREPARING = "PREPARING"  # Transaction is preparing to commit
    COMMITTED = "COMMITTED"  # Transaction has committed
    ABORTED = "ABORTED"  # Transaction has been aborted


class Transaction:
    """
    Enhanced Transaction class that manages the complete lifecycle of a database transaction.

    This class implements the transaction abstraction with proper integration into
    the concurrency control system. Key responsibilities:

    1. Transaction lifecycle management (begin, commit, abort)
    2. Integration with lock manager and buffer pool
    3. Write-ahead logging coordination
    4. Deadlock handling and retry logic
    5. Resource cleanup on completion

    Transaction Lifecycle:
    CREATED -> ACTIVE -> PREPARING -> COMMITTED/ABORTED

    Transactions follow Strict 2 PL:
    - Locks are acquired during execution
    - All locks are held until transaction completion
    - Locks are released atomically at commit/abort
    """

    def __init__(self):
        """Create a new transaction with a unique ID."""
        self.tid = TransactionId()
        self.state = TransactionState.CREATED
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self._lock = threading.RLock()

        self._pages_read: set[PageId] = set()
        self._pages_written: set[PageId] = set()

        self.deadlock_retries = 0
        self.max_deadlock_retries = 3

    def start(self) -> None:
        """
        Start the transaction.

        This marks the transaction as active and logs the beginning record.
        Must be called before any database operations.
        """
        with self._lock:
            if self.state != TransactionState.CREATED:
                raise DbException(f"Cannot start transaction in state {self.state}")

            self.state = TransactionState.ACTIVE
            self.start_time = time.time()

            try:
                from app.database import Database
                Database.get_log_file().log_transaction_begin(self.tid)
            except Exception as e:
                print(f"Warning: Could not log transaction begin: {e}")

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
        """
        Get the age of this transaction in seconds.
        Used for deadlock victim selection.
        """
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time

    def record_page_read(self, page_id: PageId) -> None:
        """Record that this transaction read from a page."""
        with self._lock:
            self._pages_read.add(page_id)

    def record_page_write(self, page_id: PageId) -> None:
        """Record that this transaction wrote to a page."""
        with self._lock:
            self._pages_written.add(page_id)

    def get_pages_read(self) -> set[PageId]:
        """Get all the pages this transaction has read."""
        with self._lock:
            return self._pages_read.copy()

    def get_pages_written(self) -> set[PageId]:
        """Get all the pages this transaction has written."""
        with self._lock:
            return self._pages_written.copy()

    def commit(self) -> None:
        """
        Commit the transaction.

        This implements the commit protocol:
        1. Prepare phase: flush dirty pages to log
        2. Commit phase: write commit record and flush log
        3. Release all locks
        4. Mark transaction as committed
        """
        with self._lock:
            if self.state != TransactionState.ACTIVE:
                raise DbException(f"Cannot commit transaction in state {self.state}")

            try:
                self.state = TransactionState.PREPARING

                # Prepare phase: write all changes to log
                self._prepare_commit()

                # Commit phase: write commit record
                self._write_commit_record()

                # Release locks and mark committed
                self._complete_commit()

            except Exception as e:
                # If commit fails, abort the transaction
                self._abort_internal(f"Commit failed: {e}")
                raise DbException(f"Transaction commit failed: {e}")

    def abort(self) -> None:
        """
        Abort the transaction.

        This undoes all changes made by the transaction and releases locks.
        """
        with self._lock:
            if self.state in [TransactionState.COMMITTED, TransactionState.ABORTED]:
                return  # Already completed

            self._abort_internal("Explicit abort requested")

    def _prepare_commit(self) -> None:
        """
        Prepare phase of commit: write all dirty pages to log.

        This ensures durability - all changes are written to the log
        before we commit.
        """
        from app.database import Database

        buffer_pool = Database.get_buffer_pool()
        log_file = Database.get_log_file()

        # Flush all pages written by this transaction
        for page_id in self._pages_written:
            try:
                page = buffer_pool.get_page_if_cached(page_id)
                if page and page.is_dirty() == self.tid:
                    before_image = page.get_before_image()
                    log_file.log_write(self.tid, before_image, page)
            except Exception as e:
                print(f"Warning: Could not log page {page_id}: {e}")

    def _write_commit_record(self) -> None:
        """Write the commit record to the log and force it to disk."""
        try:
            from app.database import Database
            log_file = Database.get_log_file()
            log_file.log_commit(self.tid)
            log_file.force()  # Ensure commit record is on disk
        except Exception as e:
            print(f"Warning: Could not log commit: {e}")

    def _complete_commit(self) -> None:
        """Complete the commit by releasing locks and updating state."""
        from app.database import Database

        try:
            # Notify buffer pool of transaction completion
            buffer_pool = Database.get_buffer_pool()
            buffer_pool.transaction_complete(self.tid, commit=True)

            # Update transaction state
            self.state = TransactionState.COMMITTED
            self.end_time = time.time()

        except Exception as e:
            raise DbException(f"Failed to complete commit: {e}")

    def _abort_internal(self, reason: str) -> None:
        """Internal abort implementation."""
        try:
            from app.database import Database

            # Write abort record to log
            try:
                log_file = Database.get_log_file()
                log_file.log_abort(self.tid)
            except Exception as e:
                print(f"Warning: Could not log abort: {e}")

            # Undo changes and release locks
            buffer_pool = Database.get_buffer_pool()
            buffer_pool.transaction_complete(self.tid, commit=False)

            # Update state
            self.state = TransactionState.ABORTED
            self.end_time = time.time()

            print(f"Transaction {self.tid} aborted: {reason}")

        except Exception as e:
            print(f"Error during abort: {e}")
            # Still mark as aborted even if cleanup fails
            self.state = TransactionState.ABORTED
            self.end_time = time.time()

    def handle_deadlock_abort(self) -> bool:
        """
        Handle being aborted due to deadlock.

        Returns:
            True if transaction should retry, False if it should give up
        """
        with self._lock:
            self.deadlock_retries += 1

            if self.deadlock_retries <= self.max_deadlock_retries:
                # Reset transaction state for retry
                self.state = TransactionState.CREATED
                self.start_time = None
                self.end_time = None
                self._pages_read.clear()
                self._pages_written.clear()

                # Exponential backoff before retry
                backoff_time = 0.1 * (2 ** self.deadlock_retries)
                time.sleep(backoff_time)

                return True
            else:
                # Too many retries - give up
                return False

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
            # No exception - commit
            self.commit()
        else:
            # Exception occurred - abort
            self.abort()
        return False  # Don't suppress exceptions
