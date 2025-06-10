from ...primitives import TransactionId
from app.database import Database


class Transaction:
    """
    Transaction encapsulates information about the state of a transaction
    and manages transaction commit/abort.

    Each transaction maintains its own ID and tracks whether it has been
    started. The transaction coordinates with the buffer pool and log file
    to ensure ACID properties.
    """

    def __init__(self):
        """Create a new transaction with a unique ID."""
        self.tid = TransactionId()
        self.started = False

    def start(self) -> None:
        """
        Start the transaction.

        This logs the transaction begin record and marks the transaction
        as started.
        """
        self.started = True
        try:
            Database.get_log_file().log_transaction_begin(self.tid)
        except Exception as e:
            print(f"Warning: Could not log transaction begin: {e}")

    def get_id(self) -> TransactionId:
        """Return the ID of this transaction."""
        return self.tid

    def commit(self) -> None:
        """
        Commit the transaction.

        This writes all dirty pages to disk and logs the commit record.
        """
        self.transaction_complete(False)

    def abort(self) -> None:
        """
        Abort the transaction.

        These discards are all changes and logs the abort record.
        """
        self.transaction_complete(True)

    def transaction_complete(self, abort: bool) -> None:
        """
        Handle the details of transaction commit/abort.

        Args:
            abort: True to abort, False to commit
        """
        if self.started:
            try:
                if abort:
                    Database.get_log_file().log_abort(self.tid)
                else:
                    Database.get_buffer_pool().flush_pages(self.tid)
                    Database.get_log_file().log_commit(self.tid)

                Database.get_buffer_pool().transaction_complete(self.tid, not abort)

            except Exception as e:
                print(f"Error during transaction completion: {e}")

            finally:
                self.started = False
