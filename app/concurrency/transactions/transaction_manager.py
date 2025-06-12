import threading
from .transaction import Transaction, TransactionState

class TransactionManager:
    """
    Manages the lifecycle of multiple transactions and provides utilities
    for transaction coordination.

    This class provides:
    1. Transaction factory methods
    2. Global transaction tracking
    3. Deadlock monitoring
    4. Transaction statistics
    """

    def __init__(self):
        self._active_transactions: set[Transaction] = set()
        self._lock = threading.RLock()

        self.transactions_started = 0
        self.transactions_committed = 0
        self.transactions_aborted = 0
        self.deadlocks_detected = 0

    def create_transaction(self) -> Transaction:
        """Create a new transaction and register it."""
        with self._lock:
            transaction = Transaction()
            self._active_transactions.add(transaction)
            self.transactions_started += 1
            return transaction

    def transaction_completed(self, transaction: Transaction) -> None:
        """Notify that a transaction has completed."""
        with self._lock:
            self._active_transactions.discard(transaction)

            if transaction.get_state() == TransactionState.COMMITTED:
                self.transactions_committed += 1
            elif transaction.get_state() == TransactionState.ABORTED:
                self.transactions_aborted += 1

    def get_active_transactions(self) -> set[Transaction]:
        """Get all currently active transactions."""
        with self._lock:
            return self._active_transactions.copy()

    def abort_all_transactions(self) -> None:
        """Abort all active transactions (used for shutdown)."""
        with self._lock:
            # Copy to avoid modification during iteration
            transactions_to_abort = list(self._active_transactions)

            for transaction in transactions_to_abort:
                try:
                    transaction.abort()
                except Exception as e:
                    print(f"Error aborting transaction {transaction.tid}: {e}")

    def get_statistics(self) -> dict:
        """Get transaction statistics."""
        with self._lock:
            return {
                'active_transactions': len(self._active_transactions),
                'transactions_started': self.transactions_started,
                'transactions_committed': self.transactions_committed,
                'transactions_aborted': self.transactions_aborted,
                'deadlocks_detected': self.deadlocks_detected,
                'commit_rate': (self.transactions_committed /
                                max(1, self.transactions_started)),
                'abort_rate': (self.transactions_aborted /
                               max(1, self.transactions_started))
            }


# Global transaction manager instance
_transaction_manager = TransactionManager()


def get_transaction_manager() -> TransactionManager:
    """Get the global transaction manager instance."""
    return _transaction_manager


def create_transaction() -> Transaction:
    """
    Convenience function to create a new transaction.

    Usage:
        with create_transaction() as txn:
            # Do database operations
            # Transaction commits automatically
    """
    return _transaction_manager.create_transaction()
