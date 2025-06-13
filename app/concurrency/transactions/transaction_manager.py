import threading
import time
from typing import Set, Dict, Optional
from .transaction import Transaction, TransactionState


class TransactionManager:
    """
    Comprehensive transaction manager with monitoring and coordination.

    Responsibilities:
    - Transaction lifecycle tracking
    - Global deadlock monitoring
    - Performance statistics
    - Resource cleanup
    - System shutdown coordination
    """

    def __init__(self):
        self._active_transactions: Set[Transaction] = set()
        self._completed_transactions: Dict[str, Transaction] = {}
        self._lock = threading.RLock()

        # Statistics
        self.transactions_started = 0
        self.transactions_committed = 0
        self.transactions_aborted = 0
        self.deadlocks_detected = 0

        # Monitoring
        self._monitoring_enabled = True
        self._monitor_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        # Start monitoring thread
        self._start_monitoring()

    def create_transaction(self) -> Transaction:
        """Create and register a new transaction."""
        with self._lock:
            transaction = Transaction()
            self.transactions_started += 1
            return transaction

    def register_transaction(self, transaction: Transaction) -> None:
        """Register an active transaction."""
        with self._lock:
            self._active_transactions.add(transaction)

    def transaction_completed(self, transaction: Transaction) -> None:
        """Handle transaction completion."""
        with self._lock:
            # Remove from active set
            self._active_transactions.discard(transaction)

            # Add to completed history (keep limited history)
            self._completed_transactions[str(transaction.tid)] = transaction
            if len(self._completed_transactions) > 1000:  # Limit memory usage
                # Remove oldest entries
                oldest_keys = list(self._completed_transactions.keys())[:100]
                for key in oldest_keys:
                    del self._completed_transactions[key]

            # Update statistics
            if transaction.get_state() == TransactionState.COMMITTED:
                self.transactions_committed += 1
            elif transaction.get_state() == TransactionState.ABORTED:
                self.transactions_aborted += 1

    def get_active_transactions(self) -> Set[Transaction]:
        """Get copy of all currently active transactions."""
        with self._lock:
            return self._active_transactions.copy()

    def get_transaction_by_id(self, tid: str) -> Optional[Transaction]:
        """Get transaction by ID from active or recent completed transactions."""
        with self._lock:
            # Check active transactions
            for txn in self._active_transactions:
                if str(txn.tid) == tid:
                    return txn

            # Check completed transactions
            return self._completed_transactions.get(tid)

    def abort_all_transactions(self, reason: str = "System shutdown") -> None:
        """Abort all active transactions."""
        with self._lock:
            # Copy to avoid modification during iteration
            transactions_to_abort = list(self._active_transactions)

            for transaction in transactions_to_abort:
                try:
                    transaction.abort(reason)
                except Exception as e:
                    print(f"Error aborting transaction {transaction.tid}: {e}")

    def detect_global_deadlocks(self) -> bool:
        """
        Perform global deadlock detection across all active transactions.

        Returns:
            True if deadlocks were found and resolved
        """
        try:
            from app.database import Database
            lock_manager = Database.get_lock_manager()

            # Get dependency graph and check for cycles
            cycles = lock_manager.detect_all_deadlocks()

            if cycles:
                self.deadlocks_detected += len(cycles)

                # Resolve each cycle by aborting youngest transaction
                for cycle in cycles:
                    if cycle:
                        victim = self._choose_global_deadlock_victim(cycle)
                        if victim:
                            try:
                                victim.abort("Global deadlock resolution")
                                print(
                                    f"Aborted transaction {victim.tid} to resolve global deadlock")
                            except Exception as e:
                                print(
                                    f"Error aborting deadlock victim {victim.tid}: {e}")

                return True

        except Exception as e:
            print(f"Error in global deadlock detection: {e}")

        return False

    def _choose_global_deadlock_victim(self, transaction_ids) -> Optional[Transaction]:
        """Choose deadlock victim from a cycle of transaction IDs."""
        with self._lock:
            # Find the youngest (highest ID) active transaction in the cycle
            candidates = []
            for tid in transaction_ids:
                for txn in self._active_transactions:
                    if txn.tid == tid:
                        candidates.append(txn)
                        break

            if candidates:
                return max(candidates, key=lambda t: t.tid.get_id())
            return None

    def _start_monitoring(self) -> None:
        """Start background monitoring thread."""
        if self._monitoring_enabled:
            self._monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True,
                name="TransactionMonitor"
            )
            self._monitor_thread.start()

    def _monitoring_loop(self) -> None:
        """Background monitoring loop for deadlock detection and cleanup."""
        while not self._shutdown_event.is_set():
            try:
                # Periodic deadlock detection
                self.detect_global_deadlocks()

                # Clean up old transactions
                self._cleanup_old_transactions()

                # Wait before next check
                self._shutdown_event.wait(5.0)  # Check every 5 seconds

            except Exception as e:
                print(f"Error in transaction monitoring: {e}")
                self._shutdown_event.wait(1.0)

    def _cleanup_old_transactions(self) -> None:
        """Clean up very old active transactions that might be stuck."""
        current_time = time.time()
        max_transaction_age = 300.0  # 5 minutes

        with self._lock:
            old_transactions = [
                txn for txn in self._active_transactions
                if txn.get_age() > max_transaction_age
            ]

            for txn in old_transactions:
                print(
                    f"Warning: Transaction {txn.tid} has been active for {txn.get_age():.1f}s")
                # Could optionally abort very old transactions here

    def get_statistics(self) -> dict:
        """Get comprehensive transaction statistics."""
        with self._lock:
            total_transactions = max(1, self.transactions_started)

            # Calculate average transaction duration from completed transactions
            completed_durations = []
            # Last 100
            for txn in list(self._completed_transactions.values())[-100:]:
                stats = txn.get_statistics()
                if stats['duration'] > 0:
                    completed_durations.append(stats['duration'])

            avg_duration = sum(
                completed_durations) / len(completed_durations) if completed_durations else 0.0

            return {
                'active_transactions': len(self._active_transactions),
                'total_started': self.transactions_started,
                'total_committed': self.transactions_committed,
                'total_aborted': self.transactions_aborted,
                'deadlocks_detected': self.deadlocks_detected,
                'commit_rate': self.transactions_committed / total_transactions,
                'abort_rate': self.transactions_aborted / total_transactions,
                'average_duration': avg_duration,
                'monitoring_enabled': self._monitoring_enabled
            }

    def shutdown(self) -> None:
        """Shutdown transaction manager and cleanup resources."""
        print("Shutting down transaction manager...")

        # Stop monitoring
        self._monitoring_enabled = False
        self._shutdown_event.set()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5.0)

        # Abort all active transactions
        self.abort_all_transactions("System shutdown")

        print("Transaction manager shutdown complete")

    def __del__(self):
        """Cleanup on destruction."""
        try:
            self.shutdown()
        except:
            pass


# Global transaction manager with thread-safe singleton
_transaction_manager: Optional[TransactionManager] = None
_transaction_manager_lock = threading.Lock()


def get_transaction_manager() -> TransactionManager:
    """Get the global transaction manager instance (thread-safe singleton)."""
    global _transaction_manager
    if _transaction_manager is None:
        with _transaction_manager_lock:
            if _transaction_manager is None:
                _transaction_manager = TransactionManager()
    return _transaction_manager


def create_transaction() -> Transaction:
    """
    Convenience function to create a new transaction.

    Usage:
        # Context manager (recommended)
        with create_transaction() as txn:
            # Do database operations
            # Transaction commits automatically on success

        # Manual management
        txn = create_transaction()
        txn.start()
        try:
            # Do operations
            txn.commit()
        except Exception:
            txn.abort()
    """
    return get_transaction_manager().create_transaction()


def shutdown_transaction_system():
    """Shutdown the entire transaction system."""
    global _transaction_manager
    if _transaction_manager:
        _transaction_manager.shutdown()
        _transaction_manager = None
