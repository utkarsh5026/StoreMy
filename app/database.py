import atexit
import time
import threading
from typing import Optional

from app.storage.buffer_pool.buffer_pool import BufferPool
from .catalog.catalog import Catalog
from .recovery.log_file import LogFile
from .concurrency.transactions import TransactionManager


class Database:
    """
    Enhanced Database singleton with robust concurrency control.

    This class manages the core database components with full transaction support:

    1. **Catalog**: Table metadata and schema management
    2. **Buffer Pool**: Page caching with lock integration
    3. **Log File**: Write-ahead logging for durability
    4. **Transaction Manager**: Transaction lifecycle coordination

    Key Features:
    - Thread-safe singleton pattern
    - Integrated concurrency control
    - ACID transaction guarantees
    - Deadlock detection and resolution
    - Performance monitoring and statistics

    Architecture:
    ```
    Database (Singleton)
    ├── Catalog (Table metadata)
    ├── BufferPool (Page cache + Lock Manager)
    ├── LogFile (Write-ahead logging)
    └── TransactionManager (Transaction coordination)
    ```

    The Database class ensures all components work together correctly
    and provides a clean API for database operations.
    """

    _instance: Optional['Database'] = None
    _lock = threading.Lock()

    def __init__(self):
        """
        Initialize the database components.

        This should only be called once due to singleton pattern.
        The initialization order is important for proper dependency management.
        """
        # Core metadata management
        self._catalog = Catalog()

        # Enhanced buffer pool with integrated lock manager
        self._buffer_pool = BufferPool(BufferPool.DEFAULT_PAGES)

        # Write-ahead logging for durability and recovery
        self._log_file = LogFile("database.log")

        # Transaction lifecycle management
        self._transaction_manager = TransactionManager()

        # Synchronization for catalog operations
        self._catalog_lock = threading.RLock()

        # System state
        self._shutdown = False

        print("Database system initialized with enhanced concurrency control")

    @classmethod
    def get_instance(cls) -> 'Database':
        """
        Get the singleton Database instance.

        Uses double-checked locking pattern for thread safety
        without excessive synchronization overhead.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = Database()
        return cls._instance

    @classmethod
    def get_catalog(cls) -> 'Catalog':
        """Return the catalog of the static Database instance."""
        return cls.get_instance()._catalog

    @classmethod
    def get_buffer_pool(cls) -> BufferPool:
        """Return the enhanced buffer pool of the static Database instance."""
        return cls.get_instance()._buffer_pool

    @classmethod
    def get_log_file(cls) -> 'LogFile':
        """Return the log file of the static Database instance."""
        return cls.get_instance()._log_file

    @classmethod
    def get_transaction_manager(cls) -> 'TransactionManager':
        """Return the transaction manager of the static Database instance."""
        return cls.get_instance()._transaction_manager

    @classmethod
    def get_catalog_lock(cls) -> threading.RLock:
        """Return the catalog lock for synchronization."""
        return cls.get_instance()._catalog_lock

    @classmethod
    def reset_buffer_pool(cls, num_pages: int) -> BufferPool:
        """
        Create a new buffer pool with the specified number of pages.

        This is used for testing to ensure clean state.

        Args:
            num_pages: Number of pages for the new buffer pool

        Returns:
            The new buffer pool instance
        """
        instance = cls.get_instance()
        instance._buffer_pool = BufferPool(num_pages)
        return instance._buffer_pool

    @classmethod
    def reset(cls) -> None:
        """
        Reset the database to a clean state.

        This is primarily used for testing. In production,
        shutdown() should be called instead.
        """
        with cls._lock:
            if cls._instance is not None:
                cls._instance.shutdown()
            cls._instance = None

    def shutdown(self) -> None:
        """
        Gracefully shutdown the database system.

        This ensures:
        1. All active transactions are completed or aborted
        2. All dirty pages are flushed to disk
        3. Final checkpoint is written
        4. All resources are cleaned up
        """
        if self._shutdown:
            return

        print("Shutting down database system...")
        self._shutdown = True

        try:
            # 1. Abort all active transactions
            print("Aborting active transactions...")
            self._transaction_manager.abort_all_transactions()

            # 2. Flush all dirty pages
            print("Flushing dirty pages...")
            self._buffer_pool.flush_all_pages()

            # 3. Write final checkpoint
            print("Writing final checkpoint...")
            try:
                self._log_file.log_checkpoint()
            except Exception as e:
                print(f"Warning: Could not write final checkpoint: {e}")

            # 4. Print final statistics
            self._print_shutdown_statistics()

            print("Database shutdown complete")

        except Exception as e:
            print(f"Error during shutdown: {e}")

    def _print_shutdown_statistics(self) -> None:
        """Print final system statistics on shutdown."""
        try:
            transaction_stats = self._transaction_manager.get_statistics()
            buffer_stats = self._buffer_pool.get_stats()

            print("\n=== Final Database Statistics ===")
            print(
                f"Transactions started: {transaction_stats['transactions_started']}")
            print(
                f"Transactions committed: {transaction_stats['transactions_committed']}")
            print(
                f"Transactions aborted: {transaction_stats['transactions_aborted']}")
            print(f"Commit rate: {transaction_stats['commit_rate']:.1%}")
            print(f"Buffer pool hit rate: {buffer_stats.hit_rate:.1%}")
            print(f"Total deadlocks: {buffer_stats.deadlocks_detected}")
            print(f"Disk reads: {buffer_stats.disk_reads}")
            print(f"Disk writes: {buffer_stats.disk_writes}")

        except Exception as e:
            print(f"Could not print statistics: {e}")

    def get_system_info(self) -> dict:
        """
        Get comprehensive system information for monitoring.

        Returns:
            Dictionary containing detailed system state
        """
        try:
            transaction_stats = self._transaction_manager.get_statistics()
            buffer_debug = self._buffer_pool.get_debug_info()

            return {
                'system_status': 'running' if not self._shutdown else 'shutdown',
                'transaction_stats': transaction_stats,
                'buffer_pool': {
                    'stats': buffer_debug['stats'].__dict__,
                    'cache_info': {
                        'cache_size': buffer_debug['cache_size'],
                        'max_cache_size': buffer_debug['max_cache_size'],
                        'dirty_pages': buffer_debug['dirty_pages'],
                        'clean_pages': buffer_debug['clean_pages']
                    }
                },
                'lock_manager': buffer_debug['lock_info'],
                'catalog': {
                    'total_tables': len(list(self._catalog.table_id_iterator()))
                }
            }
        except Exception as e:
            return {'error': f"Could not get system info: {e}"}

    def health_check(self) -> dict:
        """
        Perform a health check of the database system.

        Returns:
            Dictionary with health status and any issues
        """
        issues = []
        warnings = []

        try:
            # Check if system is running
            if self._shutdown:
                issues.append("Database is shutdown")

            # Check buffer pool health
            buffer_stats = self._buffer_pool.get_stats()
            if buffer_stats.hit_rate < 0.8:  # Less than 80% hit rate
                warnings.append(
                    f"Low buffer pool hit rate: {buffer_stats.hit_rate:.1%}")

            # Check transaction health
            transaction_stats = self._transaction_manager.get_statistics()
            if transaction_stats['abort_rate'] > 0.1:  # More than 10% abort rate
                warnings.append(
                    f"High transaction abort rate: {transaction_stats['abort_rate']:.1%}")

            # Check for excessive deadlocks
            if buffer_stats.deadlocks_detected > 100:
                warnings.append(
                    f"High deadlock count: {buffer_stats.deadlocks_detected}")

            # Determine overall health
            if issues:
                status = "critical"
            elif warnings:
                status = "warning"
            else:
                status = "healthy"

            return {
                'status': status,
                'issues': issues,
                'warnings': warnings,
                'timestamp': time.time()
            }

        except Exception as e:
            return {
                'status': 'error',
                'issues': [f"Health check failed: {e}"],
                'warnings': [],
                'timestamp': time.time()
            }


# Convenience functions for easy access

def get_database() -> Database:
    """Get the database instance."""
    return Database.get_instance()


def get_catalog():
    """Get the catalog instance."""
    return Database.get_catalog()


def get_buffer_pool():
    """Get the buffer pool instance."""
    return Database.get_buffer_pool()


def get_transaction_manager():
    """Get the transaction manager instance."""
    return Database.get_transaction_manager()


# Context manager for database operations
class DatabaseContext:
    """
    Context manager for database operations.

    Usage:
        with DatabaseContext() as db:
            # Database operations
            # Automatic cleanup on exit
    """

    def __enter__(self):
        self.database = Database.get_instance()
        return self.database

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Could add cleanup logic here if needed
        pass


# Module-level initialization

# Register shutdown handler
atexit.register(lambda: Database.get_instance().shutdown())

# Print initialization message
_db_instance = Database.get_instance()
print(f"Database module loaded - System ready for transactions")
