import threading
from typing import Optional

from .storage.buffer_pool import BufferPool
from .catalog.catalog import Catalog
from .recovery.log_file import LogFile


class Database:
    """
    Database is a singleton class that initializes and manages the core
    database components.

    It provides static access to:
    - The catalog (table metadata)
    - The buffer pool (page cache)
    - The log file (for recovery)

    This singleton pattern ensures there's only one instance of each
    core component throughout the application.
    """

    _instance: Optional['Database'] = None
    _lock = threading.Lock()

    def __init__(self):
        """
        Initialize the database components.

        This should only be called once (singleton pattern).
        """
        self._catalog = Catalog()
        self._buffer_pool = BufferPool(BufferPool.DEFAULT_PAGES)
        self._log_file = LogFile("database.log")
        self._catalog_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> 'Database':
        """Get the singleton Database instance."""
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
        """Return the buffer pool of the static Database instance."""
        return cls.get_instance()._buffer_pool

    @classmethod
    def get_log_file(cls) -> 'LogFile':
        """Return the log file of the static Database instance."""
        return cls.get_instance()._log_file

    @classmethod
    def get_catalog_lock(cls) -> threading.Lock:
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

        This is used for testing.
        """
        with cls._lock:
            cls._instance = None
