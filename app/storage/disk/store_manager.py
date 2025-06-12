from pathlib import Path
from typing import Dict
from .disk_manager import DiskManager, DiskManagerStats
from .exceptions import StorageError


class StorageManager:
    """
    High-level interface for database storage operations.

    This class provides the interface that the rest of the database
    (buffer pool, catalog, etc.) uses for persistent storage.
    """

    def __init__(self, data_directory: str = "database_data"):
        self.disk_manager = DiskManager(data_directory)
        self.data_dir = Path(data_directory)
        self._table_files: dict[int, str] = {}

    def create_table_file(self, table_id: int, table_name: str) -> str:
        """Create a new database file for a table"""
        file_path = self.data_dir / f"{table_name}_{table_id}.dat"
        file_path.touch()
        self._table_files[table_id] = str(file_path)

        return str(file_path)

    def get_table_file_path(self, table_id: int) -> str:
        """Get the file path for a table"""
        if table_id not in self._table_files:
            raise StorageError(f"No file found for table {table_id}")
        return self._table_files[table_id]

    def read_page(self, table_id: int, page_number: int) -> bytes:
        """Read a page from a table's file"""
        file_path = self.get_table_file_path(table_id)
        return self.disk_manager.read_page(file_path, page_number)

    def write_page(self, table_id: int, page_number: int, data: bytes) -> None:
        """Write a page to a table's file"""
        file_path = self.get_table_file_path(table_id)
        self.disk_manager.write_page(file_path, page_number, data)

    def get_num_pages(self, table_id: int) -> int:
        """Get the number of pages in a table's file"""
        file_path = self.get_table_file_path(table_id)
        file_size = self.disk_manager.get_file_size(file_path)
        return (file_size + 4095) // 4096

    def sync_all_files(self) -> None:
        """Force all pending writes to disk (for transaction commits)"""
        # This would iterate through all open files and call fsync()
        pass

    def get_storage_stats(self) -> DiskManagerStats:
        """Get I/O statistics for monitoring"""
        # Return a copy of the stats dataclass
        from copy import copy
        return copy(self.disk_manager.stats)
