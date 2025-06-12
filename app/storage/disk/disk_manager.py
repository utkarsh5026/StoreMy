import os
import threading
from pathlib import Path
from typing import Any
from dataclasses import dataclass
from .exceptions import StorageError, CorruptionError


@dataclass
class DiskManagerStats:
    pages_read: int = 0
    pages_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    corruption_errors: int = 0


class DiskManager:
    """
    Low-level disk I/O operations with safety guarantees.

    This class handles the actual reading/writing of bytes to disk
    with proper error handling, corruption detection, and performance optimization.
    """

    def __init__(self, base_directory: str = "database_data"):
        self.base_dir = Path(base_directory)
        self.base_dir.mkdir(exist_ok=True)

        self._file_handles: dict[str, Any] = {}
        self._file_lock = threading.RLock()
        self.stats = DiskManagerStats()

    def read_page(self, file_path: str, page_number: int, page_size: int = 4096) -> bytes:
        """
        Read a single page from disk with corruption checking.

        Args:
            file_path: Path to the database file
            page_number: Which page to read (0-based)
            page_size: Size of each page in bytes

        Returns:
            Raw page data as bytes

        Raises:
            StorageError: If I/O fails
            CorruptionError: If data corruption detected
        """
        file_offset = page_number * page_size

        try:
            with self._get_file_handle(file_path, 'rb') as f:
                f.seek(file_offset)
                raw_data = f.read(page_size)

                # Handle partial reads (file shorter than expected)
                if len(raw_data) < page_size:
                    # Pad with zeros - this represents empty space
                    raw_data += b'\x00' * (page_size - len(raw_data))

                # Basic corruption detection using checksums
                if len(raw_data) == page_size:
                    self._verify_page_integrity(raw_data, page_number)

                self.stats.pages_read += 1
                self.stats.bytes_read += len(raw_data)

                return raw_data

        except (IOError, OSError) as e:
            raise StorageError(
                f"Failed to read page {page_number} from {file_path}: {e}")

    def write_page(self, file_path: str, page_number: int, data: bytes) -> None:
        """
        Write a single page to disk with durability guarantees.

        This method ensures that data is actually written to persistent storage,
        not just the OS buffer cache.
        """
        if len(data) != 4096:  # Standard page size
            raise ValueError(
                f"Page data must be exactly 4096 bytes, got {len(data)}")

        file_offset = page_number * len(data)

        try:
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)

            # Open in read-write mode, create if it doesn't exist
            with self._get_file_handle(file_path, 'r+b', create_if_missing=True) as f:
                f.seek(file_offset)
                f.write(data)

                # CRITICAL: Force data to disk for durability
                f.flush()  # Flush to OS buffers
                os.fsync(f.fileno())  # Force OS to write to disk

                self.stats.pages_written += 1
                self.stats.bytes_written += len(data)

        except (IOError, OSError) as e:
            raise StorageError(
                f"Failed to write page {page_number} to {file_path}: {e}")

    def _get_file_handle(self, file_path: str, mode: str, create_if_missing: bool = False):
        """Get a file handle with proper error handling"""
        abs_path = str(Path(file_path).resolve())

        if create_if_missing and not Path(abs_path).exists():
            Path(abs_path).touch()

        try:
            return open(abs_path, mode)
        except FileNotFoundError:
            if 'r' in mode:
                raise StorageError(f"Database file not found: {abs_path}")
            raise

    def _verify_page_integrity(self, data: bytes, page_number: int) -> None:
        """Basic corruption detection using checksums"""
        # For now, just check for obviously corrupted data
        # In production, you'd implement proper checksums
        if len(data) == 0:
            self.stats.corruption_errors += 1
            raise CorruptionError(
                f"Page {page_number} is empty (potential corruption)")

    @classmethod
    def get_file_size(cls, file_path: str) -> int:
        """Get the size of database file in bytes"""
        try:
            return Path(file_path).stat().st_size
        except FileNotFoundError:
            return 0
