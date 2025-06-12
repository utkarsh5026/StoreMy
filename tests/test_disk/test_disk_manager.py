import pytest
import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch
from app.storage.disk import DiskManager, DiskManagerStats, StorageError, CorruptionError


class TestDiskManagerStats:
    """Tests for DiskManagerStats dataclass."""

    def test_init_default_values(self):
        """Test that stats are initialized with default values."""
        stats = DiskManagerStats()
        assert stats.pages_read == 0
        assert stats.pages_written == 0
        assert stats.bytes_read == 0
        assert stats.bytes_written == 0
        assert stats.corruption_errors == 0

    def test_init_custom_values(self):
        """Test initialization with custom values."""
        stats = DiskManagerStats(
            pages_read=10,
            pages_written=5,
            bytes_read=40960,
            bytes_written=20480,
            corruption_errors=2
        )
        assert stats.pages_read == 10
        assert stats.pages_written == 5
        assert stats.bytes_read == 40960
        assert stats.bytes_written == 20480
        assert stats.corruption_errors == 2


class TestDiskManagerExceptions:
    """Tests for custom exception classes."""

    def test_storage_error_inheritance(self):
        """Test that StorageError inherits from Exception."""
        error = StorageError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_corruption_error_inheritance(self):
        """Test that CorruptionError inherits from StorageError."""
        error = CorruptionError("corruption detected")
        assert isinstance(error, StorageError)
        assert isinstance(error, Exception)
        assert str(error) == "corruption detected"


class TestDiskManager:
    """Comprehensive tests for DiskManager implementation."""

    def setup_method(self):
        """Set up test environment before each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.disk_manager = DiskManager(base_directory=self.temp_dir)
        self.test_file = os.path.join(self.temp_dir, "test_file.db")

    def teardown_method(self):
        """Clean up after each test."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_creates_base_directory(self):
        """Test that initialization creates the base directory."""
        new_temp_dir = tempfile.mktemp()
        DiskManager(base_directory=new_temp_dir)

        assert Path(new_temp_dir).exists()
        assert Path(new_temp_dir).is_dir()

        # Clean up
        import shutil
        shutil.rmtree(new_temp_dir, ignore_errors=True)

    def test_init_default_directory(self):
        """Test initialization with default directory."""
        dm = DiskManager()
        assert dm.base_dir == Path("database_data")
        assert isinstance(dm.stats, DiskManagerStats)
        assert isinstance(dm._file_lock, type(threading.RLock()))

        # Clean up the default directory
        import shutil
        if Path("database_data").exists():
            shutil.rmtree("database_data", ignore_errors=True)

    def test_init_stats_initialized(self):
        """Test that stats are properly initialized."""
        dm = DiskManager()
        assert dm.stats.pages_read == 0
        assert dm.stats.pages_written == 0
        assert dm.stats.bytes_read == 0
        assert dm.stats.bytes_written == 0
        assert dm.stats.corruption_errors == 0

        # Clean up the default directory
        import shutil
        if Path("database_data").exists():
            shutil.rmtree("database_data", ignore_errors=True)

    def test_write_page_valid_data(self):
        """Test writing a valid page to disk."""
        data = b'A' * 4096  # Valid 4KB page

        self.disk_manager.write_page(self.test_file, 0, data)

        # Verify file was created and has correct size
        assert Path(self.test_file).exists()
        assert Path(self.test_file).stat().st_size == 4096

        # Verify stats were updated
        assert self.disk_manager.stats.pages_written == 1
        assert self.disk_manager.stats.bytes_written == 4096

    def test_write_page_invalid_size_raises_error(self):
        """Test that writing data with wrong size raises ValueError."""
        data = b'A' * 2048  # Wrong size

        with pytest.raises(ValueError, match="Page data must be exactly 4096 bytes, got 2048"):
            self.disk_manager.write_page(self.test_file, 0, data)

    def test_write_page_creates_directories(self):
        """Test that write_page creates necessary directories."""
        nested_path = os.path.join(self.temp_dir, "nested", "dirs", "file.db")
        data = b'B' * 4096

        self.disk_manager.write_page(nested_path, 0, data)

        assert Path(nested_path).exists()
        assert Path(nested_path).parent.exists()

    def test_write_multiple_pages(self):
        """Test writing multiple pages to the same file."""
        page0_data = b'0' * 4096
        page1_data = b'1' * 4096

        self.disk_manager.write_page(self.test_file, 0, page0_data)
        self.disk_manager.write_page(self.test_file, 1, page1_data)

        # File should be 8KB (2 pages)
        assert Path(self.test_file).stat().st_size == 8192
        assert self.disk_manager.stats.pages_written == 2
        assert self.disk_manager.stats.bytes_written == 8192

    def test_read_page_existing_file(self):
        """Test reading a page from an existing file."""
        # First write a page
        original_data = b'C' * 4096
        self.disk_manager.write_page(self.test_file, 0, original_data)

        # Reset stats to test read operation
        self.disk_manager.stats = DiskManagerStats()

        # Read the page back
        read_data = self.disk_manager.read_page(self.test_file, 0)

        assert read_data == original_data
        assert len(read_data) == 4096
        assert self.disk_manager.stats.pages_read == 1
        assert self.disk_manager.stats.bytes_read == 4096

    def test_read_page_nonexistent_file_raises_error(self):
        """Test reading from a non-existent file raises StorageError."""
        nonexistent_file = os.path.join(self.temp_dir, "nonexistent.db")

        with pytest.raises(StorageError, match="Database file not found"):
            self.disk_manager.read_page(nonexistent_file, 0)

    def test_read_page_beyond_file_end_pads_with_zeros(self):
        """Test reading beyond file end returns zero-padded data."""
        # Write a smaller file (1 page)
        data = b'D' * 4096
        self.disk_manager.write_page(self.test_file, 0, data)

        # Try to read page 2 (which doesn't exist)
        read_data = self.disk_manager.read_page(self.test_file, 2)

        # Should return all zeros
        assert read_data == b'\x00' * 4096
        assert len(read_data) == 4096

    def test_read_page_partial_file_pads_with_zeros(self):
        """Test reading from a file shorter than page size pads with zeros."""
        # Write partial data directly to file
        with open(self.test_file, 'wb') as f:
            f.write(b'E' * 2048)  # Only half a page

        read_data = self.disk_manager.read_page(self.test_file, 0)

        expected = b'E' * 2048 + b'\x00' * 2048
        assert read_data == expected
        assert len(read_data) == 4096

    def test_read_page_custom_page_size(self):
        """Test reading with custom page size."""
        # Write data with custom size
        custom_size = 8192
        data = b'F' * custom_size

        with open(self.test_file, 'wb') as f:
            f.write(data)

        read_data = self.disk_manager.read_page(
            self.test_file, 0, page_size=custom_size)

        assert read_data == data
        assert len(read_data) == custom_size

    def test_verify_page_integrity_empty_page_raises_corruption_error(self):
        """Test that empty page raises CorruptionError."""
        empty_data = b''

        with pytest.raises(CorruptionError, match="Page 0 is empty \\(potential corruption\\)"):
            self.disk_manager._verify_page_integrity(empty_data, 0)

        assert self.disk_manager.stats.corruption_errors == 1

    def test_verify_page_integrity_valid_page(self):
        """Test that valid page passes integrity check."""
        valid_data = b'G' * 4096

        # Should not raise any exception
        self.disk_manager._verify_page_integrity(valid_data, 0)
        assert self.disk_manager.stats.corruption_errors == 0

    def test_get_file_size_existing_file(self):
        """Test getting size of an existing file."""
        data = b'H' * 4096
        self.disk_manager.write_page(self.test_file, 0, data)

        size = self.disk_manager.get_file_size(self.test_file)
        assert size == 4096

    def test_get_file_size_nonexistent_file(self):
        """Test getting size of non-existent file returns 0."""
        nonexistent_file = os.path.join(self.temp_dir, "nonexistent.db")

        size = self.disk_manager.get_file_size(nonexistent_file)
        assert size == 0

    def test_get_file_size_multiple_pages(self):
        """Test getting size of file with multiple pages."""
        for i in range(3):
            data = bytes([i]) * 4096
            self.disk_manager.write_page(self.test_file, i, data)

        size = self.disk_manager.get_file_size(self.test_file)
        assert size == 12288  # 3 * 4096

    def test_get_file_handle_read_mode(self):
        """Test getting file handle in read mode."""
        # Create file first
        data = b'I' * 4096
        self.disk_manager.write_page(self.test_file, 0, data)

        with self.disk_manager._get_file_handle(self.test_file, 'rb') as f:
            assert f.readable()
            assert not f.writable()

    def test_get_file_handle_write_mode_create_if_missing(self):
        """Test getting file handle in write mode with create_if_missing."""
        new_file = os.path.join(self.temp_dir, "new_file.db")

        with self.disk_manager._get_file_handle(new_file, 'r+b', create_if_missing=True) as f:
            assert f.readable()
            assert f.writable()

        assert Path(new_file).exists()

    def test_get_file_handle_read_nonexistent_raises_error(self):
        """Test that reading non-existent file raises StorageError."""
        nonexistent_file = os.path.join(self.temp_dir, "nonexistent.db")

        with pytest.raises(StorageError, match="Database file not found"):
            self.disk_manager._get_file_handle(nonexistent_file, 'rb')

    def test_thread_safety_concurrent_writes(self):
        """Test thread safety with concurrent write operations."""
        def write_worker(page_num, data_char):
            data = bytes([ord(data_char)]) * 4096
            self.disk_manager.write_page(self.test_file, page_num, data)

        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=write_worker, args=(i, chr(ord('A') + i)))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all pages were written correctly
        assert self.disk_manager.stats.pages_written == 5
        assert Path(self.test_file).stat().st_size == 20480  # 5 * 4096

    def test_thread_safety_concurrent_reads(self):
        """Test thread safety with concurrent read operations."""
        # First, write some test data
        for i in range(3):
            data = bytes([ord('A') + i]) * 4096
            self.disk_manager.write_page(self.test_file, i, data)

        # Reset stats
        self.disk_manager.stats = DiskManagerStats()

        results = {}

        def read_worker(page_num):
            results[page_num] = self.disk_manager.read_page(
                self.test_file, page_num)

        threads = []
        for i in range(3):
            thread = threading.Thread(target=read_worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all reads completed successfully
        assert len(results) == 3
        assert self.disk_manager.stats.pages_read == 3

        # Verify data integrity
        for i in range(3):
            expected = bytes([ord('A') + i]) * 4096
            assert results[i] == expected

    def test_fsync_called_during_write(self):
        """Test that fsync is called to ensure durability."""
        data = b'J' * 4096

        with patch('os.fsync') as mock_fsync:
            self.disk_manager.write_page(self.test_file, 0, data)
            mock_fsync.assert_called_once()

    def test_io_error_handling_write(self):
        """Test proper error handling for I/O errors during write."""
        data = b'K' * 4096

        with patch('builtins.open', side_effect=IOError("Disk full")):
            with pytest.raises(StorageError, match="Failed to write page 0"):
                self.disk_manager.write_page(self.test_file, 0, data)

    def test_io_error_handling_read(self):
        """Test proper error handling for I/O errors during read."""
        with patch('builtins.open', side_effect=IOError("Read error")):
            with pytest.raises(StorageError, match="Failed to read page 0"):
                self.disk_manager.read_page(self.test_file, 0)

    def test_write_page_offset_calculation(self):
        """Test that page offsets are calculated correctly."""
        # Write pages at different positions
        page_data = [b'0' * 4096, b'1' * 4096, b'2' * 4096]

        for i, data in enumerate(page_data):
            self.disk_manager.write_page(self.test_file, i, data)

        # Read back and verify each page
        for i, expected_data in enumerate(page_data):
            read_data = self.disk_manager.read_page(self.test_file, i)
            assert read_data == expected_data

    def test_write_non_sequential_pages(self):
        """Test writing pages in non-sequential order."""
        # Write pages 0, 2, 1 (out of order)
        self.disk_manager.write_page(self.test_file, 0, b'0' * 4096)
        self.disk_manager.write_page(self.test_file, 2, b'2' * 4096)
        self.disk_manager.write_page(self.test_file, 1, b'1' * 4096)

        # File should be 3 pages long
        assert Path(self.test_file).stat().st_size == 12288

        # Verify each page
        assert self.disk_manager.read_page(self.test_file, 0) == b'0' * 4096
        assert self.disk_manager.read_page(self.test_file, 1) == b'1' * 4096
        assert self.disk_manager.read_page(self.test_file, 2) == b'2' * 4096

    def test_stats_tracking_accuracy(self):
        """Test that statistics are tracked accurately."""
        # Perform various operations
        data1 = b'L' * 4096
        data2 = b'M' * 4096

        self.disk_manager.write_page(self.test_file, 0, data1)
        self.disk_manager.write_page(self.test_file, 1, data2)

        self.disk_manager.read_page(self.test_file, 0)
        self.disk_manager.read_page(self.test_file, 1)

        # Trigger a corruption error
        try:
            self.disk_manager._verify_page_integrity(b'', 0)
        except CorruptionError:
            pass

        # Verify stats
        assert self.disk_manager.stats.pages_written == 2
        assert self.disk_manager.stats.bytes_written == 8192
        assert self.disk_manager.stats.pages_read == 2
        assert self.disk_manager.stats.bytes_read == 8192
        assert self.disk_manager.stats.corruption_errors == 1

    def test_edge_case_zero_page_number(self):
        """Test reading/writing page 0."""
        data = b'N' * 4096
        self.disk_manager.write_page(self.test_file, 0, data)

        read_data = self.disk_manager.read_page(self.test_file, 0)
        assert read_data == data

    def test_edge_case_large_page_number(self):
        """Test reading/writing large page numbers."""
        large_page_num = 1000
        data = b'O' * 4096

        self.disk_manager.write_page(self.test_file, large_page_num, data)
        read_data = self.disk_manager.read_page(self.test_file, large_page_num)

        assert read_data == data
        # File should be sized to accommodate the large offset
        expected_size = (large_page_num + 1) * 4096
        assert Path(self.test_file).stat().st_size == expected_size

    def test_path_resolution(self):
        """Test that file paths are resolved correctly."""
        # Test with relative path - it should resolve relative to current working directory
        relative_path = "relative_file.db"
        data = b'P' * 4096

        try:
            self.disk_manager.write_page(relative_path, 0, data)

            # Should create file in current working directory, not base directory
            expected_path = Path(relative_path).resolve()
            assert expected_path.exists()

        finally:
            # Clean up the file created in working directory
            expected_path = Path(relative_path).resolve()
            if expected_path.exists():
                expected_path.unlink()

    def test_file_handle_context_management(self):
        """Test that file handles are properly closed."""
        data = b'Q' * 4096
        self.disk_manager.write_page(self.test_file, 0, data)

        # File operations should work even after previous operations
        # (indicating handles were properly closed)
        read_data = self.disk_manager.read_page(self.test_file, 0)
        assert read_data == data
