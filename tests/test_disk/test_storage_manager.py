import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from app.storage.disk.store_manager import StorageManager
from app.storage.disk.disk_manager import DiskManagerStats
from app.storage.disk.exceptions import StorageError


class TestStorageManager:
    """Comprehensive tests for StorageManager implementation."""

    def setup_method(self):
        """Set up test environment before each test."""
        # Create temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        self.storage_manager = StorageManager(data_directory=self.temp_dir)

    def teardown_method(self):
        """Clean up after each test."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init_creates_data_directory(self):
        """Test that initialization creates the data directory."""
        new_temp_dir = tempfile.mktemp()
        sm = StorageManager(data_directory=new_temp_dir)

        assert Path(new_temp_dir).exists()
        assert Path(new_temp_dir).is_dir()
        assert sm.data_dir == Path(new_temp_dir)
        assert hasattr(sm, 'disk_manager')
        assert sm._table_files == {}

        # Clean up
        import shutil
        shutil.rmtree(new_temp_dir, ignore_errors=True)

    def test_init_default_directory(self):
        """Test initialization with default directory."""
        sm = StorageManager()
        assert sm.data_dir == Path("database_data")
        assert hasattr(sm, 'disk_manager')
        assert sm._table_files == {}

        # Clean up the default directory
        import shutil
        if Path("database_data").exists():
            shutil.rmtree("database_data", ignore_errors=True)

    def test_create_table_file_valid(self):
        """Test creating a table file with valid parameters."""
        table_id = 1
        table_name = "users"

        file_path = self.storage_manager.create_table_file(
            table_id, table_name)

        # Verify file was created
        expected_path = Path(self.temp_dir) / f"{table_name}_{table_id}.dat"
        assert Path(file_path) == expected_path
        assert Path(file_path).exists()
        assert Path(file_path).is_file()

        # Verify mapping was stored
        assert table_id in self.storage_manager._table_files
        assert self.storage_manager._table_files[table_id] == str(
            expected_path)

    def test_create_table_file_multiple_tables(self):
        """Test creating multiple table files."""
        tables = [
            (1, "users"),
            (2, "products"),
            (3, "orders")
        ]

        created_files = []
        for table_id, table_name in tables:
            file_path = self.storage_manager.create_table_file(
                table_id, table_name)
            created_files.append(file_path)

            # Verify each file exists
            assert Path(file_path).exists()

            # Verify mapping
            assert table_id in self.storage_manager._table_files

        # Verify all mappings are present
        assert len(self.storage_manager._table_files) == 3

        # Verify file paths are different
        assert len(set(created_files)) == 3

    def test_create_table_file_overwrites_existing_mapping(self):
        """Test that creating a table file with existing table_id overwrites mapping."""
        table_id = 1

        # Create first file
        file1 = self.storage_manager.create_table_file(table_id, "table1")

        # Create second file with same table_id
        file2 = self.storage_manager.create_table_file(table_id, "table2")

        # Verify second file overwrote the mapping
        assert self.storage_manager._table_files[table_id] == file2
        assert file1 != file2

        # Both files should exist
        assert Path(file1).exists()
        assert Path(file2).exists()

    def test_get_table_file_path_existing_table(self):
        """Test getting file path for an existing table."""
        table_id = 1
        table_name = "test_table"

        # Create table first
        created_path = self.storage_manager.create_table_file(
            table_id, table_name)

        # Get path
        retrieved_path = self.storage_manager.get_table_file_path(table_id)

        assert retrieved_path == created_path

    def test_get_table_file_path_nonexistent_table_raises_error(self):
        """Test that getting path for non-existent table raises StorageError."""
        nonexistent_table_id = 999

        with pytest.raises(StorageError, match=f"No file found for table {nonexistent_table_id}"):
            self.storage_manager.get_table_file_path(nonexistent_table_id)

    def test_write_page_valid_table(self):
        """Test writing a page to a valid table."""
        table_id = 1
        table_name = "test_table"
        page_number = 0
        data = b'A' * 4096

        # Create table first
        self.storage_manager.create_table_file(table_id, table_name)

        # Write page
        self.storage_manager.write_page(table_id, page_number, data)

        # Verify file has correct size
        file_path = self.storage_manager.get_table_file_path(table_id)
        assert Path(file_path).stat().st_size == 4096

    def test_write_page_nonexistent_table_raises_error(self):
        """Test that writing to non-existent table raises StorageError."""
        nonexistent_table_id = 999
        page_number = 0
        data = b'A' * 4096

        with pytest.raises(StorageError, match=f"No file found for table {nonexistent_table_id}"):
            self.storage_manager.write_page(
                nonexistent_table_id, page_number, data)

    def test_write_page_invalid_data_size_raises_error(self):
        """Test that writing invalid data size raises ValueError."""
        table_id = 1
        table_name = "test_table"
        page_number = 0
        invalid_data = b'A' * 2048  # Wrong size

        # Create table first
        self.storage_manager.create_table_file(table_id, table_name)

        with pytest.raises(ValueError, match="Page data must be exactly 4096 bytes"):
            self.storage_manager.write_page(
                table_id, page_number, invalid_data)

    def test_read_page_existing_data(self):
        """Test reading a page from a table with existing data."""
        table_id = 1
        table_name = "test_table"
        page_number = 0
        original_data = b'B' * 4096

        # Create table and write data
        self.storage_manager.create_table_file(table_id, table_name)
        self.storage_manager.write_page(table_id, page_number, original_data)

        # Read data back
        read_data = self.storage_manager.read_page(table_id, page_number)

        assert read_data == original_data
        assert len(read_data) == 4096

    def test_read_page_nonexistent_table_raises_error(self):
        """Test that reading from non-existent table raises StorageError."""
        nonexistent_table_id = 999
        page_number = 0

        with pytest.raises(StorageError, match=f"No file found for table {nonexistent_table_id}"):
            self.storage_manager.read_page(nonexistent_table_id, page_number)

    def test_read_page_beyond_file_end_returns_zeros(self):
        """Test reading beyond file end returns zero-padded data."""
        table_id = 1
        table_name = "test_table"

        # Create table but don't write any data
        self.storage_manager.create_table_file(table_id, table_name)

        # Try to read page 0 (should be all zeros since file is empty)
        read_data = self.storage_manager.read_page(table_id, 0)

        assert read_data == b'\x00' * 4096
        assert len(read_data) == 4096

    def test_read_write_multiple_pages(self):
        """Test reading and writing multiple pages."""
        table_id = 1
        table_name = "test_table"
        num_pages = 5

        # Create table
        self.storage_manager.create_table_file(table_id, table_name)

        # Write multiple pages with different data
        page_data = {}
        for page_num in range(num_pages):
            data = bytes([page_num] * 4096)
            self.storage_manager.write_page(table_id, page_num, data)
            page_data[page_num] = data

        # Read back and verify
        for page_num in range(num_pages):
            read_data = self.storage_manager.read_page(table_id, page_num)
            assert read_data == page_data[page_num]

    def test_get_num_pages_empty_file(self):
        """Test getting number of pages for an empty file."""
        table_id = 1
        table_name = "test_table"

        # Create table (creates empty file)
        self.storage_manager.create_table_file(table_id, table_name)

        num_pages = self.storage_manager.get_num_pages(table_id)
        assert num_pages == 0

    def test_get_num_pages_single_page(self):
        """Test getting number of pages for a file with one page."""
        table_id = 1
        table_name = "test_table"
        data = b'C' * 4096

        # Create table and write one page
        self.storage_manager.create_table_file(table_id, table_name)
        self.storage_manager.write_page(table_id, 0, data)

        num_pages = self.storage_manager.get_num_pages(table_id)
        assert num_pages == 1

    def test_get_num_pages_multiple_pages(self):
        """Test getting number of pages for a file with multiple pages."""
        table_id = 1
        table_name = "test_table"
        num_pages_written = 3

        # Create table and write multiple pages
        self.storage_manager.create_table_file(table_id, table_name)
        for i in range(num_pages_written):
            data = bytes([i] * 4096)
            self.storage_manager.write_page(table_id, i, data)

        num_pages = self.storage_manager.get_num_pages(table_id)
        assert num_pages == num_pages_written

    def test_get_num_pages_partial_page(self):
        """Test getting number of pages when file size is not exactly divisible by page size."""
        table_id = 1
        table_name = "test_table"

        # Create table file
        file_path = self.storage_manager.create_table_file(
            table_id, table_name)

        # Write partial data directly to file (simulate partial page)
        with open(file_path, 'wb') as f:
            f.write(b'D' * 2048)  # Half a page

        num_pages = self.storage_manager.get_num_pages(table_id)
        assert num_pages == 1  # Should round up

    def test_get_num_pages_nonexistent_table_raises_error(self):
        """Test that getting number of pages for non-existent table raises StorageError."""
        nonexistent_table_id = 999

        with pytest.raises(StorageError, match=f"No file found for table {nonexistent_table_id}"):
            self.storage_manager.get_num_pages(nonexistent_table_id)

    def test_sync_all_files(self):
        """Test sync_all_files method (currently a no-op)."""
        # This method is currently a placeholder, so just verify it doesn't crash
        self.storage_manager.sync_all_files()
        # If it doesn't raise an exception, it passes

    def test_get_storage_stats(self):
        """Test getting storage statistics."""
        table_id = 1
        table_name = "test_table"
        data = b'E' * 4096

        # Create table and perform some operations
        self.storage_manager.create_table_file(table_id, table_name)
        self.storage_manager.write_page(table_id, 0, data)
        self.storage_manager.read_page(table_id, 0)

        # Get stats
        stats = self.storage_manager.get_storage_stats()

        # Verify stats structure and content (should be DiskManagerStats instance)
        assert isinstance(stats, DiskManagerStats)
        assert hasattr(stats, 'pages_written')
        assert hasattr(stats, 'pages_read')
        assert hasattr(stats, 'bytes_written')
        assert hasattr(stats, 'bytes_read')
        assert hasattr(stats, 'corruption_errors')

        # Verify some operations were recorded
        assert stats.pages_written >= 1
        assert stats.pages_read >= 1
        assert stats.bytes_written >= 4096
        assert stats.bytes_read >= 4096

    def test_integration_full_workflow(self):
        """Test a complete workflow with multiple tables and operations."""
        # Create multiple tables
        tables = [(1, "users"), (2, "products"), (3, "orders")]

        for table_id, table_name in tables:
            self.storage_manager.create_table_file(table_id, table_name)

        # Write data to each table
        for table_id, _ in tables:
            for page_num in range(3):  # 3 pages per table
                data = bytes([table_id, page_num] * 2048)  # 4096 bytes
                self.storage_manager.write_page(table_id, page_num, data)

        # Read data back and verify
        for table_id, _ in tables:
            for page_num in range(3):
                read_data = self.storage_manager.read_page(table_id, page_num)
                expected_data = bytes([table_id, page_num] * 2048)
                assert read_data == expected_data

        # Verify page counts
        for table_id, _ in tables:
            assert self.storage_manager.get_num_pages(table_id) == 3

        # Verify all files exist
        for table_id in [1, 2, 3]:
            file_path = self.storage_manager.get_table_file_path(table_id)
            assert Path(file_path).exists()

    def test_table_file_naming_convention(self):
        """Test that table files are named correctly."""
        test_cases = [
            (1, "users"),
            (42, "products_catalog"),
            (999, "special_table_with_underscores")
        ]

        for table_id, table_name in test_cases:
            file_path = self.storage_manager.create_table_file(
                table_id, table_name)
            expected_name = f"{table_name}_{table_id}.dat"

            assert Path(file_path).name == expected_name
            assert Path(file_path).parent == Path(self.temp_dir)

    def test_error_propagation_from_disk_manager(self):
        """Test that errors from DiskManager are properly propagated."""
        table_id = 1
        table_name = "test_table"

        # Create table
        self.storage_manager.create_table_file(table_id, table_name)

        # Mock DiskManager to raise an error
        with patch.object(self.storage_manager.disk_manager, 'write_page',
                          side_effect=StorageError("Disk full")):
            with pytest.raises(StorageError, match="Disk full"):
                self.storage_manager.write_page(table_id, 0, b'F' * 4096)

    def test_disk_manager_delegation(self):
        """Test that StorageManager properly delegates to DiskManager."""
        table_id = 1
        table_name = "test_table"
        page_number = 0
        data = b'G' * 4096

        # Create table
        file_path = self.storage_manager.create_table_file(
            table_id, table_name)

        # Mock DiskManager methods
        with patch.object(self.storage_manager.disk_manager, 'write_page') as mock_write, \
                patch.object(self.storage_manager.disk_manager, 'read_page') as mock_read, \
                patch.object(self.storage_manager.disk_manager, 'get_file_size') as mock_size:

            mock_read.return_value = data
            mock_size.return_value = 4096

            # Test write delegation
            self.storage_manager.write_page(table_id, page_number, data)
            mock_write.assert_called_once_with(file_path, page_number, data)

            # Test read delegation
            result = self.storage_manager.read_page(table_id, page_number)
            mock_read.assert_called_once_with(file_path, page_number)
            assert result == data

            # Test get_num_pages delegation
            num_pages = self.storage_manager.get_num_pages(table_id)
            mock_size.assert_called_once_with(file_path)
            assert num_pages == 1

    def test_concurrent_table_operations(self):
        """Test concurrent operations on different tables."""
        import threading

        num_tables = 5
        pages_per_table = 10
        results = {}
        errors = []

        def create_and_populate_table(table_id):
            try:
                table_name = f"table_{table_id}"
                self.storage_manager.create_table_file(table_id, table_name)

                for page_num in range(pages_per_table):
                    data = bytes([table_id, page_num] * 2048)
                    self.storage_manager.write_page(table_id, page_num, data)

                results[table_id] = "success"
            except Exception as e:
                errors.append(f"Table {table_id}: {e}")

        # Create threads for concurrent table operations
        threads = []
        for table_id in range(1, num_tables + 1):
            thread = threading.Thread(
                target=create_and_populate_table, args=(table_id,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == num_tables

        # Verify all tables are accessible
        for table_id in range(1, num_tables + 1):
            assert self.storage_manager.get_num_pages(
                table_id) == pages_per_table

            # Verify data integrity
            for page_num in range(pages_per_table):
                read_data = self.storage_manager.read_page(table_id, page_num)
                expected_data = bytes([table_id, page_num] * 2048)
                assert read_data == expected_data
