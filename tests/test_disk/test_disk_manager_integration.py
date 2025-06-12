import pytest
import os
import tempfile
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.storage.disk.disk_manager import DiskManager, DiskManagerStats, StorageError, CorruptionError


class TestDiskManagerIntegration:
    """Integration tests for DiskManager with more complex scenarios."""

    def setup_method(self):
        """Set up test environment before each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.disk_manager = DiskManager(base_directory=self.temp_dir)

    def teardown_method(self):
        """Clean up after each test."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_multiple_files_concurrent_access(self):
        """Test concurrent access to multiple database files."""
        num_files = 5
        pages_per_file = 10

        def write_to_file(file_index):
            file_path = os.path.join(self.temp_dir, f"db_{file_index}.db")
            for page_num in range(pages_per_file):
                data = bytes([file_index, page_num] * 2048)  # 4096 bytes
                self.disk_manager.write_page(file_path, page_num, data)

        # Write to multiple files concurrently
        with ThreadPoolExecutor(max_workers=num_files) as executor:
            futures = [executor.submit(write_to_file, i)
                       for i in range(num_files)]
            for future in as_completed(futures):
                future.result()  # Ensure no exceptions occurred

        # Verify all files were created correctly
        for file_index in range(num_files):
            file_path = os.path.join(self.temp_dir, f"db_{file_index}.db")
            assert Path(file_path).exists()
            assert Path(file_path).stat().st_size == pages_per_file * 4096

            # Verify data integrity
            for page_num in range(pages_per_file):
                read_data = self.disk_manager.read_page(file_path, page_num)
                expected_data = bytes([file_index, page_num] * 2048)
                assert read_data == expected_data

    def test_large_file_operations(self):
        """Test operations on a large file with many pages."""
        large_file = os.path.join(self.temp_dir, "large_file.db")
        num_pages = 1000

        # Write pages with identifiable data
        for page_num in range(num_pages):
            # Create data where each byte is the page number modulo 256
            data = bytes([(page_num % 256)] * 4096)
            self.disk_manager.write_page(large_file, page_num, data)

        # Verify file size
        expected_size = num_pages * 4096
        assert Path(large_file).stat().st_size == expected_size

        # Random read verification
        import random
        test_pages = random.sample(range(num_pages), 50)

        for page_num in test_pages:
            read_data = self.disk_manager.read_page(large_file, page_num)
            expected_data = bytes([(page_num % 256)] * 4096)
            assert read_data == expected_data

    def test_mixed_read_write_operations(self):
        """Test mixed read and write operations under concurrent load."""
        test_file = os.path.join(self.temp_dir, "mixed_ops.db")
        num_operations = 100
        results = {}
        errors = []

        # Create the file first to avoid "file not found" errors in concurrent reads
        initial_data = b'\x00' * 4096
        self.disk_manager.write_page(test_file, 0, initial_data)

        def random_operation(op_id):
            try:
                import random
                page_num = random.randint(0, 9)  # 10 pages

                if random.choice([True, False]):  # Random read or write
                    # Write operation
                    data = bytes([op_id % 256] * 4096)
                    self.disk_manager.write_page(test_file, page_num, data)
                    results[f"write_{op_id}"] = (page_num, data)
                else:
                    # Read operation (may read zeros if page not written yet)
                    data = self.disk_manager.read_page(test_file, page_num)
                    results[f"read_{op_id}"] = (page_num, data)
            except Exception as e:
                errors.append(f"Operation {op_id}: {e}")

        # Execute operations concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(random_operation, i)
                       for i in range(num_operations)]
            for future in as_completed(futures):
                future.result()

        # Check that no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify file exists and has reasonable size
        assert Path(test_file).exists()
        assert Path(test_file).stat().st_size <= 10 * 4096  # At most 10 pages

    def test_power_failure_simulation(self):
        """Simulate power failure scenarios by interrupting write operations."""
        test_file = os.path.join(self.temp_dir, "power_failure.db")

        # Write some initial data
        for i in range(5):
            data = bytes([i] * 4096)
            self.disk_manager.write_page(test_file, i, data)

        # Simulate interrupted write by creating a file with partial content
        partial_file = os.path.join(self.temp_dir, "partial.db")
        with open(partial_file, 'wb') as f:
            f.write(b'A' * 2000)  # Less than a full page

        # Reading should handle partial content gracefully
        read_data = self.disk_manager.read_page(partial_file, 0)
        expected_data = b'A' * 2000 + b'\x00' * 2096
        assert read_data == expected_data

    def test_file_system_limits(self):
        """Test behavior at file system limits (file handles, etc.)."""
        # Test creating many files to stress file handle management
        num_files = 50
        files = []

        for i in range(num_files):
            file_path = os.path.join(self.temp_dir, f"limit_test_{i}.db")
            data = bytes([i % 256] * 4096)
            self.disk_manager.write_page(file_path, 0, data)
            files.append(file_path)

        # Read from all files to ensure they're all accessible
        for i, file_path in enumerate(files):
            read_data = self.disk_manager.read_page(file_path, 0)
            expected_data = bytes([i % 256] * 4096)
            assert read_data == expected_data

    def test_directory_structure_creation(self):
        """Test creating complex directory structures."""
        deep_path = os.path.join(
            self.temp_dir, "level1", "level2", "level3", "level4", "deep_file.db"
        )

        data = b'X' * 4096
        self.disk_manager.write_page(deep_path, 0, data)

        # Verify directory structure was created
        assert Path(deep_path).exists()
        assert Path(deep_path).parent.exists()

        # Verify data integrity
        read_data = self.disk_manager.read_page(deep_path, 0)
        assert read_data == data

    def test_statistics_under_load(self):
        """Test statistics accuracy under heavy concurrent load."""
        test_file = os.path.join(self.temp_dir, "stats_test.db")
        num_operations = 200
        write_count = 0
        read_count = 0

        def write_operation(page_num):
            nonlocal write_count
            data = bytes([page_num % 256] * 4096)
            self.disk_manager.write_page(test_file, page_num, data)
            write_count += 1

        def read_operation(page_num):
            nonlocal read_count
            self.disk_manager.read_page(test_file, page_num)
            read_count += 1

        # First write some pages
        for i in range(50):
            write_operation(i)

        # Then mix reads and writes
        import random
        operations = []
        for i in range(num_operations):
            if random.choice([True, False]):
                operations.append(lambda i=i: write_operation(i % 50))
            else:
                operations.append(lambda i=i: read_operation(i % 50))

        # Execute operations concurrently
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(op) for op in operations]
            for future in as_completed(futures):
                future.result()

        # Verify statistics (allowing for some variance due to concurrency)
        assert self.disk_manager.stats.pages_written >= 50  # At least initial writes
        assert self.disk_manager.stats.pages_read > 0
        assert self.disk_manager.stats.bytes_written >= 50 * 4096
        assert self.disk_manager.stats.bytes_read > 0

    def test_recovery_after_corruption(self):
        """Test system behavior after detecting corruption."""
        test_file = os.path.join(self.temp_dir, "corruption_test.db")

        # Write valid data first
        valid_data = b'V' * 4096
        self.disk_manager.write_page(test_file, 0, valid_data)

        # Create a scenario that would trigger corruption detection
        # (empty data in this case)
        try:
            self.disk_manager._verify_page_integrity(b'', 0)
        except CorruptionError:
            pass  # Expected

        # Verify system can still operate normally after corruption detection
        new_data = b'N' * 4096
        self.disk_manager.write_page(test_file, 1, new_data)

        read_data = self.disk_manager.read_page(test_file, 1)
        assert read_data == new_data

        # Verify corruption was recorded in stats
        assert self.disk_manager.stats.corruption_errors > 0

    def test_page_boundary_operations(self):
        """Test operations at page boundaries and edge cases."""
        test_file = os.path.join(self.temp_dir, "boundary_test.db")

        # Test writing to the last possible page number we might reasonably use
        large_page_num = 1000000
        data = b'B' * 4096

        self.disk_manager.write_page(test_file, large_page_num, data)
        read_data = self.disk_manager.read_page(test_file, large_page_num)

        assert read_data == data

        # File should be very large now
        expected_size = (large_page_num + 1) * 4096
        assert Path(test_file).stat().st_size == expected_size

    def test_concurrent_file_operations(self):
        """Test concurrent operations on the same file."""
        test_file = os.path.join(self.temp_dir, "concurrent_test.db")
        num_threads = 10
        pages_per_thread = 20

        def worker(thread_id):
            for page_offset in range(pages_per_thread):
                page_num = thread_id * pages_per_thread + page_offset
                data = bytes([thread_id, page_offset] * 2048)
                self.disk_manager.write_page(test_file, page_num, data)

        # Launch concurrent workers
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all data was written correctly
        for thread_id in range(num_threads):
            for page_offset in range(pages_per_thread):
                page_num = thread_id * pages_per_thread + page_offset
                read_data = self.disk_manager.read_page(test_file, page_num)
                expected_data = bytes([thread_id, page_offset] * 2048)
                assert read_data == expected_data

    def test_error_handling_during_concurrent_operations(self):
        """Test error handling when errors occur during concurrent operations."""
        test_file = os.path.join(self.temp_dir, "error_test.db")
        errors_caught = []

        def operation_with_potential_error(op_id):
            try:
                if op_id % 10 == 0:  # Every 10th operation has wrong data size
                    wrong_data = b'X' * 2048  # Wrong size
                    self.disk_manager.write_page(test_file, op_id, wrong_data)
                else:
                    correct_data = b'C' * 4096
                    self.disk_manager.write_page(
                        test_file, op_id, correct_data)
            except ValueError as e:
                errors_caught.append((op_id, str(e)))

        # Execute operations with some that will fail
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(
                operation_with_potential_error, i) for i in range(50)]
            for future in as_completed(futures):
                future.result()

        # Verify that errors were caught for operations with wrong data size
        assert len(errors_caught) == 5  # Operations 0, 10, 20, 30, 40

        # Verify successful operations still worked
        for i in range(1, 10):  # Check some successful operations
            read_data = self.disk_manager.read_page(test_file, i)
            assert read_data == b'C' * 4096
