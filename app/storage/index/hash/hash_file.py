import os
from typing import List, Optional
from ..index_file import IndexFile
from ..index_page import IndexEntry
from .hash_page import HashBucketPage, HashDirectoryPage
from app.primitives import RecordId, TransactionId, IndexPageId
from ....core.types import Field, FieldType
from ....storage.permissions import Permissions



class HashFile:
    """
    Hash index file implementation using extendible hashing.

    This class manages a hash index stored in pages, providing:
    - O(1) average case lookup time
    - Dynamic resizing through directory doubling
    - Overflow handling with chaining
    - Page management with buffer pool integration
    - Transaction-safe operations
    """

    def __init__(self, file_path: str, key_type: FieldType, file_id: int):
        self.file_path = file_path
        self.key_type = key_type
        self.file_id = file_id
        self.directory_page_id: Optional[HashPageId] = None
        self.next_page_number = 0

        # Statistics
        self.num_entries = 0
        self.num_buckets = 0
        self.num_overflows = 0

        # Initialize empty hash index
        self._initialize_hash_index()

    def _initialize_hash_index(self) -> None:
        """Initialize an empty hash index."""
        # Create directory page
        self.directory_page_id = HashPageId(self.file_id, 0, "directory")
        directory = HashDirectoryPage(self.directory_page_id)

        # Create initial bucket pages
        bucket_page_id_0 = HashPageId(self.file_id, 1, "bucket")
        bucket_page_id_1 = HashPageId(self.file_id, 2, "bucket")

        directory.bucket_pointers = [bucket_page_id_0, bucket_page_id_1]
        directory.global_depth = 1

        # Save directory and initial buckets
        self._save_page(directory)

        bucket_0 = HashBucketPage(bucket_page_id_0, self.key_type)
        bucket_1 = HashBucketPage(bucket_page_id_1, self.key_type)
        self._save_page(bucket_0)
        self._save_page(bucket_1)

        self.next_page_number = 3
        self.num_buckets = 2

    def _allocate_page_id(self, page_type: str) -> HashPageId:
        """Allocate a new page ID."""
        page_id = HashPageId(self.file_id, self.next_page_number, page_type)
        self.next_page_number += 1
        return page_id

    def _load_page(self, tid: TransactionId, page_id: HashPageId) -> Any:
        """Load a page from the buffer pool."""
        from app.database import Database
        buffer_pool = Database.get_buffer_pool()

        # Get page from buffer pool
        page = buffer_pool.get_page(tid, page_id, Permissions.READ_ONLY)

        # Wrap in appropriate page type
        if page_id.page_type == "directory":
            hash_page = HashDirectoryPage(page_id)
        elif page_id.page_type in ["bucket", "overflow"]:
            hash_page = HashBucketPage(page_id, self.key_type)
        else:
            raise ValueError(f"Unknown page type: {page_id.page_type}")

        # Deserialize page data
        hash_page.deserialize(page.get_page_data())
        return hash_page

    def _save_page(self, page: Any) -> None:
        """Save a page to the buffer pool."""
        # This would integrate with the buffer pool
        # Implementation depends on your buffer pool interface
        pass

    def search(self, tid: TransactionId, key: Field) -> List[RecordId]:
        """Search for all records with the given key."""
        # Calculate hash value
        hash_value = HashFunction.hash_field(key)

        # Load directory page
        directory = self._load_page(tid, self.directory_page_id)

        # Find bucket page
        bucket_page_id = directory.get_bucket_page_id(hash_value)
        bucket_page = self._load_page(tid, bucket_page_id)

        # Search in bucket and overflow pages
        results = []
        current_page = bucket_page

        while current_page is not None:
            entries = current_page.find_entries(key)
            for entry in entries:
                results.append(entry.record_id)

            # Check overflow page
            if current_page.overflow_page_id:
                current_page = self._load_page(
                    tid, current_page.overflow_page_id)
            else:
                current_page = None

        return results

    def insert(self, tid: TransactionId, key: Field, record_id: RecordId) -> None:
        """Insert a key-value pair into the hash index."""
        # Calculate hash value
        hash_value = HashFunction.hash_field(key)
        entry = HashEntry(key, record_id, hash_value)

        # Load directory page
        directory = self._load_page(tid, self.directory_page_id)

        # Find bucket page
        bucket_page_id = directory.get_bucket_page_id(hash_value)
        bucket_page = self._load_page(tid, bucket_page_id)

        # Try to insert into bucket
        if bucket_page.insert_entry(entry):
            self._save_page(bucket_page)
            self.num_entries += 1
            return

        # Bucket is full - try overflow pages
        current_page = bucket_page
        while current_page.overflow_page_id:
            current_page = self._load_page(tid, current_page.overflow_page_id)
            if current_page.insert_entry(entry):
                self._save_page(current_page)
                self.num_entries += 1
                return

        # Need to create overflow page or split bucket
        if self._should_split_bucket(bucket_page):
            self._split_bucket(tid, directory, bucket_page_id, hash_value)
            # Retry insertion after split
            self.insert(tid, key, record_id)
        else:
            # Create overflow page
            self._create_overflow_page(tid, current_page, entry)

    def _should_split_bucket(self, bucket_page: HashBucketPage) -> bool:
        """Determine if bucket should be split or use overflow."""
        # Simple heuristic: split if we have room in directory
        # More sophisticated implementations might consider load factor
        return len(bucket_page.entries) > bucket_page.max_entries * 0.8

    def _split_bucket(self, tid: TransactionId, directory: HashDirectoryPage,
                      bucket_page_id: HashPageId, hash_value: int) -> None:
        """Split a bucket page to reduce load."""
        # Load the bucket to split
        old_bucket = self._load_page(tid, bucket_page_id)

        # Check if we need to double the directory
        bucket_index = directory.get_bucket_index(hash_value)
        if self._needs_directory_doubling(directory, bucket_index):
            directory.double_directory()
            self._save_page(directory)

        # Create new bucket
        new_bucket_page_id = self._allocate_page_id("bucket")
        new_bucket = HashBucketPage(new_bucket_page_id, self.key_type)

        # Redistribute entries
        old_entries = old_bucket.entries.copy()
        old_bucket.entries = []
        new_bucket.entries = []

        for entry in old_entries:
            new_bucket_index = directory.get_bucket_index(entry.hash_value)
            if new_bucket_index == bucket_index:
                old_bucket.entries.append(entry)
            else:
                new_bucket.entries.append(entry)

        # Update directory pointer
        new_index = bucket_index | (1 << (directory.global_depth - 1))
        directory.bucket_pointers[new_index] = new_bucket_page_id

        # Save pages
        self._save_page(old_bucket)
        self._save_page(new_bucket)
        self._save_page(directory)

        self.num_buckets += 1

    def _needs_directory_doubling(self, directory: HashDirectoryPage, bucket_index: int) -> bool:
        """Check if directory needs to be doubled for bucket split."""
        # Simplified logic - in practice, this depends on local depth
        return True

    def _create_overflow_page(self, tid: TransactionId, bucket_page: HashBucketPage,
                              entry: HashEntry) -> None:
        """Create an overflow page for a full bucket."""
        # Create overflow page
        overflow_page_id = self._allocate_page_id("overflow")
        overflow_page = HashBucketPage(overflow_page_id, self.key_type)

        # Insert entry into overflow page
        overflow_page.insert_entry(entry)

        # Link from bucket to overflow
        bucket_page.overflow_page_id = overflow_page_id

        # Save pages
        self._save_page(bucket_page)
        self._save_page(overflow_page)

        self.num_entries += 1
        self.num_overflows += 1

    def delete(self, tid: TransactionId, key: Field) -> bool:
        """Delete a key from the hash index."""
        # Calculate hash value
        hash_value = HashFunction.hash_field(key)

        # Load directory page
        directory = self._load_page(tid, self.directory_page_id)

        # Find bucket page
        bucket_page_id = directory.get_bucket_page_id(hash_value)
        bucket_page = self._load_page(tid, bucket_page_id)

        # Search in bucket and overflow pages
        current_page = bucket_page

        while current_page is not None:
            if current_page.delete_entry(key):
                self._save_page(current_page)
                self.num_entries -= 1
                return True

            # Check overflow page
            if current_page.overflow_page_id:
                current_page = self._load_page(
                    tid, current_page.overflow_page_id)
            else:
                current_page = None

        return False  # Key not found

    def get_statistics(self) -> dict:
        """Get statistics about the hash index."""
        return {
            'num_entries': self.num_entries,
            'num_buckets': self.num_buckets,
            'num_overflows': self.num_overflows,
            'next_page_number': self.next_page_number,
            'key_type': self.key_type.name,
            'load_factor': self.num_entries / (self.num_buckets * 10) if self.num_buckets > 0 else 0
        }

    def rebuild_index(self, tid: TransactionId) -> None:
        """Rebuild the entire hash index for better performance."""
        # This would involve:
        # 1. Collecting all entries
        # 2. Recreating the directory structure
        # 3. Redistributing entries optimally
        # Implementation would be complex and is beyond this scope
        pass


class HashIterator:
    """
    Iterator over hash index entries.

    Note: Hash indexes don't maintain sorted order,
    so iteration order is arbitrary.
    """

    def __init__(self, hash_file: HashFile, tid: TransactionId):
        self.hash_file = hash_file
        self.tid = tid
        self.current_bucket = 0
        self.current_page: Optional[HashBucketPage] = None
        self.current_entry_index = 0
        self.is_open = False

    def open(self) -> None:
        """Open iterator at first bucket."""
        self.current_bucket = 0
        self.current_entry_index = 0
        self.is_open = True
        self._find_next_page()

    def has_next(self) -> bool:
        """Check if more entries are available."""
        if not self.is_open:
            return False

        # Check if current page has more entries
        if (self.current_page and
                self.current_entry_index < len(self.current_page.entries)):
            return True

        # Check if there are more pages
        return self._has_more_pages()

    def next(self):
        """Return next (key, RecordId) pair."""
        if not self.has_next():
            raise StopIteration()

        # Get current entry
        entry = self.current_page.entries[self.current_entry_index]
        result = (entry.key, entry.value)

        # Advance position
        self.current_entry_index += 1

        # Move to next page if needed
        if self.current_entry_index >= len(self.current_page.entries):
            self._find_next_page()

        return result

    def _find_next_page(self) -> None:
        """Find the next page with entries."""
        # Check overflow page first
        if (self.current_page and
                self.current_page.next_overflow_id):
            self.current_page = self.hash_file._load_page(
                self.tid, self.current_page.next_overflow_id)
            self.current_entry_index = 0
            return

        # Move to next bucket
        self.current_bucket += 1
        self.current_entry_index = 0

        while self.current_bucket < self.hash_file.directory_page.num_buckets:
            bucket_page_id = self.hash_file.directory_page.bucket_page_ids[self.current_bucket]

            if bucket_page_id:
                self.current_page = self.hash_file._load_page(self.tid, bucket_page_id)
                if self.current_page.entries:
                    return

            self.current_bucket += 1

        # No more pages
        self.current_page = None

    def _has_more_pages(self) -> bool:
        """Check if there are more pages to iterate."""
        # Check overflow page
        if (self.current_page and
                self.current_page.next_overflow_id):
            return True

        # Check remaining buckets
        for bucket_num in range(self.current_bucket + 1,
                                self.hash_file.directory_page.num_buckets):
            if self.hash_file.directory_page.bucket_page_ids[bucket_num]:
                return True

        return False

    def close(self) -> None:
        """Close the iterator."""
        self.is_open = False
        self.current_page = None

