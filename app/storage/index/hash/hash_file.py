import os
from typing import List, Optional
from ..index_file import IndexFile
from ..index_page import IndexEntry
from .hash_page import HashBucketPage, HashDirectoryPage
from app.primitives import RecordId, TransactionId, IndexPageId
from ....core.types import Field, FieldType
from ....storage.permissions import Permissions


class HashFile(IndexFile):
    """
    Hash index file implementation.

    Features:
    - Fast exact key lookups (O(1) average case)
    - No range queries (keys not sorted)
    - Dynamic resizing based on load factor
    - Overflow handling via page chaining
    - Good for equality-based queries

    File Structure:
    - Page 0: Directory page with metadata
    - Page 1+: Bucket pages and overflow pages

    Hash Function Strategy:
    - Simple modulo hashing: hash(key) % num_buckets
    - Dynamic expansion when load factor > threshold
    - Rehashing on expansion
    """

    LOAD_FACTOR_THRESHOLD = 0.75  # Expand when load factor exceeds this

    def __init__(self, file_path: str, key_type: FieldType):
        super().__init__(file_path, key_type)
        self.directory_page: Optional[HashDirectoryPage] = None
        self.next_page_number = 1  # Page 0 is directory

        # Create file if it doesn't exist
        if not os.path.exists(file_path):
            self._create_empty_hash_table()
        else:
            self._load_metadata()

    def find_equal(self, tid: TransactionId, key: Field) -> List[RecordId]:
        """Find all RecordIds with the given key value."""
        if self.directory_page is None:
            return []

        record_ids = []

        # Find bucket for key
        bucket_num = self.directory_page.get_bucket_for_key(key)
        bucket_page_id = self.directory_page.bucket_page_ids[bucket_num]

        if bucket_page_id is None:
            return []

        # Search bucket and overflow pages
        current_page = self._load_page(tid, bucket_page_id)

        while current_page is not None:
            # Search current page
            page_records = current_page.find_record_ids(key)
            record_ids.extend(page_records)

            # Move to overflow page if exists
            if current_page.next_overflow_id:
                current_page = self._load_page(tid, current_page.next_overflow_id)
            else:
                break

        return record_ids

    def find_range(self, tid: TransactionId,
                   min_key: Optional[Field], max_key: Optional[Field],
                   include_min: bool = True, include_max: bool = True) -> List[RecordId]:
        """
        Hash indexes don't support range queries efficiently.

        This method scans all buckets, which is O(n) - not recommended.
        """
        if self.directory_page is None:
            return []

        record_ids = []

        # Scan all buckets (inefficient!)
        for bucket_num in range(self.directory_page.num_buckets):
            bucket_page_id = self.directory_page.bucket_page_ids[bucket_num]

            if bucket_page_id is None:
                continue

            # Scan bucket and overflow pages
            current_page = self._load_page(tid, bucket_page_id)

            while current_page is not None:
                # Check each entry in page
                for entry in current_page.entries:
                    key = entry.key

                    # Check if key is in range
                    in_range = True

                    # Check lower bound
                    if min_key is not None:
                        if include_min:
                            if key.compare_to(min_key) < 0:
                                in_range = False
                        else:
                            if key.compare_to(min_key) <= 0:
                                in_range = False

                    # Check upper bound
                    if max_key is not None and in_range:
                        if include_max:
                            if key.compare_to(max_key) > 0:
                                in_range = False
                        else:
                            if key.compare_to(max_key) >= 0:
                                in_range = False

                    if in_range:
                        record_ids.append(entry.value)

                # Move to overflow page
                if current_page.next_overflow_id:
                    current_page = self._load_page(tid, current_page.next_overflow_id)
                else:
                    break

        return record_ids

    def insert_entry(self, tid: TransactionId, key: Field, record_id: RecordId) -> None:
        """Insert a new key-RecordId pair."""
        if self.directory_page is None:
            self._create_empty_hash_table()

        # Create new entry
        entry = IndexEntry(key, record_id)

        # Find bucket
        bucket_num = self.directory_page.get_bucket_for_key(key)

        # Ensure bucket page exists
        if self.directory_page.bucket_page_ids[bucket_num] is None:
            self._create_bucket_page(tid, bucket_num)

        # Insert into bucket
        success = self._insert_into_bucket(tid, bucket_num, entry)

        if success:
            # Update statistics
            self.directory_page.total_entries += 1
            self._save_page(tid, self.directory_page)

            # Check if we need to expand
            if self.directory_page.get_load_factor() > self.LOAD_FACTOR_THRESHOLD:
                self._expand_hash_table(tid)

    def delete_entry(self, tid: TransactionId, key: Field, record_id: RecordId) -> None:
        """Delete a key-RecordId pair."""
        if self.directory_page is None:
            return

        # Find bucket
        bucket_num = self.directory_page.get_bucket_for_key(key)
        bucket_page_id = self.directory_page.bucket_page_ids[bucket_num]

        if bucket_page_id is None:
            return

        # Search bucket and overflow pages for specific entry
        current_page = self._load_page(tid, bucket_page_id)

        while current_page is not None:
            if current_page.delete_specific_entry(key, record_id):
                self._save_page(tid, current_page)

                # Update statistics
                self.directory_page.total_entries -= 1
                self._save_page(tid, self.directory_page)
                return

            # Move to overflow page
            if current_page.next_overflow_id:
                current_page = self._load_page(tid, current_page.next_overflow_id)
            else:
                break

    def _insert_into_bucket(self, tid: TransactionId, bucket_num: int, entry) -> bool:
        """Insert entry into specified bucket, handling overflow."""
        bucket_page_id = self.directory_page.bucket_page_ids[bucket_num]
        current_page = self._load_page(tid, bucket_page_id)

        # Try to insert into current page
        if current_page.insert_entry(entry):
            self._save_page(tid, current_page)
            return True

        # Page is full, follow overflow chain
        while current_page.next_overflow_id is not None:
            current_page = self._load_page(tid, current_page.next_overflow_id)

            if current_page.insert_entry(entry):
                self._save_page(tid, current_page)
                return True

        # Create new overflow page
        overflow_page = current_page.split_page()
        self._save_page(tid, current_page)
        self._save_page(tid, overflow_page)

        # Try inserting into new overflow page
        if overflow_page.insert_entry(entry):
            self._save_page(tid, overflow_page)
            return True

        return False

    def _create_bucket_page(self, tid: TransactionId, bucket_num: int) -> None:
        """Create a new bucket page."""
        page_id = self._allocate_page_id()
        bucket_page = HashBucketPage(page_id, self.key_type, bucket_num)

        # Update directory
        self.directory_page.bucket_page_ids[bucket_num] = page_id

        # Save pages
        self._save_page(tid, bucket_page)
        self._save_page(tid, self.directory_page)

    def _expand_hash_table(self, tid: TransactionId) -> None:
        """
        Expand hash table by doubling bucket count.

        This requires rehashing all existing entries.
        """
        print(f"Expanding hash table from {self.directory_page.num_buckets} buckets")

        # Save old bucket information
        old_buckets = self.directory_page.bucket_page_ids.copy()
        old_num_buckets = self.directory_page.num_buckets

        # Expand directory
        self.directory_page.expand_hash_table()

        # Rehash entries from old buckets to new buckets
        for old_bucket_num in range(old_num_buckets):
            old_bucket_page_id = old_buckets[old_bucket_num]

            if old_bucket_page_id is None:
                continue

            # Collect all entries from old bucket and its overflows
            entries_to_rehash = []
            current_page = self._load_page(tid, old_bucket_page_id)

            while current_page is not None:
                entries_to_rehash.extend(current_page.entries)

                if current_page.next_overflow_id:
                    current_page = self._load_page(tid, current_page.next_overflow_id)
                else:
                    break

            # Clear old bucket (reuse the page for new bucket)
            old_bucket_page = self._load_page(tid, old_bucket_page_id)
            old_bucket_page.clear_entries()
            old_bucket_page.next_overflow_id = None

            # Assign old bucket page to even-numbered new bucket
            new_bucket_num = old_bucket_num * 2
            self.directory_page.bucket_page_ids[new_bucket_num] = old_bucket_page_id

            # Rehash all entries
            for entry in entries_to_rehash:
                new_bucket_num = self.directory_page.get_bucket_for_key(entry.key)

                # Ensure bucket exists
                if self.directory_page.bucket_page_ids[new_bucket_num] is None:
                    self._create_bucket_page(tid, new_bucket_num)

                # Insert into new bucket (don't count toward total - already counted)
                self._insert_into_bucket(tid, new_bucket_num, entry)

        # Save directory
        self._save_page(tid, self.directory_page)

        print(f"Hash table expanded to {self.directory_page.num_buckets} buckets")

    def _create_empty_hash_table(self) -> None:
        """Create an empty hash table with directory page."""
        # Create directory page
        dir_page_id = IndexPageId(self.get_id(), 0, "hash")
        self.directory_page = HashDirectoryPage(dir_page_id, self.key_type)

        # Save metadata
        self._save_metadata()

    def _load_metadata(self) -> None:
        """Load hash table metadata from file."""
        # Load directory page
        dir_page_id = IndexPageId(self.get_id(), 0, "hash")
        # This would load the actual directory page from disk
        self.directory_page = HashDirectoryPage(dir_page_id, self.key_type)

    def _save_metadata(self) -> None:
        """Save hash table metadata to file."""
        # Directory page contains all metadata
        pass

    def _allocate_page_id(self) -> IndexPageId:
        """Allocate a new page ID."""
        page_id = IndexPageId(self.get_id(), self.next_page_number, "hash")
        self.next_page_number += 1
        return page_id

    def _load_page(self, tid: TransactionId, page_id: IndexPageId):
        """Load a page from buffer pool."""
        from app.database import Database
        buffer_pool = Database.get_buffer_pool()
        return buffer_pool.get_page(tid, page_id, Permissions.READ_ONLY)

    def _save_page(self, tid: TransactionId, page) -> None:
        """Save a page back to buffer pool."""
        page.mark_dirty(True)

    # Inherited methods from DbFile
    def get_tuple_desc(self):
        """Indexes don't have tuple descriptors."""
        raise NotImplementedError("Indexes don't have tuple descriptors")

    def iterator(self, tid: TransactionId):
        """Return iterator over all key-RecordId pairs (unordered)."""
        return HashIterator(self, tid)


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

