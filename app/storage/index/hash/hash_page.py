import struct
from typing import List, Optional
from ..index_page import IndexPage, IndexEntry
from ....primitives import RecordId, IndexPageId
from ....core.types import Field, FieldType


class HashBucketPage(IndexPage):
    """
    Hash index bucket page implementation.

    A bucket page stores key-RecordId pairs that hash to the same bucket.
    When a bucket overflows, it chains to overflow pages.

    Page Structure:
    [Header: 20 bytes]
    - Page type (4 bytes): HASH_BUCKET = 3
    - Number of entries (4 bytes)
    - Next overflow page ID (4 bytes, -1 if none)
    - Parent bucket number (4 bytes)
    - Free space pointer (4 bytes)

    [Entry Directory: 8 bytes per entry]
    - Key size (4 bytes)
    - Value offset (4 bytes)

    [Free Space]

    [Entry Data: variable size]
    - Key data
    - RecordId (8 bytes)
    """

    def set_before_image(self) -> None:
        pass

    def get_before_image(self) -> 'Page':
        pass

    HEADER_SIZE = 20
    ENTRY_DIR_SIZE = 8
    RECORD_ID_SIZE = 8

    def __init__(self, page_id: IndexPageId, key_type: FieldType, bucket_number: int):
        super().__init__(page_id, key_type)
        self.bucket_number = bucket_number
        self.next_overflow_id: Optional[IndexPageId] = None
        self.free_space_pointer = self.PAGE_SIZE

    def insert_entry(self, entry: IndexEntry) -> bool:
        """
        Insert a key-RecordId pair into this bucket page.

        Returns:
            True if insertion successful, False if page is full
        """
        if self.is_full():
            return False

        # Hash indexes allow duplicates, so just append
        self.entries.append(entry)
        self.mark_dirty(True)

        return True

    def delete_entry(self, key: Field) -> bool:
        """
        Delete first entry with given key.

        Returns:
            True if deletion successful, False if key not found
        """
        for i, entry in enumerate(self.entries):
            if entry.key.equals(key):
                del self.entries[i]
                self.mark_dirty(True)
                return True
        return False

    def delete_specific_entry(self, key: Field, record_id: RecordId) -> bool:
        """
        Delete specific key-RecordId pair.

        Returns:
            True if deletion successful, False if pair not found
        """
        for i, entry in enumerate(self.entries):
            if entry.key.equals(key) and entry.value.equals(record_id):
                del self.entries[i]
                self.mark_dirty(True)
                return True
        return False

    def find_record_ids(self, key: Field) -> List[RecordId]:
        """Find all RecordIds for the given key in this page."""
        record_ids = []

        for entry in self.entries:
            if entry.key.equals(key):
                record_ids.append(entry.value)

        return record_ids

    def split_page(self) -> 'HashBucketPage':
        """
        Hash pages don't split like B+ tree pages.
        Instead, they chain overflow pages.
        """
        # Create overflow page
        overflow_page_id = IndexPageId(
            self.page_id.index_file_id,
            self._get_next_page_number(),
            "hash"
        )
        overflow_page = HashBucketPage(overflow_page_id, self.key_type, self.bucket_number)

        # Move half the entries to overflow page
        mid_point = len(self.entries) // 2
        overflow_page.entries = self.entries[mid_point:]
        self.entries = self.entries[:mid_point]

        # Link to overflow page
        self.next_overflow_id = overflow_page_id

        # Both pages are dirty
        self.mark_dirty(True)
        overflow_page.mark_dirty(True)

        return overflow_page

    def get_page_data(self) -> bytes:
        """Serialize page to bytes."""
        data = bytearray(self.PAGE_SIZE)

        # Write header
        struct.pack_into('I', data, 0, 3)  # Page type: HASH_BUCKET
        struct.pack_into('I', data, 4, len(self.entries))
        struct.pack_into('i', data, 8, self.next_overflow_id.page_number if self.next_overflow_id else -1)
        struct.pack_into('I', data, 12, self.bucket_number)
        struct.pack_into('I', data, 16, self.free_space_pointer)

        # Write entries
        offset = self.HEADER_SIZE
        for entry in self.entries:
            key_bytes = entry.key.serialize()
            record_id_bytes = entry.value.serialize()

            # Write key size and data
            struct.pack_into('I', data, offset, len(key_bytes))
            data[offset + 4:offset + 4 + len(key_bytes)] = key_bytes
            offset += 4 + len(key_bytes)

            # Write RecordId
            data[offset:offset + len(record_id_bytes)] = record_id_bytes
            offset += len(record_id_bytes)

        return bytes(data)

    def _calculate_page_size(self) -> int:
        """Calculate current page size in bytes."""
        size = self.HEADER_SIZE

        for entry in self.entries:
            size += 4  # Key size field
            size += len(entry.key.serialize())
            size += self.RECORD_ID_SIZE

        return size

    def _estimate_entry_size(self) -> int:
        """Estimate size of one entry."""
        if self.key_type == FieldType.INT:
            return 4 + 4 + self.RECORD_ID_SIZE
        elif self.key_type == FieldType.STRING:
            return 4 + 20 + self.RECORD_ID_SIZE
        else:
            return 32

    def _get_next_page_number(self) -> int:
        """Get next available page number."""
        return self.page_id.page_number + 1000


class HashDirectoryPage(IndexPage):
    """
    Hash index directory page.

    Stores metadata about the hash table:
    - Number of buckets
    - Hash function parameters
    - Load factor statistics
    - Pointers to bucket pages

    This is the first page (page 0) of a hash index file.
    """

    def __init__(self, page_id: IndexPageId, key_type: FieldType):
        super().__init__(page_id, key_type)
        self.num_buckets = 1  # Start with 1 bucket
        self.global_depth = 0  # For extendible hashing
        self.total_entries = 0
        self.bucket_page_ids: List[Optional[IndexPageId]] = [None]

    def insert_entry(self, entry: IndexEntry) -> bool:
        """Directory pages don't store entries directly."""
        raise NotImplementedError("Directory pages don't store entries")

    def delete_entry(self, key: Field) -> bool:
        """Directory pages don't store entries directly."""
        raise NotImplementedError("Directory pages don't store entries")

    def split_page(self) -> 'IndexPage':
        """Directory pages don't split."""
        raise NotImplementedError("Directory pages don't split")

    def get_bucket_for_key(self, key: Field) -> int:
        """
        Calculate which bucket a key belongs to.

        Uses a simple hash function: hash(key) % num_buckets
        """
        # Get hash value
        key_hash = self._hash_key(key)

        # Map to bucket
        return key_hash % self.num_buckets

    def expand_hash_table(self) -> None:
        """
        Double the number of buckets and rehash.

        This is called when the load factor gets too high.
        """
        old_num_buckets = self.num_buckets
        self.num_buckets *= 2

        # Extend bucket array
        new_buckets = [None] * self.num_buckets

        # Copy old bucket pointers to even positions
        for i in range(old_num_buckets):
            new_buckets[i * 2] = self.bucket_page_ids[i]

        self.bucket_page_ids = new_buckets
        self.mark_dirty(True)

    def get_load_factor(self) -> float:
        """Calculate current load factor."""
        if self.num_buckets == 0:
            return 0.0
        return self.total_entries / self.num_buckets

    def _hash_key(self, key: Field) -> int:
        """
        Hash function for keys.

        Uses different strategies based on key type.
        """
        if key.get_type() == FieldType.INT:
            # Simple hash for integers
            return abs(hash(key.get_value()))
        elif key.get_type() == FieldType.STRING:
            # String hash using built-in hash
            return abs(hash(key.get_value()))
        else:
            # Generic hash using serialized bytes
            key_bytes = key.serialize()
            return abs(hash(key_bytes))

    def get_page_data(self) -> bytes:
        """Serialize directory page to bytes."""
        data = bytearray(self.PAGE_SIZE)

        # Write header
        struct.pack_into('I', data, 0, 4)  # Page type: HASH_DIRECTORY
        struct.pack_into('I', data, 4, self.num_buckets)
        struct.pack_into('I', data, 8, self.global_depth)
        struct.pack_into('I', data, 12, self.total_entries)

        # Write bucket page IDs
        offset = 16
        for page_id in self.bucket_page_ids:
            page_num = page_id.page_number if page_id else -1
            struct.pack_into('i', data, offset, page_num)
            offset += 4

        return bytes(data)

    def _calculate_page_size(self) -> int:
        """Calculate directory page size."""
        return 16 + (4 * len(self.bucket_page_ids))

    def _estimate_entry_size(self) -> int:
        """Directory pages don't have entries."""
        return 0
