from typing import Optional, List, Tuple
import struct

from app.core.types import Field, FieldType
from app.primitives import RecordId
from app.storage.interfaces import Page

from .primitives import BTreeEntry, BTreePageId
from .btree_page import BTreePage


class BTreeLeafPage(BTreePage):
    """Leaf page in B+ tree containing actual data records."""

    def get_before_image(self) -> 'Page':
        pass

    def set_before_image(self) -> None:
        pass

    def __init__(self, page_id: BTreePageId, key_type: FieldType):
        super().__init__(page_id, key_type)
        self.next_leaf_id: Optional[BTreePageId] = None
        self.prev_leaf_id: Optional[BTreePageId] = None

    def _calculate_max_entries(self) -> int:
        """Calculate max entries for leaf page."""
        key_size = self.key_type.get_length()
        record_id_size = 16  # Estimated RecordId size
        entry_size = 4 + key_size + record_id_size  # 4 bytes for key length
        available_space = self.PAGE_SIZE_IN_BYTES - self.HEADER_SIZE_IN_BYTES - \
            16
        return available_space // entry_size

    def insert_entry(self, key: Field, record_id: RecordId) -> bool:
        """Insert a key-value pair into the leaf page."""
        if self.is_full():
            return False

        # Find insertion point
        insert_index = self.find_key_index(key)

        # Check for duplicate key
        if (insert_index < len(self.entries) and
                self.entries[insert_index].key.equals(key)):
            # Handle duplicate - could append to list or reject
            return False

        # Insert new entry
        entry = BTreeEntry(key, record_id)
        self.entries.insert(insert_index, entry)
        return True

    def delete_entry(self, key: Field) -> bool:
        """Delete an entry with the given key."""
        for i, entry in enumerate(self.entries):
            if entry.key.equals(key):
                self.entries.pop(i)
                return True
        return False

    def find_record_ids(self, key: Field) -> List[RecordId]:
        """Find all RecordIds for a given key."""
        results = []
        for entry in self.entries:
            if entry.key.equals(key):
                results.append(entry.value)
        return results

    def find_range_record_ids(self, min_key: Optional[Field], max_key: Optional[Field],
                              include_min: bool = True, include_max: bool = True) -> List[RecordId]:
        """Find all RecordIds in a key range."""
        results = []

        for entry in self.entries:
            key = entry.key

            # Check lower bound
            if min_key is not None:
                comparison = key.compare_to(min_key)
                if comparison < 0 or (comparison == 0 and not include_min):
                    continue

            # Check upper bound
            if max_key is not None:
                comparison = key.compare_to(max_key)
                if comparison > 0 or (comparison == 0 and not include_max):
                    break

            results.append(entry.value)

        return results

    def split(self, new_page_id: BTreePageId) -> Tuple['BTreeLeafPage', Field]:
        """Split this leaf page into two pages."""
        # Create a new page
        new_page = BTreeLeafPage(new_page_id, self.key_type)

        # Calculate split point
        split_index = len(self.entries) // 2

        # Move entries to new page
        new_page.entries = self.entries[split_index:]
        self.entries = self.entries[:split_index]

        # Update sibling pointers
        new_page.next_leaf_id = self.next_leaf_id
        new_page.prev_leaf_id = self.page_id
        self.next_leaf_id = new_page.page_id

        # Update next page's prev pointer if it exists
        if new_page.next_leaf_id:
            # This would require loading the next page and updating it
            pass

        # Return new page and the key to promote
        promote_key = new_page.entries[0].key
        return new_page, promote_key

    def merge_with(self, right_page: 'BTreeLeafPage') -> None:
        """Merge this page with the right sibling page."""
        # Move all entries from right page to this page
        self.entries.extend(right_page.entries)

        # Update sibling pointers
        self.next_leaf_id = right_page.next_leaf_id

        # Update next page's prev pointer if it exists
        if self.next_leaf_id:
            # This would require loading the next page and updating it
            pass

    def serialize(self) -> bytes:
        """Serialize leaf page to bytes."""
        data = bytearray()

        # Page header
        header = struct.pack('!IIII',
                             1,  # Page type (1 = leaf)
                             len(self.entries),  # Number of entries
                             self.next_leaf_id.page_number if self.next_leaf_id else 0,
                             self.prev_leaf_id.page_number if self.prev_leaf_id else 0)
        data.extend(header)

        # Serialize entries
        for entry in self.entries:
            data.extend(entry.serialize())

        # Pad to page size
        padding_size = self.PAGE_SIZE_IN_BYTES - len(data)
        data.extend(b'\x00' * padding_size)

        return bytes(data)

    def deserialize(self, data: bytes) -> None:
        """Deserialize leaf page from bytes."""
        # Read header
        _, num_entries, next_page, prev_page = struct.unpack(
            '!IIII', data[:16])

        # Set sibling pointers
        if next_page != 0:
            self.next_leaf_id = BTreePageId(
                self.page_id.index_file_id, next_page, True)
        if prev_page != 0:
            self.prev_leaf_id = BTreePageId(
                self.page_id.index_file_id, prev_page, True)

        # Deserialize entries
        self.entries = []
        offset = 16
        for _ in range(num_entries):
            entry = BTreeEntry.deserialize(data[offset:], self.key_type, True)
            self.entries.append(entry)
            offset += len(entry.serialize())
