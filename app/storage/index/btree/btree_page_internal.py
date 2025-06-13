from typing import Optional, List, Tuple
import struct

from app.core.types import Field, FieldType
from app.primitives import RecordId
from app.storage.interfaces import Page

from .primitives import BTreeEntry, BTreePageId
from .btree_page import BTreePage


class BTreeInternalPage(BTreePage):
    """Internal page in B+ tree containing keys and child pointers."""

    def _calculate_max_entries(self) -> int:
        """Calculate max entries for internal page."""
        key_size = self.key_type.get_length()
        page_id_size = 16  # Estimated BTreePageId size
        entry_size = 4 + key_size + page_id_size  # 4 bytes for key length
        available_space = self.PAGE_SIZE_IN_BYTES - self.HEADER_SIZE_IN_BYTES
        return available_space // entry_size

    def insert_entry(self, key: Field, child_page_id: BTreePageId) -> bool:
        """Insert a key and child pointer into the internal page."""
        if self.is_full():
            return False

        # Find insertion point
        insert_index = self.find_key_index(key)

        # Insert new entry
        entry = BTreeEntry(key, child_page_id)
        self.entries.insert(insert_index, entry)
        return True

    def find_child_page_id(self, key: Field) -> BTreePageId:
        """Find the child page that should contain the given key."""
        for entry in self.entries:
            if key.compare_to(entry.key) < 0:
                return entry.value

        # Key is greater than all entries - use last child
        if self.entries:
            return self.entries[-1].value

        # No entries - shouldn't happen in valid tree
        raise ValueError("Internal page has no entries")

    def split(self, new_page_id: BTreePageId) -> Tuple['BTreeInternalPage', Field]:
        """Split this internal page into two pages."""
        # Create new page
        new_page = BTreeInternalPage(new_page_id, self.key_type)

        # Calculate split point
        split_index = len(self.entries) // 2

        # The middle key will be promoted
        promote_key = self.entries[split_index].key

        # Move entries to new page (excluding the promoted key)
        new_page.entries = self.entries[split_index + 1:]
        self.entries = self.entries[:split_index]

        return new_page, promote_key

    def merge_with(self, right_page: 'BTreeInternalPage', separator_key: Field) -> None:
        """Merge this page with the right sibling page."""
        # Add separator key as an entry
        separator_entry = BTreeEntry(
            separator_key, self.entries[-1].value if self.entries else None)

        # Move all entries from right page to this page
        self.entries.append(separator_entry)
        self.entries.extend(right_page.entries)

    def serialize(self) -> bytes:
        """Serialize internal page to bytes."""
        data = bytearray()

        # Page header
        header = struct.pack('!II',
                             0,  # Page type (0 = internal)
                             len(self.entries))  # Number of entries
        data.extend(header)

        # Serialize entries
        for entry in self.entries:
            data.extend(entry.serialize())

        # Pad to page size
        padding_size = self.PAGE_SIZE_IN_BYTES - len(data)
        data.extend(b'\x00' * padding_size)

        return bytes(data)

    def deserialize(self, data: bytes) -> None:
        """Deserialize internal page from bytes."""
        # Read header
        page_type, num_entries = struct.unpack('!II', data[:8])

        # Deserialize entries
        self.entries = []
        offset = 8
        for _ in range(num_entries):
            entry = BTreeEntry.deserialize(data[offset:], self.key_type, False)
            self.entries.append(entry)
            offset += len(entry.serialize())
