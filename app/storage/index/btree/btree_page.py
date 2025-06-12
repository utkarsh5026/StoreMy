import struct
from typing import List, Optional, Tuple
from ..index_page import IndexPage, IndexEntry
from ....primitives import IndexPageId, RecordId
from ....core.types import Field, FieldType


class BTreeLeafPage(IndexPage):
    """
    B+ Tree leaf page implementation.

    Leaf pages store key-RecordId pairs and form the bottom level of the B+ tree.
    All data records are referenced from leaf pages.

    Page Structure:
    [Header: 24 bytes]
    - Page type (4 bytes): LEAF = 1
    - Number of entries (4 bytes)
    - Next leaf page ID (4 bytes, -1 if none)
    - Previous leaf page ID (4 bytes, -1 if none)
    - Parent page ID (4 bytes, -1 if root)
    - Free space pointer (4 bytes)

    [Entry Directory: 8 bytes per entry]
    - Key size (4 bytes)
    - Value offset (4 bytes)

    [Free Space]

    [Entry Data: variable size]
    - Key data
    - RecordId (8 bytes: page_id + tuple_id)
    """

    HEADER_SIZE = 24
    ENTRY_DIR_SIZE = 8  # Size of each directory entry
    RECORD_ID_SIZE = 8  # RecordId serialization size

    def __init__(self, page_id: IndexPageId, key_type: FieldType):
        super().__init__(page_id, key_type)
        self.next_leaf_id: Optional[IndexPageId] = None
        self.prev_leaf_id: Optional[IndexPageId] = None
        self.free_space_pointer = self.PAGE_SIZE_IN_BYTES  # Grows downward

    def insert_entry(self, entry: IndexEntry) -> bool:
        """
        Insert a key-RecordId pair into this leaf page.

        Returns:
            True if insertion successful, False if page is full
        """
        if self.is_full():
            return False

        # Find insertion position
        pos = self.find_key_position(entry.key)

        # Insert into entries list
        self.entries.insert(pos, entry)
        self.mark_dirty(True)

        return True

    def delete_entry(self, key: Field) -> bool:
        """
        Delete entry with given key.

        Returns:
            True if deletion successful, False if key not found
        """
        key_pos = self.search_key(key)
        if key_pos is None:
            return False

        del self.entries[key_pos]
        self.mark_dirty(True)
        return True

    def split_page(self) -> 'BTreeLeafPage':
        """
        Split this leaf page into two pages.

        Returns:
            New leaf page containing the right half of entries
        """
        # Create new page
        new_page_id = IndexPageId(
            self.page_id.index_file_id,
            self._get_next_page_number(),  # Implementation detail
            "btree"
        )
        new_page = BTreeLeafPage(new_page_id, self.key_type)

        # Split entries roughly in half
        mid_point = len(self.entries) // 2
        new_page.entries = self.entries[mid_point:]
        self.entries = self.entries[:mid_point]

        # Update leaf page pointers
        new_page.next_leaf_id = self.next_leaf_id
        new_page.prev_leaf_id = self.page_id

        if self.next_leaf_id:
            # Update old next page to point back to new page
            # This would require loading the next page - implementation detail
            pass

        self.next_leaf_id = new_page_id

        # Both pages are dirty
        self.mark_dirty(True)
        new_page.mark_dirty(True)

        return new_page

    def find_record_ids(self, key: Field) -> List[RecordId]:
        """
        Find all RecordIds for the given key.

        Since this is a leaf page, we search locally.
        """
        record_ids = []

        # Find all entries with matching key (handles duplicates)
        for entry in self.entries:
            if entry.key.equals(key):
                record_ids.append(entry.value)

        return record_ids

    def find_range_record_ids(self, min_key: Optional[Field], max_key: Optional[Field],
                              include_min: bool = True, include_max: bool = True) -> List[RecordId]:
        """
        Find all RecordIds in the given key range.
        """
        record_ids = []

        for entry in self.entries:
            key = entry.key

            # Check lower bound
            if min_key is not None:
                if include_min:
                    if key.compare_to(min_key) < 0:
                        continue
                else:
                    if key.compare_to(min_key) <= 0:
                        continue

            # Check upper bound
            if max_key is not None:
                if include_max:
                    if key.compare_to(max_key) > 0:
                        break  # Since entries are sorted, we can break
                else:
                    if key.compare_to(max_key) >= 0:
                        break

            record_ids.append(entry.value)

        return record_ids

    def get_page_data(self) -> bytes:
        """Serialize page to bytes."""
        data = bytearray(self.PAGE_SIZE_IN_BYTES)

        # Write header
        struct.pack_into('I', data, 0, 1)  # Page type: LEAF
        struct.pack_into('I', data, 4, len(self.entries))
        struct.pack_into('i', data, 8, self.next_leaf_id.page_number if self.next_leaf_id else -1)
        struct.pack_into('i', data, 12, self.prev_leaf_id.page_number if self.prev_leaf_id else -1)
        struct.pack_into('i', data, 16, self.parent_page_id.page_number if self.parent_page_id else -1)
        struct.pack_into('I', data, 20, self.free_space_pointer)

        # Write entries (simplified serialization)
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
            return 4 + 4 + self.RECORD_ID_SIZE  # size + int + RecordId
        elif self.key_type == FieldType.STRING:
            return 4 + 20 + self.RECORD_ID_SIZE  # size + avg string + RecordId
        else:
            return 32  # Conservative estimate

    def _get_next_page_number(self) -> int:
        """Get next available page number - implementation detail."""
        # This would be managed by the BTreeFile
        return self.page_id.page_number + 1000  # Placeholder


class BTreeInternalPage(IndexPage):
    """
    B+ Tree internal page implementation.

    Internal pages store key-PageId pairs and guide searches to the correct leaf pages.
    They do not contain actual data records.

    Page Structure:
    [Header: 20 bytes]
    - Page type (4 bytes): INTERNAL = 2
    - Number of entries (4 bytes)
    - Parent page ID (4 bytes, -1 if root)
    - First child page ID (4 bytes)
    - Free space pointer (4 bytes)

    [Entry Directory: variable size]
    - Key data + Child page ID pairs

    Key Invariant: For internal pages with keys [k1, k2, ..., kn]:
    - First child contains keys < k1
    - Second child contains keys >= k1 and < k2
    - Last child contains keys >= kn
    """

    HEADER_SIZE = 20
    PAGE_ID_SIZE = 4

    def __init__(self, page_id: IndexPageId, key_type: FieldType):
        super().__init__(page_id, key_type)
        self.first_child_id: Optional[IndexPageId] = None

    def insert_entry(self, entry: IndexEntry) -> bool:
        """
        Insert a key-PageId pair into this internal page.

        Returns:
            True if insertion successful, False if page is full
        """
        if self.is_full():
            return False

        # Find insertion position
        pos = self.find_key_position(entry.key)

        # Insert into entries list
        self.entries.insert(pos, entry)
        self.mark_dirty(True)

        return True

    def delete_entry(self, key: Field) -> bool:
        """Delete entry with given key."""
        key_pos = self.search_key(key)
        if key_pos is None:
            return False

        del self.entries[key_pos]
        self.mark_dirty(True)
        return True

    def split_page(self) -> 'BTreeInternalPage':
        """
        Split this internal page into two pages.

        Returns:
            New internal page containing the right half of entries
        """
        # Create new page
        new_page_id = IndexPageId(
            self.page_id.index_file_id,
            self._get_next_page_number(),
            "btree"
        )
        new_page = BTreeInternalPage(new_page_id, self.key_type)

        # Split entries - middle key gets promoted to parent
        mid_point = len(self.entries) // 2
        middle_key = self.entries[mid_point].key

        # Right page gets entries after middle
        new_page.entries = self.entries[mid_point + 1:]
        new_page.first_child_id = self.entries[mid_point].value  # Middle key's child

        # Left page keeps entries before middle
        self.entries = self.entries[:mid_point]

        # Both pages are dirty
        self.mark_dirty(True)
        new_page.mark_dirty(True)

        return new_page

    def find_child_page(self, key: Field) -> IndexPageId:
        """
        Find which child page should contain the given key.

        Returns:
            PageId of the child page to search
        """
        # If no entries, return first child
        if not self.entries:
            return self.first_child_id

        # Search for appropriate child
        for i, entry in enumerate(self.entries):
            if key.compare_to(entry.key) < 0:
                # Key is less than this entry's key
                if i == 0:
                    return self.first_child_id
                else:
                    return self.entries[i - 1].value

        # Key is >= all entries, use last child
        return self.entries[-1].value

    def get_page_data(self) -> bytes:
        """Serialize page to bytes."""
        data = bytearray(self.PAGE_SIZE)

        # Write header
        struct.pack_into('I', data, 0, 2)  # Page type: INTERNAL
        struct.pack_into('I', data, 4, len(self.entries))
        struct.pack_into('i', data, 8, self.parent_page_id.page_number if self.parent_page_id else -1)
        struct.pack_into('i', data, 12, self.first_child_id.page_number if self.first_child_id else -1)
        struct.pack_into('I', data, 16, self.free_space_pointer)

        # Write entries
        offset = self.HEADER_SIZE
        for entry in self.entries:
            key_bytes = entry.key.serialize()

            # Write key size and data
            struct.pack_into('I', data, offset, len(key_bytes))
            data[offset + 4:offset + 4 + len(key_bytes)] = key_bytes
            offset += 4 + len(key_bytes)

            # Write child page ID
            struct.pack_into('i', data, offset, entry.value.page_number)
            offset += self.PAGE_ID_SIZE

        return bytes(data)

    def _calculate_page_size(self) -> int:
        """Calculate current page size in bytes."""
        size = self.HEADER_SIZE

        for entry in self.entries:
            size += 4  # Key size field
            size += len(entry.key.serialize())
            size += self.PAGE_ID_SIZE

        return size

    def _estimate_entry_size(self) -> int:
        """Estimate size of one entry."""
        if self.key_type == FieldType.INT:
            return 4 + 4 + self.PAGE_ID_SIZE  # size + int + PageId
        elif self.key_type == FieldType.STRING:
            return 4 + 20 + self.PAGE_ID_SIZE  # size + avg string + PageId
        else:
            return 32  # Conservative estimate

    def _get_next_page_number(self) -> int:
        """Get next available page number - implementation detail."""
        return self.page_id.page_number + 1000  # Placeholder

