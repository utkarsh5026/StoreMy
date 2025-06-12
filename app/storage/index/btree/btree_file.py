import os
from typing import List, Optional
from .index_file import IndexFile
from .btree_page import BTreeLeafPage, BTreeInternalPage
from .index_page_id import IndexPageId
from ...core.tuple import RecordId
from ...core.types import Field, FieldType
from ...concurrency.transactions import TransactionId
from ...storage.permissions import Permissions



class BTreeFile(IndexFile):
    """
    B+ Tree index file implementation.

    Features:
    - Sorted key access
    - Range queries
    - Logarithmic search time
    - Sequential access via leaf page links
    - Page-based storage with buffer pool integration

    B+ Tree Properties:
    - All data in leaf pages
    - Internal pages only guide searches
    - Leaf pages linked for sequential access
    - Balanced tree structure
    - High fan-out for disk efficiency
    """

    def __init__(self, file_path: str, key_type: FieldType):
        super().__init__(file_path, key_type)
        self.root_page_id: Optional[IndexPageId] = None
        self.next_page_number = 0

        # Create file if it doesn't exist
        if not os.path.exists(file_path):
            self._create_empty_btree()
        else:
            self._load_metadata()

    def find_equal(self, tid: TransactionId, key: Field) -> List[RecordId]:
        """Find all RecordIds with the given key value."""
        if self.root_page_id is None:
            return []

        # Navigate to leaf page
        leaf_page = self._find_leaf_page(tid, key)

        # Search within leaf page
        return leaf_page.find_record_ids(key)

    def find_range(self, tid: TransactionId,
                   min_key: Optional[Field], max_key: Optional[Field],
                   include_min: bool = True, include_max: bool = True) -> List[RecordId]:
        """Find all RecordIds with keys in the given range."""
        if self.root_page_id is None:
            return []

        record_ids = []

        # Find starting leaf page
        if min_key is not None:
            current_leaf = self._find_leaf_page(tid, min_key)
        else:
            # Start from leftmost leaf
            current_leaf = self._find_leftmost_leaf(tid)

        # Scan leaf pages until we exceed max_key
        while current_leaf is not None:
            # Get records from current leaf
            leaf_records = current_leaf.find_range_record_ids(
                min_key, max_key, include_min, include_max
            )
            record_ids.extend(leaf_records)

            # Check if we've gone past max_key
            if max_key is not None and current_leaf.entries:
                last_key = current_leaf.entries[-1].key
                if last_key.compare_to(max_key) >= 0:
                    break

            # Move to next leaf page
            if current_leaf.next_leaf_id:
                current_leaf = self._load_page(tid, current_leaf.next_leaf_id)
            else:
                break

        return record_ids

    def insert_entry(self, tid: TransactionId, key: Field, record_id: RecordId) -> None:
        """Insert a new key-RecordId pair."""
        if self.root_page_id is None:
            self._create_empty_btree()

        # Create new entry
        from .index_page import IndexEntry
        entry = IndexEntry(key, record_id)

        # Insert starting from root
        self._insert_recursive(tid, self.root_page_id, entry)

    def delete_entry(self, tid: TransactionId, key: Field, record_id: RecordId) -> None:
        """Delete a key-RecordId pair."""
        if self.root_page_id is None:
            return

        # Find leaf page containing the key
        leaf_page = self._find_leaf_page(tid, key)

        # Delete from leaf page
        success = leaf_page.delete_entry(key)
        if success:
            self._save_page(tid, leaf_page)

            # Check if page is too empty and needs merging
            # This is a complex operation for full B+ tree implementation
            # For now, just delete the entry

    def _find_leaf_page(self, tid: TransactionId, key: Field) -> BTreeLeafPage:
        """Navigate from root to leaf page that should contain the key."""
        current_page_id = self.root_page_id

        while True:
            page = self._load_page(tid, current_page_id)

            if isinstance(page, BTreeLeafPage):
                return page
            elif isinstance(page, BTreeInternalPage):
                # Find child page to follow
                current_page_id = page.find_child_page(key)
            else:
                raise ValueError(f"Unknown page type: {type(page)}")

    def _find_leftmost_leaf(self, tid: TransactionId) -> BTreeLeafPage:
        """Find the leftmost (smallest key) leaf page."""
        current_page_id = self.root_page_id

        while True:
            page = self._load_page(tid, current_page_id)

            if isinstance(page, BTreeLeafPage):
                return page
            elif isinstance(page, BTreeInternalPage):
                # Always go to first child
                current_page_id = page.first_child_id
            else:
                raise ValueError(f"Unknown page type: {type(page)}")

    def _insert_recursive(self, tid: TransactionId, page_id: IndexPageId,
                          entry) -> Optional:
        """
        Recursive insert implementation.

        Returns:
            None if no split, or (key, new_page_id) if page split occurred
        """
        page = self._load_page(tid, page_id)

        if isinstance(page, BTreeLeafPage):
            # Insert into leaf page
            if page.insert_entry(entry):
                self._save_page(tid, page)
                return None  # No split needed
            else:
                # Page is full, split it
                new_page = page.split_page()
                self._save_page(tid, page)
                self._save_page(tid, new_page)

                # Return middle key to promote to parent
                middle_key = new_page.entries[0].key
                return (middle_key, new_page.page_id)

        elif isinstance(page, BTreeInternalPage):
            # Find child to insert into
            child_page_id = page.find_child_page(entry.key)

            # Recursive call
            split_result = self._insert_recursive(tid, child_page_id, entry)

            if split_result is None:
                return None  # No split in child

            # Child page split, need to insert new key/page into this page
            promoted_key, new_child_id = split_result
            from .index_page import IndexEntry
            new_entry = IndexEntry(promoted_key, new_child_id)

            if page.insert_entry(new_entry):
                self._save_page(tid, page)
                return None  # No split needed
            else:
                # This page also needs to split
                new_page = page.split_page()
                self._save_page(tid, page)
                self._save_page(tid, new_page)

                # Promote middle key further up
                middle_key = new_page.entries[0].key if new_page.entries else promoted_key
                return (middle_key, new_page.page_id)

    def _load_page(self, tid: TransactionId, page_id: IndexPageId):
        """Load a page from buffer pool."""
        # This would integrate with the buffer pool
        # For now, simplified implementation
        from app.database import Database
        buffer_pool = Database.get_buffer_pool()

        page = buffer_pool.get_page(tid, page_id, Permissions.READ_ONLY)
        return page

    def _save_page(self, tid: TransactionId, page) -> None:
        """Save a page back to buffer pool."""
        # Page is automatically saved when marked dirty
        page.mark_dirty(True)

    def _create_empty_btree(self) -> None:
        """Create an empty B+ tree with a single leaf page as root."""
        # Create root page (initially a leaf)
        root_id = IndexPageId(self.get_id(), 0, "btree")
        root_page = BTreeLeafPage(root_id, self.key_type)

        self.root_page_id = root_id
        self.next_page_number = 1

        # Save metadata and root page
        self._save_metadata()

    def _load_metadata(self) -> None:
        """Load B+ tree metadata from file."""
        # This would read metadata from the beginning of the file
        # For now, simplified implementation
        self.root_page_id = IndexPageId(self.get_id(), 0, "btree")
        self.next_page_number = 1

    def _save_metadata(self) -> None:
        """Save B+ tree metadata to file."""
        # This would write metadata to the beginning of the file
        # Implementation depends on file format details
        pass

    def _allocate_page_id(self) -> IndexPageId:
        """Allocate a new page ID."""
        page_id = IndexPageId(self.get_id(), self.next_page_number, "btree")
        self.next_page_number += 1
        return page_id

    # Inherited methods from DbFile
    def get_tuple_desc(self):
        """Indexes don't have tuple descriptors."""
        raise NotImplementedError("Indexes don't have tuple descriptors")

    def iterator(self, tid: TransactionId):
        """Return iterator over all key-RecordId pairs in sorted order."""
        return BTreeIterator(self, tid)



class BTreeIterator:
    """
    Iterator over B+ tree entries in sorted key order.

    Uses the linked list of leaf pages for efficient sequential access.
    """

    def __init__(self, btree_file: BTreeFile, tid: TransactionId):
        self.btree_file = btree_file
        self.tid = tid
        self.current_leaf: Optional[BTreeLeafPage] = None
        self.current_entry_index = 0
        self.is_open = False

    def open(self) -> None:
        """Open iterator at leftmost leaf page."""
        if self.btree_file.root_page_id is None:
            self.current_leaf = None
        else:
            self.current_leaf = self.btree_file._find_leftmost_leaf(self.tid)

        self.current_entry_index = 0
        self.is_open = True

    def has_next(self) -> bool:
        """Check if more entries are available."""
        if not self.is_open:
            return False

        if self.current_leaf is None:
            return False

        # Check current page
        if self.current_entry_index < len(self.current_leaf.entries):
            return True

        # Check next page
        return self.current_leaf.next_leaf_id is not None

    def next(self):
        """Return next (key, RecordId) pair."""
        if not self.has_next():
            raise StopIteration()

        # Get current entry
        entry = self.current_leaf.entries[self.current_entry_index]
        result = (entry.key, entry.value)

        # Advance position
        self.current_entry_index += 1

        # Move to next page if needed
        if (self.current_entry_index >= len(self.current_leaf.entries) and
                self.current_leaf.next_leaf_id is not None):
            self.current_leaf = self.btree_file._load_page(self.tid, self.current_leaf.next_leaf_id)
            self.current_entry_index = 0

        return result

    def close(self) -> None:
        """Close the iterator."""
        self.is_open = False
        self.current_leaf = None

