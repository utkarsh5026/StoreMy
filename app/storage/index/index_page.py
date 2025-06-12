from abc import ABC, abstractmethod
from typing import List, Optional
from ..page import Page
from ...core.types import Field, FieldType
from ...primitives import IndexPageId, TransactionId


class IndexEntry:
    """
    An entry in an index page.

    Contains a key and either:
    - RecordId (for leaf pages pointing to heap tuples)
    - PageId (for internal pages pointing to child pages)
    """

    def __init__(self, key: Field, value):
        """
        Create an index entry.

        Args:
            key: The index key
            value: Either RecordId or PageId depending on a page type
        """
        self.key = key
        self.value = value  # RecordId for leaves, PageId for internal nodes

    def __str__(self) -> str:
        return f"IndexEntry(key={self.key}, value={self.value})"

    def __repr__(self) -> str:
        return self.__str__()

class IndexPage(Page, ABC):
    """
    Abstract base class for all index pages.

    Provides common functionality:
    - Entry management
    - Binary search on keys
    - Page splitting logic
    - Serialization framework
    """

    def __init__(self, page_id: 'IndexPageId', key_type: 'FieldType'):
        """
        Create an index page.

        Args:
            page_id: Unique identifier for this page
            key_type: Type of keys stored in this index
        """
        self.page_id = page_id
        self.key_type = key_type
        self.entries: List[IndexEntry] = []
        self.is_dirty_flag = False
        self.parent_page_id: Optional['IndexPageId'] = None

    def get_id(self) -> 'IndexPageId':
        """Return the page ID."""
        return self.page_id

    def is_dirty(self) -> bool:
        """Return whether this page has been modified."""
        return self.is_dirty_flag

    def mark_dirty(self, dirty: bool, transaction_id: TransactionId) -> None:
        """Mark this page as dirty or clean."""
        self.is_dirty_flag = dirty

    def get_num_entries(self) -> int:
        """Return number of entries on this page."""
        return len(self.entries)

    def is_full(self) -> bool:
        """Check if page is full and needs splitting."""
        # Calculate if adding one more entry would exceed page size
        current_size = self._calculate_page_size()
        return current_size + self._estimate_entry_size() > self.PAGE_SIZE_IN_BYTES

    def find_key_position(self, key: Field) -> int:
        """
        Find position where key should be inserted.
        Uses binary search for efficiency.

        Returns:
            Index where key should be inserted (may equal len(entries))
        """
        left, right = 0, len(self.entries)

        while left < right:
            mid = (left + right) // 2
            if self.entries[mid].key.compare_to(key) < 0:
                left = mid + 1
            else:
                right = mid

        return left

    def search_key(self, key: Field) -> Optional[int]:
        """
        Search for exact key match.

        Returns:
            Index of key if found, None otherwise
        """
        pos = self.find_key_position(key)
        if pos < len(self.entries) and self.entries[pos].key.equals(key):
            return pos
        return None

    @abstractmethod
    def insert_entry(self, entry: IndexEntry) -> bool:
        """
        Insert an entry into this page.

        Returns:
            True if insertion successful, False if page is full
        """
        pass

    @abstractmethod
    def delete_entry(self, key: Field) -> bool:
        """
        Delete entry with given key.

        Returns:
            True if deletion successful, False if key not found
        """
        pass

    @abstractmethod
    def split_page(self) -> 'IndexPage':
        """
        Split this page into two pages.

        Returns:
            New page containing half the entries
        """
        pass

    @abstractmethod
    def _calculate_page_size(self) -> int:
        """Calculate current page size in bytes."""
        pass

    @abstractmethod
    def _estimate_entry_size(self) -> int:
        """Estimate the size of one entry in bytes."""
        pass

    def get_entries(self) -> List[IndexEntry]:
        """Return copy of entries list."""
        return self.entries.copy()

    def clear_entries(self) -> None:
        """Remove all entries from the page."""
        self.entries.clear()
        self.mark_dirty(True)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.page_id}, entries={len(self.entries)})"

    def __repr__(self) -> str:
        return self.__str__()