from typing import Optional
from abc import abstractmethod

from app.primitives import TransactionId
from app.core.types import Field, FieldType
from app.storage.interfaces import Page

from .primitives import  BTreeEntry, BTreePageId


class BTreePage(Page):
    """Abstract base class for B+ tree pages."""

    PAGE_SIZE_IN_BYTES = 4096
    HEADER_SIZE_IN_BYTES = 32

    def __init__(self, page_id: BTreePageId, key_type: FieldType):
        self.page_id = page_id
        self.key_type = key_type
        self.entries: list[BTreeEntry] = []
        self.is_dirty = False
        self.dirty_transaction: Optional[TransactionId] = None
        self.max_entries = self._calculate_max_entries()

    @abstractmethod
    def _calculate_max_entries(self) -> int:
        """Calculate the maximum number of entries for this page type."""
        pass

    @abstractmethod
    def serialize(self) -> bytes:
        """Serialize page to bytes for disk storage."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> None:
        """Deserialize page from bytes."""
        pass

    def get_id(self) -> BTreePageId:
        return self.page_id

    def is_dirty(self) -> bool:
        return self.is_dirty

    def mark_dirty(self, dirty: bool, tid: Optional[TransactionId] = None) -> None:
        self.is_dirty = dirty
        if dirty:
            self.dirty_transaction = tid
        else:
            self.dirty_transaction = None

    def get_num_entries(self) -> int:
        return len(self.entries)

    def is_full(self) -> bool:
        return len(self.entries) >= self.max_entries

    def is_underflow(self) -> bool:
        """Check if the page has too few entries (needs merging)."""
        return len(self.entries) < self.max_entries // 2

    def find_key_index(self, key: Field) -> int:
        """Find the index where a key should be inserted."""
        for i, entry in enumerate(self.entries):
            if key.compare(entry.key) <= 0:
                return i
        return len(self.entries)


