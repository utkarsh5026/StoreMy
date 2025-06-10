from abc import ABC, abstractmethod
from typing import Optional
from ...primitives import PageId, TransactionId


class Page(ABC):
    """
    Abstract interface for database pages.

    A page is the unit of I/O in the database - all disk reads/writes
    happen at the page level. Pages have a fixed size (typically 4KB)
    and contain multiple tuples plus metadata.

    Key concepts:
    - Dirty bit: has the page been modified since last write to disk?
    - Before image: copy of page state before current transaction
    """

    PAGE_SIZE_IN_BYTES = 4096  # 4KB pages (standard database page size)

    @abstractmethod
    def get_id(self) -> PageId:
        """Return the unique identifier for this page."""
        pass

    @abstractmethod
    def get_page_data(self) -> bytes:
        """
        Return the raw bytes of this page for writing to disk.

        This is the complete page content including headers, data,
        and any padding needed to reach PAGE_SIZE_IN_BYTES bytes.
        """
        pass

    @abstractmethod
    def is_dirty(self) -> Optional[TransactionId]:
        """
        Return the transaction ID that dirtied this page, or None if clean.

        A page is "dirty" if it has been modified since it was last
        written to disk. The transaction ID helps track which transaction
        is responsible for the changes.
        """
        pass

    @abstractmethod
    def mark_dirty(self, dirty: bool, transaction_id: Optional[TransactionId]) -> None:
        """
        Mark this page as dirty or clean.

        Args:
            dirty: True to mark dirty, False to mark clean
            transaction_id: ID of transaction making the change (if dirty=True)
        """
        pass

    @abstractmethod
    def get_before_image(self) -> 'Page':
        """
        Return a copy of this page from before the current transaction.
        Used for recovery and rollback operations.
        """
        pass

    @abstractmethod
    def set_before_image(self) -> None:
        """
        Save the current state as the "before image".
        Called when a transaction starts modifying the page.
        """
        pass
