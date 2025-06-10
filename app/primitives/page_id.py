from abc import ABC, abstractmethod


class PageId(ABC):
    """
    Abstract interface for page identifiers.

    A PageId uniquely identifies a page within the database.
    Different storage implementations (heap files, B+ trees, etc.)
    can have different PageId formats.
    """

    @abstractmethod
    def get_table_id(self) -> int:
        """Return the table ID that this page belongs to."""
        pass

    @abstractmethod
    def get_page_number(self) -> int:
        """Return the page number within the table."""
        pass

    @abstractmethod
    def serialize(self) -> list[int]:
        """
        Serialize this PageId to a list of integers.
        Used for logging and recovery.
        """
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        """Check equality with another PageId."""
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """Hash value for use in dictionaries."""
        pass
