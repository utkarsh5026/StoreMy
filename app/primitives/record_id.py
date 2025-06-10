from typing import TYPE_CHECKING
from .page_id import PageId

if TYPE_CHECKING:
    from ..storage.page import PageId


class RecordId:
    """
    Unique identifier for a tuple (row) within the database.

    A RecordId consists of:
    1. PageId: identifies which page contains the tuple
    2. tuple_number: slot number within that page (0-based index)

    This is similar to a "row ID" or "TID" in other database systems.
    It provides a physical address for any tuple in the database.
    """

    def __init__(self, page_id: PageId, tuple_number: int):
        if tuple_number < 0:
            raise ValueError(
                f"Tuple number must be non-negative, got {tuple_number}")

        self.page_id = page_id
        self.tuple_number = tuple_number

    def get_page_id(self) -> PageId:
        """Return the page ID portion of this record ID."""
        return self.page_id

    def get_tuple_number(self) -> int:
        """Return the tuple number (slot) within the page."""
        return self.tuple_number

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RecordId):
            return False
        return (self.page_id == other.page_id and
                self.tuple_number == other.tuple_number)

    def __hash__(self) -> int:
        return hash((self.page_id, self.tuple_number))

    def __str__(self) -> str:
        return f"RecordId(page={self.page_id}, tuple={self.tuple_number})"

    def __repr__(self) -> str:
        return self.__str__()
