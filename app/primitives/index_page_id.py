from .page_id import PageId

class IndexPageId(PageId):
    """
    Unique identifier for index pages.

    Unlike HeapPageId, IndexPageId includes:
    - Index file identifier
    - Page number within an index
    - Index type (for debugging/validation)
    """

    def __init__(self, index_file_id: int, page_number: int, index_type: str):
        """
        Create an IndexPageId.

        Args:
            index_file_id: Unique identifier for the index file
            page_number: Page number within the index file
            index_type: Type of index ("btree", "hash", etc.)
        """
        self.index_file_id = index_file_id
        self.page_number = page_number
        self.index_type = index_type

    def get_table_id(self) -> int:
        """Return the index file ID (not table ID)."""
        return self.index_file_id

    def get_page_number(self) -> int:
        """Return the page number within the index file."""
        return self.page_number

    def serialize(self) -> bytes:
        """Serialize the page ID for storage/transmission."""
        return f"{self.index_file_id}:{self.page_number}:{self.index_type}".encode('utf-8')

    def hash_code(self) -> int:
        """Return hash code for use in hash tables."""
        return hash((self.index_file_id, self.page_number, self.index_type))

    def equals(self, other) -> bool:
        """Check equality with another IndexPageId."""
        if not isinstance(other, IndexPageId):
            return False
        return (self.index_file_id == other.index_file_id and
                self.page_number == other.page_number and
                self.index_type == other.index_type)

    def __str__(self) -> str:
        return f"IndexPageId(file={self.index_file_id}, page={self.page_number}, type={self.index_type})"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other) -> bool:
        return self.equals(other)

    def __hash__(self) -> int:
        return self.hash_code()