from app.primitives import PageId


class HeapPageId(PageId):
    """
    Page identifier for heap file pages.

    Consists of:
    - table_id: which table this page belongs to
    - page_number: position within the table (0-based)
    """

    def __init__(self, table_id: int, page_number: int):
        if table_id < 0:
            raise ValueError(f"Table ID must be non-negative, got {table_id}")
        if page_number < 0:
            raise ValueError(
                f"Page number must be non-negative, got {page_number}"
            )

        self.table_id = table_id
        self.page_number = page_number

    def get_table_id(self) -> int:
        return self.table_id

    def get_page_number(self) -> int:
        return self.page_number

    def serialize(self) -> list[int]:
        """Serialize to [table_id, page_number]."""
        return [self.table_id, self.page_number]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HeapPageId):
            return False
        return (self.table_id == other.table_id and
                self.page_number == other.page_number)

    def __hash__(self) -> int:
        return hash((self.table_id, self.page_number))

    def __str__(self) -> str:
        return f"HeapPageId(table={self.table_id}, page={self.page_number})"

    def __repr__(self) -> str:
        return self.__str__()
