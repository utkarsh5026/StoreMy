from typing import Optional
from .abstract_iterator import AbstractDbIterator
from ...storage.file import DbFile
from ...core.tuple import Tuple, TupleDesc
from ...concurrency.transactions import TransactionId


class SeqScan(AbstractDbIterator):
    """
    SeqScan is an implementation of a sequential scan access method.

    This operator reads each tuple of a table in no particular order
    (i.e., as they are laid out on disk). It's the fundamental way
    to access data from tables.

    Key Characteristics:
    1. Full table scan: Reads every tuple in the table
    2. No ordering: Tuples returned in physical storage order
    3. **Uses buffer pool**: Proper caching and locking behavior
    4. **Transaction-aware**: Acquires appropriate locks
    """

    def __init__(self, tid: TransactionId, table_id: int, table_alias: str = None):
        """
        Create a sequential scan over the specified table.

        Args:
            tid: The transaction this scan runs as part of
            table_id: The ID of the table to scan
            table_alias: Optional alias for the table (used in queries)
        """
        super().__init__()
        self.tid = tid
        self.table_id = table_id
        self.table_alias = table_alias or f"table_{table_id}"

        from app.database import Database
        self.table_file: DbFile = Database.get_catalog().get_db_file(table_id)
        self.file_iterator = None

    def open(self) -> None:
        """
        Open the scan by creating a file iterator.

        This delegates to the underlying HeapFile's iterator,
        which handles page-by-page scanning through the buffer pool.
        """
        super().open()
        self.file_iterator = self.table_file.iterator(self.tid)
        self.file_iterator.open()

    def close(self) -> None:
        """Close the file iterator and clean up resources."""
        if self.file_iterator:
            self.file_iterator.close()
            self.file_iterator = None
        super().close()

    def rewind(self) -> None:
        """Reset the scan to the beginning of the table."""
        if self.file_iterator:
            self.file_iterator.rewind()

    def get_tuple_desc(self) -> TupleDesc:
        """
        Return the TupleDesc with field names prefixed with table alias.

        This allows distinguishing between fields from different tables
        in join operations.
        """
        return self.table_file.get_tuple_desc()

    def read_next(self) -> Optional[Tuple]:
        """
        Read the next tuple from the underlying file iterator.

        This method implements the core scanning logic by delegating
        to the HeapFile's iterator.
        """
        if not self.file_iterator:
            return None

        try:
            if self.file_iterator.has_next():
                return self.file_iterator.next()
            else:
                return None
        except StopIteration:
            return None
