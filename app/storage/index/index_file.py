from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING
from ..file import DbFile
from ...core.tuple import RecordId
from ...core.types import Field, FieldType

if TYPE_CHECKING:
    from ...concurrency.transactions import TransactionId


class IndexFile(DbFile, ABC):
    """
    Abstract base class for index files.

    An IndexFile provides:
    1. Key-based lookup: find RecordIds for given key
    2. Range queries: find RecordIds for key ranges
    3. Iterator interface: scan all entries in key order
    4. Maintenance: insert/delete entries

    Different implementations:
    - BTreeFile: B+ tree index
    - HashFile: Hash index
    - Other specialized indexes
    """

    def __init__(self, file_path: str, key_type: FieldType):
        """
        Create an index file.

        Args:
            file_path: Path to the index file
            key_type: Type of keys in this index
        """
        super().__init__(file_path)
        self.key_type = key_type

    @abstractmethod
    def find_equal(self, tid: 'TransactionId', key: Field) -> List[RecordId]:
        """
        Find all RecordIds with the given key value.

        Args:
            tid: Transaction ID
            key: Key to search for

        Returns:
            List of RecordIds with matching key
        """
        pass

    @abstractmethod
    def find_range(self, tid: 'TransactionId',
                   min_key: Optional[Field], max_key: Optional[Field],
                   include_min: bool = True, include_max: bool = True) -> List[RecordId]:
        """
        Find all RecordIds with keys in the given range.

        Args:
            tid: Transaction ID
            min_key: Minimum key (None for no lower bound)
            max_key: Maximum key (None for no upper bound)
            include_min: Whether to include min_key in results
            include_max: Whether to include max_key in results

        Returns:
            List of RecordIds in the key range
        """
        pass

    @abstractmethod
    def insert_entry(self, tid: 'TransactionId', key: Field, record_id: RecordId) -> None:
        """
        Insert a new key-RecordId pair.

        Args:
            tid: Transaction ID
            key: Index key
            record_id: RecordId in heap file
        """
        pass

    @abstractmethod
    def delete_entry(self, tid: 'TransactionId', key: Field, record_id: RecordId) -> None:
        """
        Delete a key-RecordId pair.

        Args:
            tid: Transaction ID
            key: Index key
            record_id: RecordId to remove
        """
        pass

    def get_key_type(self) -> FieldType:
        """Return the type of keys in this index."""
        return self.key_type

    # Methods inherited from DbFile that indexes don't use
    def add_tuple(self, tid: 'TransactionId', tuple_obj) -> List[RecordId]:
        """Indexes don't store tuples directly."""
        raise NotImplementedError("Indexes don't support add_tuple")

    def delete_tuple(self, tid: 'TransactionId', tuple_obj) -> None:
        """Indexes don't delete tuples directly."""
        raise NotImplementedError("Indexes don't support delete_tuple")
