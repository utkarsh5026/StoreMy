import struct
from typing import Union
from enum import Enum
from dataclasses import dataclass

from app.primitives import PageId, RecordId
from app.core.types import Field, FieldType
from app.core.types.predicate import Predicate
from app.storage.index.index_type import IndexType


class BTreePageType(Enum):
    LEAF = "leaf"
    INTERNAL = "internal"


class BTreePageId(PageId):
    """Specialized page ID for B+ tree pages."""

    def __init__(self, index_file_id: int, page_number: int, page_type: BTreePageType):
        self.index_file_id = index_file_id
        self.page_number = page_number
        self.page_type = page_type
        self.index_type = IndexType.BTREE

    def get_table_id(self) -> int:
        return self.index_file_id

    def get_page_number(self) -> int:
        return self.page_number

    def serialize(self) -> bytes:
        return f"btree:{self.index_file_id}:{self.page_number}:{self.page_type}".encode()

    def __str__(self) -> str:
        page_type = "leaf" if self.page_type == BTreePageType.LEAF else "internal"
        return f"BTreePageId(file={self.index_file_id}, page={self.page_number}, {page_type})"

    def __hash__(self) -> int:
        return hash((self.index_file_id, self.page_number, self.page_type))

    def __eq__(self, other) -> bool:
        if not isinstance(other, BTreePageId):
            return False
        return (self.index_file_id == other.index_file_id and
                self.page_number == other.page_number and
                self.page_type == other.page_type)


@dataclass
class BTreeEntry:
    """Represents an entry in a B+ tree node."""
    key: Field
    # RecordId for leaf, PageId for internal
    value: Union[RecordId, 'BTreePageId']

    def __str__(self) -> str:
        return f"BTreeEntry({self.key}, {self.value})"

    def serialize(self) -> bytes:
        """Serialize entry to bytes."""
        key_bytes = self.key.serialize()
        value_bytes = self.value.serialize() if hasattr(
            self.value, 'serialize') else bytes(self.value)

        # Format: [key_size][key_data][value_data]
        return struct.pack('!I', len(key_bytes)) + key_bytes + value_bytes

    def compare_to(self, other: Union['BTreeEntry', Field, None]) -> int:
        """
        Compare this entry with another entry or key.

        Args:
            other: Another BTreeEntry, a Field (key), or None

        Returns:
            int: -1 if this < other, 0 if this == other, 1 if this > other

        Raises:
            TypeError: If other is not a compatible type
            ValueError: If comparison cannot be performed
        """
        if other is None:
            raise ValueError("Cannot compare BTreeEntry with None")

        if isinstance(other, BTreeEntry):
            other_key = other.key
        elif isinstance(other, Field):
            other_key = other
        else:
            raise TypeError(f"Cannot compare BTreeEntry with {type(other)}. "
                            f"Expected BTreeEntry or Field, got {type(other)}")

        # Validate that keys are compatible for comparison
        if type(self.key) != type(other_key):
            raise TypeError(
                f"Cannot compare {type(self.key)} with {type(other_key)}")

        try:
            # Use the Field's comparison methods to determine ordering
            if self.key.compare(Predicate.LESS_THAN, other_key):
                return -1
            elif self.key.compare(Predicate.GREATER_THAN, other_key):
                return 1
            elif self.key.compare(Predicate.EQUALS, other_key):
                return 0
            else:
                # This should not happen with a properly implemented Field
                raise ValueError(
                    f"Inconsistent comparison results for keys {self.key} and {other_key}")
        except Exception as e:
            raise ValueError(
                f"Failed to compare keys {self.key} and {other_key}: {e}")

    def compare_key(self, key: Field) -> int:
        """
        Compare this entry's key with another key.

        Args:
            key: The key to compare with

        Returns:
            int: -1 if this.key < key, 0 if this.key == key, 1 if this.key > key
        """
        return self.compare_to(key)

    def __lt__(self, other: 'BTreeEntry') -> bool:
        """Less than comparison for sorting."""
        if not isinstance(other, BTreeEntry):
            return NotImplemented
        return self.compare_to(other) < 0

    def __le__(self, other: 'BTreeEntry') -> bool:
        """Less than or equal comparison."""
        if not isinstance(other, BTreeEntry):
            return NotImplemented
        return self.compare_to(other) <= 0

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, BTreeEntry):
            return False
        return self.compare_to(other) == 0

    def __ne__(self, other: object) -> bool:
        """Not equal comparison."""
        return not self.__eq__(other)

    def __gt__(self, other: 'BTreeEntry') -> bool:
        """Greater than comparison."""
        if not isinstance(other, BTreeEntry):
            return NotImplemented
        return self.compare_to(other) > 0

    def __ge__(self, other: 'BTreeEntry') -> bool:
        """Greater than or equal comparison."""
        if not isinstance(other, BTreeEntry):
            return NotImplemented
        return self.compare_to(other) >= 0

    def __hash__(self) -> int:
        """Hash based on a key for use in sets/dicts."""
        return hash(self.key)

    @classmethod
    def deserialize(cls, data: bytes, key_type: FieldType, is_leaf: bool) -> 'BTreeEntry':
        """Deserialize entry from bytes."""
        # Read key size
        key_size = struct.unpack('!I', data[:4])[0]

        # Read key data
        key_data = data[4:4+key_size]
        key = Field.deserialize(key_data)

        # Read value data
        value_data = data[4+key_size:]
        if is_leaf:
            value = RecordId.deserialize(value_data)
        else:
            value = BTreePageId.deserialize(value_data)

        return cls(key, value)
