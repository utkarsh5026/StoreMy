from abc import ABC, abstractmethod
from ..type_enum import FieldType
from ..predicate import Predicate


class Field(ABC):
    """
    Abstract base class for all field types in the database.

    A field represents a single value in a tuple (row). Each field has:
    - A type (int, string, etc.)
    - A value
    - Methods for comparison, serialization, and hashing

    This follows the Template Method pattern - subclasses implement
    specific behavior while the base class defines the interface.
    """

    @abstractmethod
    def serialize(self) -> bytes:
        """
        Convert this field to bytes for storage on disk.
        """
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data: bytes) -> 'Field':
        """
        Create field instance from serialized bytes.

        Args:
            data: The bytes to deserialize

        Returns:
            Field instance

        Raises:
            ValueError: If data is invalid or corrupted
        """
        pass

    @classmethod
    @abstractmethod
    def get_size(cls) -> int:
        """
        Get the fixed size in bytes for this field type.

        Returns:
            Size in bytes
        """
        pass

    @abstractmethod
    def get_type(self) -> FieldType:
        """
        Return the type of this field.
        """
        pass

    @abstractmethod
    def compare(self, predicate: Predicate, other: 'Field') -> bool:
        """
        Compare this field with another using the given predicate.
        """
        pass

    @abstractmethod
    def __str__(self) -> str:
        """
        String representation of the field value.
        """
        pass

    @abstractmethod
    def __repr__(self) -> str:
        """
        Unambiguous string representation for debugging.
        """
        pass

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        """
        Check equality with another field.
        """
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """
        Hash value for this field (needed for sets/dicts).
        """
        pass
