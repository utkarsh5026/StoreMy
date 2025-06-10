import struct
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class IntField(Field):
    """
    Field implementation for 32-bit signed integers.

    Storage format: 4 bytes in little-endian format
    Range: -2,147,483,648 to 2,147,483,647
    """

    # Define range constants for clarity
    MIN_VALUE = -2**31
    MAX_VALUE = 2**31 - 1

    def __init__(self, value):
        """
        Initialize integer field with validation.

        Args:
            value: Must be an integer within 32-bit signed range

        Raises:
            TypeError: If value is not an integer
            ValueError: If value is out of range
        """
        if not isinstance(value, int):
            # Try to convert if it's a numeric type
            if hasattr(value, '__int__'):
                try:
                    value = int(value)
                except (ValueError, OverflowError):
                    raise TypeError(
                        f"IntField requires int, got {type(value)}")
            else:
                raise TypeError(f"IntField requires int, got {type(value)}")

        if not (self.MIN_VALUE <= value <= self.MAX_VALUE):
            raise ValueError(
                f"Integer value {value} out of range [{self.MIN_VALUE}, {self.MAX_VALUE}]")

        self.value = value

    def get_type(self) -> FieldType:
        return FieldType.INT

    def serialize(self) -> bytes:
        """Serialize to 4 bytes in little-endian format."""
        return struct.pack('<i', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'IntField':
        """
        Deserialize a 4-byte integer from bytes.

        Args:
            data: Must be exactly 4 bytes

        Returns:
            IntField instance

        Raises:
            ValueError: If data length is not 4 bytes or data is invalid
            TypeError: If data is not bytes/bytearray
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data)}")

        if len(data) != 4:
            raise ValueError(
                f"IntField requires exactly 4 bytes, got {len(data)}"
            )

        try:
            value = struct.unpack('<i', data)[0]
            return cls(value)
        except struct.error as e:
            raise ValueError(f"Invalid integer data: {e}")

    @classmethod
    def get_size(cls) -> int:
        """Return the size in bytes for integer fields."""
        return 4

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare this integer field with another field.

        Args:
            predicate: The predicate to use for comparison
            other: The other field to compare with

        Returns:
            bool: True if the comparison is true, False otherwise

        Raises:
            TypeError: If other is not an IntField
            ValueError: If predicate is not supported
        """
        if not isinstance(other, IntField):
            raise TypeError(f"Cannot compare IntField with {type(other)}")

        other_value = other.value

        comparisons = {
            Predicate.EQUALS: lambda a, b: a == b,
            Predicate.NOT_EQUALS: lambda a, b: a != b,
            Predicate.GREATER_THAN: lambda a, b: a > b,
            Predicate.GREATER_THAN_OR_EQ: lambda a, b: a >= b,
            Predicate.LESS_THAN: lambda a, b: a < b,
            Predicate.LESS_THAN_OR_EQ: lambda a, b: a <= b,
            Predicate.LIKE: lambda a, b: a == b,
        }

        if predicate not in comparisons:
            raise ValueError(
                f"Unsupported predicate for IntField: {predicate}"
            )

        return comparisons[predicate](self.value, other_value)

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"IntField({self.value})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, IntField) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)
