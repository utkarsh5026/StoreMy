import struct
from app.core.types.fields.field import Field
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class IntField(Field):
    """
    Field implementation for 32-bit signed integers.

    Storage format: 4 bytes in little-endian format
    Range: -2,147,483,648 to 2,147,483,647
    """

    def __init__(self, value):
        if not isinstance(value, int):
            raise TypeError(f"IntField requires int, got {type(value)}")
        if not (-2**31 <= value < 2**31):
            raise ValueError(
                f"Integer value {value} out of range for 32-bit signed int")
        self.value = value

    def get_type(self) -> FieldType:
        return FieldType.INT

    def serialize(self) -> bytes:
        return struct.pack('<i', self.value)

    def __str__(self) -> str:
        return str(self.value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, IntField) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"IntField({self.value})"

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare this integer field with another field.

        The comparison logic implements all standard SQL comparison operators.

        Args:
            predicate (Predicate): The predicate to use for comparison.
            other (Field): The other field to compare with.

        Returns:
            bool: True if the comparison is true, False otherwise.
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
                f"Unsupported predicate for IntField: {predicate}")

        return comparisons[predicate](self.value, other_value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'IntField':
        """
        Deserialize a 4-byte integer from bytes.

        Args:
            data (bytes): The bytes to deserialize.

        Returns:
            IntField: The deserialized integer field.
        """
        if len(data) != 4:
            raise ValueError(f"IntField requires 4 bytes, got {len(data)}")
        return cls(struct.unpack('<i', data)[0])
