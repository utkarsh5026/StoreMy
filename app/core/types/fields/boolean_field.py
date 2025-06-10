import struct
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class BoolField(Field):
    """
    Field implementation for boolean values.

    Storage format: 1 byte (0 for False, 1 for True)
    """

    def __init__(self, value):
        """
        Initialize boolean field.

        Args:
            value: Any value that can be converted to bool

        Note: Accepts any type and converts to bool using Python's truthiness rules
        """
        if value is None:
            raise TypeError("BoolField cannot accept None value")
        self.value = bool(value)

    def get_type(self) -> FieldType:
        return FieldType.BOOLEAN

    def serialize(self) -> bytes:
        """Pack as single byte: 1 for True, 0 for False"""
        return struct.pack('?', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'BoolField':
        """
        Deserialize boolean field from bytes.

        Args:
            data: Must be exactly 1 byte

        Returns:
            BoolField instance

        Raises:
            ValueError: If data length is not 1 byte
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data)}")

        if len(data) != 1:
            raise ValueError(
                f"BoolField requires exactly 1 byte, got {len(data)}")

        try:
            value = struct.unpack('?', data)[0]
            return cls(value)
        except struct.error as e:
            raise ValueError(f"Invalid boolean data: {e}")

    @classmethod
    def get_size(cls) -> int:
        return 1

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare boolean fields.

        Only EQUALS and NOT_EQUALS are supported for booleans.
        """
        if not isinstance(other, BoolField):
            raise TypeError(f"Cannot compare BoolField with {type(other)}")

        if predicate == Predicate.EQUALS:
            return self.value == other.value
        elif predicate == Predicate.NOT_EQUALS:
            return self.value != other.value
        else:
            raise ValueError(
                f"Unsupported predicate {predicate} for boolean fields")

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"BoolField({self.value})"

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other) -> bool:
        return isinstance(other, BoolField) and self.value == other.value
