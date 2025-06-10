from app.core.types.field import Field
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate
import struct


class StringField(Field):
    """
    Field implementation for fixed-length strings.

    Storage format:
    - 4 bytes: length of actual string (little-endian int32)
    - 128 bytes: string content (UTF-8 encoded, null-padded)

    Total: 132 bytes per string field

    Why fixed-length? It simplifies page layout calculations and
    memory management. Variable-length strings would require more
    complex storage schemes.
    """
    MAX_LENGTH_IN_BYTES = 128

    def __init__(self, value):
        if not isinstance(value, str):
            raise TypeError(f"StringField requires str, got {type(value)}")

        encoded = value.encode('utf-8')
        if len(encoded) > self.MAX_LENGTH_IN_BYTES:
            raise ValueError(
                f"String too long: {len(encoded)} bytes > {self.MAX_LENGTH_IN_BYTES}")

        self.value = value
        self._encoded = encoded

    def get_type(self) -> FieldType:
        return FieldType.STRING

    def serialize(self) -> bytes:
        """
        Serialize string to fixed 132-byte format:
        - Bytes 0-3: length of string in bytes (int32, little-endian)
        - Bytes 4-131: UTF-8 encoded string content, null-padded to 128 bytes
        """
        encoded_length = len(self._encoded)
        length_bytes = struct.pack('<i', encoded_length)

        content_bytes = self._encoded + b'\0' * \
            (self.MAX_LENGTH_IN_BYTES - encoded_length)

        return length_bytes + content_bytes

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare this string field with another field.

        String comparison uses lexicographic ordering (dictionary order).
        LIKE operator supports simple substring matching (SQL LIKE without wildcards).
        """
        if not isinstance(other, StringField):
            raise TypeError(f"Cannot compare StringField with {type(other)}")

        other_value = other.value

        comparisons = {
            Predicate.EQUALS: lambda a, b: a == b,
            Predicate.NOT_EQUALS: lambda a, b: a != b,
            Predicate.GREATER_THAN: lambda a, b: a > b,
            Predicate.GREATER_THAN_OR_EQ: lambda a, b: a >= b,
            Predicate.LESS_THAN: lambda a, b: a < b,
            Predicate.LESS_THAN_OR_EQ: lambda a, b: a <= b,
            Predicate.LIKE: lambda a, b: b in a,
        }

        if predicate not in comparisons:
            raise ValueError(
                f"Unsupported predicate for StringField: {predicate}")

        return comparisons[predicate](self.value, other_value)

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StringField) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        return f"StringField('{self.value}')"

    @classmethod
    def deserialize(cls, data: bytes) -> 'StringField':
        """Create StringField from serialized bytes."""
        if len(data) != 132:
            raise ValueError(
                f"StringField requires exactly 132 bytes, got {len(data)}")

        length = struct.unpack('<i', data[:4])[0]
        if length < 0 or length > cls.MAX_LENGTH_IN_BYTES:
            raise ValueError(f"Invalid string length: {length}")

        content_bytes = data[4:4+length]
        try:
            value = content_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError(f"Invalid UTF-8 data in StringField: {e}")

        return cls(value)
