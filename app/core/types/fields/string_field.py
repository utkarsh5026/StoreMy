from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate
import struct


class StringField(Field[str]):
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
    TOTAL_SIZE = 132  # 4 bytes for length + 128 bytes for content

    def __init__(self, value):
        """
        Initialize string field with validation.

        Args:
            value: Must be a string or convertible to string

        Raises:
            TypeError: If value cannot be converted to string
            ValueError: If encoded string exceeds maximum length
        """
        if value is None:
            raise TypeError("StringField cannot accept None value")

        if not isinstance(value, str):
            try:
                value = str(value)
            except Exception as e:
                raise TypeError(
                    f"StringField requires str, got {type(value)}: {e}")

        if '\x00' in value:
            raise ValueError("StringField cannot contain null bytes")

        try:
            encoded = value.encode('utf-8')
        except UnicodeEncodeError as e:
            raise ValueError(f"StringField cannot encode value as UTF-8: {e}")

        if len(encoded) > self.MAX_LENGTH_IN_BYTES:
            raise ValueError(
                f"String too long: {len(encoded)} bytes > {self.MAX_LENGTH_IN_BYTES}")

        self.value = value
        self._encoded = encoded

    def get_value(self) -> str:
        """Return the string value stored in this field."""
        return self.value

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

        result = length_bytes + content_bytes
        assert len(
            result) == self.TOTAL_SIZE, f"Expected {self.TOTAL_SIZE} bytes, got {len(result)}"
        return result

    @classmethod
    def deserialize(cls, data: bytes) -> 'StringField':
        """
        Create StringField from serialized bytes.

        Args:
            data: Must be exactly 132 bytes

        Returns:
            StringField instance

        Raises:
            ValueError: If data is invalid
            TypeError: If data is not bytes/bytearray
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data)}")

        if len(data) != cls.TOTAL_SIZE:
            raise ValueError(
                f"StringField requires exactly {cls.TOTAL_SIZE} bytes, got {len(data)}")

        try:
            length = struct.unpack('<i', data[:4])[0]
        except struct.error as e:
            raise ValueError(f"Invalid length data in StringField: {e}")

        if length < 0:
            raise ValueError(
                f"Invalid string length: {length} (cannot be negative)")

        if length > cls.MAX_LENGTH_IN_BYTES:
            raise ValueError(
                f"Invalid string length: {length} > {cls.MAX_LENGTH_IN_BYTES}")

        content_bytes = data[4:4+length]

        try:
            value = content_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError(f"Invalid UTF-8 data in StringField: {e}")

        padding_start = 4 + length
        padding_bytes = data[padding_start:]
        if len(padding_bytes) > 0 and padding_bytes != b'\0' * len(padding_bytes):
            # This could be a warning instead of an error, depending on requirements
            pass  # Allow non-null padding for compatibility

        return cls(value)

    @classmethod
    def get_size(cls) -> int:
        """Return the fixed size in bytes for string fields."""
        return cls.TOTAL_SIZE

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare this string field with another field.

        String comparison uses lexicographic ordering (dictionary order).
        LIKE operator supports simple substring matching (SQL LIKE without wildcards).

        Args:
            predicate: The comparison predicate
            other: The other field to compare with

        Returns:
            bool: Result of comparison

        Raises:
            TypeError: If other is not a StringField
            ValueError: If predicate is not supported
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
            Predicate.LIKE: lambda a, b: b in a
        }

        if predicate not in comparisons:
            raise ValueError(
                f"Unsupported predicate for StringField: {predicate}")

        return comparisons[predicate](self.value, other_value)

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        escaped_value = repr(self.value)
        return f"StringField({escaped_value})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StringField) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)
