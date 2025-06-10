import struct
import math
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class DoubleField(Field):
    """64-bit floating point field"""

    # Tolerance for double comparison (more precise than float)
    EPSILON = 1e-9

    def __init__(self, value):
        """
        Initialize double field with validation.

        Args:
            value: Must be convertible to float

        Raises:
            TypeError: If value cannot be converted to float
            ValueError: If value is NaN
        """
        if value is None:
            raise TypeError("DoubleField cannot accept None value")

        try:
            self.value = float(value)
        except (ValueError, TypeError) as e:
            raise TypeError(
                f"DoubleField requires numeric value, got {type(value)}: {e}")

        if math.isnan(self.value):
            raise ValueError("DoubleField does not support NaN values")

    def get_type(self) -> FieldType:
        return FieldType.DOUBLE

    def serialize(self) -> bytes:
        """8 bytes, IEEE 754 double precision"""
        return struct.pack('d', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'DoubleField':
        """
        Deserialize double field from bytes.

        Args:
            data: Must be exactly 8 bytes

        Returns:
            DoubleField instance

        Raises:
            ValueError: If data length is not 8 bytes or data is invalid
            TypeError: If data is not bytes/bytearray
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data)}")

        if len(data) != 8:
            raise ValueError(
                f"DoubleField requires exactly 8 bytes, got {len(data)}")

        try:
            value = struct.unpack('d', data)[0]
            return cls(value)
        except struct.error as e:
            raise ValueError(f"Invalid double data: {e}")

    @classmethod
    def get_size(cls) -> int:
        return 8

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare double fields with epsilon tolerance.

        Args:
            predicate: The comparison predicate
            other: The other field to compare with

        Returns:
            bool: Result of comparison

        Raises:
            TypeError: If other is not a DoubleField
            ValueError: If predicate is not supported
        """
        if not isinstance(other, DoubleField):
            raise TypeError(f"Cannot compare DoubleField with {type(other)}")

        if math.isinf(self.value) or math.isinf(other.value):
            comparisons = {
                Predicate.EQUALS: self.value == other.value,
                Predicate.NOT_EQUALS: self.value != other.value,
                Predicate.GREATER_THAN: self.value > other.value,
                Predicate.LESS_THAN: self.value < other.value,
                Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
                Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
            }
        else:
            comparisons = {
                Predicate.EQUALS: abs(self.value - other.value) < self.EPSILON,
                Predicate.NOT_EQUALS: abs(self.value - other.value) >= self.EPSILON,
                Predicate.GREATER_THAN: self.value > other.value,
                Predicate.LESS_THAN: self.value < other.value,
                Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
                Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
            }

        if predicate not in comparisons:
            raise ValueError(
                f"Unsupported predicate for DoubleField: {predicate}")

        return comparisons[predicate]

    def __str__(self) -> str:
        # Handle special values nicely
        if math.isinf(self.value):
            return "inf" if self.value > 0 else "-inf"
        return str(self.value)

    def __repr__(self) -> str:
        return f"DoubleField({self.value})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DoubleField):
            return False

        if math.isinf(self.value) or math.isinf(other.value):
            return self.value == other.value

        return abs(self.value - other.value) < self.EPSILON

    def __hash__(self) -> int:
        # Round to avoid floating point precision issues in hashing
        return hash(round(self.value, 9))
