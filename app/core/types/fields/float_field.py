import struct
import math
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class FloatField(Field[float]):
    """32-bit floating point field"""

    EPSILON = 1e-6

    def __init__(self, value):
        """
        Initialize float field with validation.

        Args:
            value: Must be convertible to float

        Raises:
            TypeError: If value cannot be converted to float
            ValueError: If value is NaN (optional - could allow NaN if needed)
        """
        if value is None:
            raise TypeError("FloatField cannot accept None value")

        try:
            self.value = float(value)
        except (ValueError, TypeError) as e:
            raise TypeError(
                f"FloatField requires numeric value, got {type(value)}: {e}")

        if math.isnan(self.value):
            raise ValueError("FloatField does not support NaN values")

    def get_value(self) -> float:
        """Return the float value stored in this field."""
        return self.value

    def get_type(self) -> FieldType:
        return FieldType.FLOAT

    def serialize(self) -> bytes:
        """4 bytes, IEEE 754 single precision"""
        return struct.pack('f', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'FloatField':
        """
        Deserialize float field from bytes.

        Args:
            data: Must be exactly 4 bytes

        Returns:
            FloatField instance

        Raises:
            ValueError: If data length is not 4 bytes or data is invalid
            TypeError: If data is not bytes/bytearray
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data)}")

        if len(data) != 4:
            raise ValueError(
                f"FloatField requires exactly 4 bytes, got {len(data)}")

        try:
            value = struct.unpack('f', data)[0]
            return cls(value)
        except struct.error as e:
            raise ValueError(f"Invalid float data: {e}")

    @classmethod
    def get_size(cls) -> int:
        return 4

    def compare(self, predicate: Predicate, other: Field) -> bool:
        """
        Compare float fields with epsilon tolerance.

        Args:
            predicate: The comparison predicate
            other: The other field to compare with

        Returns:
            bool: Result of comparison

        Raises:
            TypeError: If other is not a FloatField
            ValueError: If predicate is not supported
        """
        if not isinstance(other, FloatField):
            raise TypeError(f"Cannot compare FloatField with {type(other)}")

        if math.isinf(self.value) or math.isinf(other.value):
            return {
                Predicate.EQUALS: self.value == other.value,
                Predicate.NOT_EQUALS: self.value != other.value,
                Predicate.GREATER_THAN: self.value > other.value,
                Predicate.LESS_THAN: self.value < other.value,
                Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
                Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
            }.get(predicate, False)

        return {
            Predicate.EQUALS: abs(self.value - other.value) < self.EPSILON,
            Predicate.NOT_EQUALS: abs(self.value - other.value) >= self.EPSILON,
            Predicate.GREATER_THAN: self.value > other.value,
            Predicate.LESS_THAN: self.value < other.value,
            Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
            Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
        }.get(predicate)

    def __str__(self) -> str:
        if math.isinf(self.value):
            return "inf" if self.value > 0 else "-inf"
        return str(self.value)

    def __repr__(self) -> str:
        return f"FloatField({self.value})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FloatField):
            return False

        if math.isinf(self.value) or math.isinf(other.value):
            return self.value == other.value

        return abs(self.value - other.value) < self.EPSILON

    def __hash__(self) -> int:
        return hash(round(self.value, 6))
