from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate
import struct


class DoubleField(Field):
    """64-bit floating point field"""

    def __init__(self, value: float):
        self.value = float(value)

    def serialize(self) -> bytes:
        """8 bytes, IEEE 754 double precision"""
        return struct.pack('d', self.value)

    @classmethod
    def get_size(cls) -> int:
        return 8

    def get_type(self) -> FieldType:
        return FieldType.DOUBLE

    @classmethod
    def deserialize(cls, data: bytes) -> 'DoubleField':
        value = struct.unpack('d', data)[0]
        return cls(value)

    def compare(self, predicate: Predicate, other: Field) -> bool:
        if not isinstance(other, DoubleField):
            raise TypeError("Cannot compare DoubleField with other types")

        return {
            Predicate.EQUALS: abs(self.value - other.value) < 1e-7,
            Predicate.NOT_EQUALS: abs(self.value - other.value) >= 1e-7,
            Predicate.GREATER_THAN: self.value > other.value,
            Predicate.LESS_THAN: self.value < other.value,
            Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
            Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
        }[predicate]
