import struct
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class FloatField(Field):
    """32-bit floating point field"""

    def __init__(self, value: float):
        self.value = float(value)

    def get_type(self) -> FieldType:
        return FieldType.FLOAT

    def serialize(self) -> bytes:
        """4 bytes, IEEE 754 single precision"""
        return struct.pack('f', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'FloatField':
        value = struct.unpack('f', data)[0]
        return cls(value)

    @classmethod
    def get_size(cls) -> int:
        return 4

    def compare(self, predicate: Predicate, other: Field) -> bool:
        if not isinstance(other, FloatField):
            raise TypeError("Cannot compare FloatField with other types")

        return {
            Predicate.EQUALS: abs(self.value - other.value) < 1e-7,
            Predicate.NOT_EQUALS: abs(self.value - other.value) >= 1e-7,
            Predicate.GREATER_THAN: self.value > other.value,
            Predicate.LESS_THAN: self.value < other.value,
            Predicate.GREATER_THAN_OR_EQ: self.value >= other.value,
            Predicate.LESS_THAN_OR_EQ: self.value <= other.value,
        }[predicate]
