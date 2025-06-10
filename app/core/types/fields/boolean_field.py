import struct
from .field import Field
from ..type_enum import FieldType
from ..predicate import Predicate


class BoolField(Field):
    def __init__(self, value: bool):
        self.value = bool(value)

    def get_type(self) -> FieldType:
        return FieldType.BOOLEAN

    def serialize(self) -> bytes:
        """Pack as single byte: 1 for True, 0 for False"""
        return struct.pack('?', self.value)

    @classmethod
    def deserialize(cls, data: bytes) -> 'BoolField':
        value = struct.unpack('?', data)[0]
        return cls(value)

    @classmethod
    def get_size(cls) -> int:
        return 1

    def compare(self, predicate: Predicate, other: Field) -> bool:
        if not isinstance(other, BoolField):
            raise TypeError("Cannot compare BoolField with other types")

        if predicate == Predicate.EQUALS:
            return self.value == other.value
        elif predicate == Predicate.NOT_EQUALS:
            return self.value != other.value
        else:
            raise ValueError(f"Unsupported predicate {predicate} for boolean")

    def __str__(self) -> str:
        return str(self.value)

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other) -> bool:
        return isinstance(other, BoolField) and self.value == other.value
