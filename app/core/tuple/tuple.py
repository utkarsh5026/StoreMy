from ..types import Field, IntField, StringField, FieldType
from .tuple_desc import TupleDesc
from .record_id import RecordId


class Tuple:
    """
    Represents a single row of data in the database.

    A Tuple contains:
    1. TupleDesc: schema defining the structure
    2. List of Fields: the actual data values
    3. Optional RecordId: physical location identifier

    This is the fundamental data unit that flows through query operators.
    """

    def __init__(self, tuple_desc: TupleDesc):
        self.tuple_desc = tuple_desc
        self.fields: list[Field | None] = [None] * tuple_desc.num_fields()
        self.record_id: RecordId | None = None

    def get_tuple_desc(self) -> TupleDesc:
        """Return the schema descriptor for this tuple."""
        return self.tuple_desc

    def get_record_id(self) -> RecordId | None:
        """Return the physical location of this tuple (may be None)."""
        return self.record_id

    def set_record_id(self, record_id: RecordId | None) -> None:
        """Set the physical location of this tuple."""
        self.record_id = record_id

    def set_field(self, field_index: int, field: Field) -> None:
        """
        Set the value of a field at the given index.

        Validates that:
        1. The index is valid
        2. The field type matches the schema
        """
        if not (0 <= field_index < self.tuple_desc.num_fields()):
            raise IndexError(f"Field index {field_index} out of range")

        expected_type = self.tuple_desc.get_field_type(field_index)
        actual_type = field.get_type()

        if expected_type != actual_type:
            raise TypeError(
                f"Field {field_index} expects {expected_type}, got {actual_type}")

        self.fields[field_index] = field

    def get_field(self, field_index: int) -> Field:
        """
        Get the value of a field at the given index.

        Raises an exception if the field hasn't been set yet.
        """
        if not (0 <= field_index < self.tuple_desc.num_fields()):
            raise IndexError(f"Field index {field_index} out of range")

        field = self.fields[field_index]
        if field is None:
            raise ValueError(f"Field {field_index} has not been set")

        return field

    def is_complete(self) -> bool:
        """Check if all fields in this tuple have been set."""
        return all(field is not None for field in self.fields)

    @staticmethod
    def combine(tuple1: 'Tuple', tuple2: 'Tuple') -> 'Tuple':
        """
        Combine two tuples into one (used for joins).

        The result has a combined schema and all fields from both tuples.
        The record_id is taken from tuple1 (arbitrary choice).
        """
        combined_desc = TupleDesc.combine(tuple1.tuple_desc, tuple2.tuple_desc)
        result = Tuple(combined_desc)

        # Copy fields from tuple1
        for i in range(tuple1.tuple_desc.num_fields()):
            if tuple1.fields[i] is not None:
                result.set_field(i, tuple1.fields[i])

        # Copy fields from tuple2
        offset = tuple1.tuple_desc.num_fields()
        for i in range(tuple2.tuple_desc.num_fields()):
            if tuple2.fields[i] is not None:
                result.set_field(offset + i, tuple2.fields[i])

        # Preserve record_id from first tuple
        result.set_record_id(tuple1.get_record_id())

        return result

    def serialize(self) -> bytes:
        """
        Serialize this tuple to bytes for storage.

        Format: concatenation of all field serializations in order.
        The tuple descriptor is not included (assumed to be known).
        """
        if not self.is_complete():
            raise ValueError("Cannot serialize incomplete tuple")

        data = b''
        for field in self.fields:
            data += field.serialize()

        return data

    @classmethod
    def deserialize(cls, data: bytes, tuple_desc: TupleDesc) -> 'Tuple':
        """
        Create a tuple from serialized bytes and a schema descriptor.

        This is the inverse of serialize() - it reconstructs a tuple
        from its byte representation.
        """
        tuple_obj = cls(tuple_desc)
        offset = 0

        for i in range(tuple_desc.num_fields()):
            field_type = tuple_desc.get_field_type(i)
            field_size = field_type.get_length()

            if offset + field_size > len(data):
                raise ValueError(f"Insufficient data to deserialize field {i}")

            field_data = data[offset:offset + field_size]

            if field_type == FieldType.INT:
                field = IntField.deserialize(field_data)
            elif field_type == FieldType.STRING:
                field = StringField.deserialize(field_data)
            else:
                raise ValueError(f"Unknown field type: {field_type}")

            tuple_obj.set_field(i, field)
            offset += field_size

        return tuple_obj

    def __str__(self) -> str:
        """Tab-separated string representation (database output format)."""
        if not self.is_complete():
            return "<incomplete tuple>"

        field_strs = [str(field) for field in self.fields]
        return '\t'.join(field_strs)

    def __repr__(self) -> str:
        return f"Tuple({self.fields}, record_id={self.record_id})"

    def __eq__(self, other: object) -> bool:
        """
        Two tuples are equal if they have the same schema and field values.
        RecordId is NOT considered for equality (logical vs physical equality).
        """
        if not isinstance(other, Tuple):
            return False

        return (self.tuple_desc.equals(other.tuple_desc) and
                self.fields == other.fields)

    def __hash__(self) -> int:
        """Hash based on field values (for use in sets/dicts)."""
        if not self.is_complete():
            raise ValueError("Cannot hash incomplete tuple")
        return hash(tuple(self.fields))
