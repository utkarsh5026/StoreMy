from typing import Optional
from ..types import FieldType


class TupleDesc:
    """
    Schema descriptor for a tuple (row).

    A TupleDesc defines:
    1. The types of fields in the tuple (int, string, etc.)
    2. Optional field names for each position
    3. Methods to calculate tuple size and lookup fields

    This is the database's "schema" - it tells us what a row looks like.
    Think of it as the column definitions in a CREATE TABLE statement.
    """

    def __init__(self, field_types: list[FieldType], field_names: list[str] | None = None):
        if not field_types:
            raise ValueError("TupleDesc must have at least one field")

        if field_names is not None and len(field_names) != len(field_types):
            raise ValueError(f"Number of field names ({len(field_names)}) "
                             f"must match number of field types ({len(field_types)})")

        self.field_types = field_types.copy()
        self.field_names = field_names.copy() if field_names else None

    def num_fields(self) -> int:
        """Return the number of fields in this tuple descriptor."""
        return len(self.field_types)

    def get_field_type(self, field_index: int) -> FieldType:
        """Get the type of the field at the given index."""
        if not (0 <= field_index < len(self.field_types)):
            raise IndexError(
                f"Field index {field_index} out of range [0, {len(self.field_types)})")
        return self.field_types[field_index]

    def get_field_name(self, field_index: int) -> Optional[str]:
        """Get the name of the field at the given index (if names are defined)."""
        if not (0 <= field_index < len(self.field_types)):
            raise IndexError(
                f"Field index {field_index} out of range [0, {len(self.field_types)})")

        if self.field_names is None:
            return None
        return self.field_names[field_index]

    def name_to_index(self, field_name: str) -> int:
        """
        Find the index of a field by name.

        Supports both simple names ("id") and qualified names ("table.id").
        For qualified names, we strip the table prefix and match on the field name.
        """
        if self.field_names is None:
            raise ValueError(
                "Cannot lookup field by name - no field names defined")


        if '.' in field_name:
            field_name = field_name.split('.', 1)[1]

        try:
            return self.field_names.index(field_name)
        except ValueError:
            raise ValueError(
                f"Field '{field_name}' not found in tuple descriptor")

    def get_size(self) -> int:
        """
        Calculate the total size in bytes for a tuple with this descriptor.

        This is the sum of the sizes of all field types.
        Used for page layout calculations and memory allocation.
        """
        return sum(field_type.get_length() for field_type in self.field_types)

    def equals(self, other: 'TupleDesc') -> bool:
        """
        Check if two tuple descriptors are equivalent.

        Two TupleDescs are equal if they have the same field types in the same order.
        Field names are NOT considered for equality (schema compatibility).
        """
        if not isinstance(other, TupleDesc):
            return False
        return self.field_types == other.field_types

    @staticmethod
    def combine(td1: 'TupleDesc', td2: 'TupleDesc') -> 'TupleDesc':
        """
        Combine two tuple descriptors into one.

        The result has fields from td1 followed by fields from td2.
        Used for join operations where we concatenate tuples.
        """
        combined_types = td1.field_types + td2.field_types

        combined_names = None
        if td1.field_names is not None and td2.field_names is not None:
            combined_names = td1.field_names + td2.field_names
        elif td1.field_names is not None:
            combined_names = td1.field_names + [None] * td2.num_fields()
        elif td2.field_names is not None:
            combined_names = [None] * td1.num_fields() + td2.field_names

        return TupleDesc(combined_types, combined_names)

    def __eq__(self, other: object) -> bool:
        """Python equality operator - delegates to equals method."""
        return isinstance(other, TupleDesc) and self.equals(other)

    def __str__(self) -> str:
        """Human-readable string representation."""
        parts = []
        for i, field_type in enumerate(self.field_types):
            name = self.get_field_name(i) if self.field_names else f"field_{i}"
            parts.append(f"{field_type.value}({name})")
        return f"TupleDesc({', '.join(parts)})"

    def __repr__(self) -> str:
        return self.__str__()
