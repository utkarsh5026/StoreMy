from enum import Enum


class FieldType(Enum):
    """
    Enum for field types.
    """
    INT = "int"
    STRING = "string"
    BOOLEAN = "boolean"
    FLOAT = "float"
    DOUBLE = "double"

    def get_length(self) -> int:
        """Get the length of the field type in bytes."""
        length_map = {
            FieldType.INT: 4,
            FieldType.STRING: 132,
            FieldType.BOOLEAN: 1,
            FieldType.FLOAT: 4,
            FieldType.DOUBLE: 8,
        }

        return length_map[self]
