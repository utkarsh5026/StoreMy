from enum import Enum


class FieldType(Enum):
    """
    Enum for field types.
    """
    INT = "int"
    STRING = "string"

    def get_length(self) -> int:

        if self == FieldType.INT:
            return 4
        elif self == FieldType.STRING:
            return 132  # 4 bytes for length + 128 bytes for string content
        else:
            raise ValueError(f"Invalid field type: {self}")
