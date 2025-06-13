"""
Schema loading from various file formats.
Supports simple, SQL, and JSON schema formats.
"""
import json
from pathlib import Path
from typing import Dict, Callable

from .table_info import TableMetadata
from ..storage.heap import HeapFile
from ..core.tuple import TupleDesc
from ..core.types import FieldType
from ..core.exceptions import DbException


class SchemaLoader:
    """Loads database schemas from various file formats."""

    type_mapping = {
        'int': FieldType.INT,
        'string': FieldType.STRING,
        'boolean': FieldType.BOOLEAN,
        'float': FieldType.FLOAT,
        'double': FieldType.DOUBLE
    }

    def __init__(self, add_table_callback: Callable):
        """
        Initialize schema loader.

        Args:
            add_table_callback: Function to call when adding tables
        """
        self.add_table_callback = add_table_callback
        self._format_loaders = {
            "simple": self._load_simple_schema,
            "sql": self._load_sql_schema,
            "json": self._load_json_schema
        }

    def load_schema_file(self, schema_file: str, format_type: str = "simple") -> None:
        """Load schema from various file formats."""
        schema_path = Path(schema_file)
        if not schema_path.exists():
            raise DbException(f"Schema file not found: {schema_file}")

        if format_type not in self._format_loaders:
            raise DbException(f"Unsupported schema format: {format_type}")

        self._format_loaders[format_type](schema_path)

    def _load_simple_schema(self, schema_path: Path) -> None:
        """Load simple schema format: table_name (field type, field type, ...)"""
        with open(schema_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                try:
                    self._parse_simple_table_definition(line)
                except Exception as e:
                    raise DbException(f"Error parsing line {line_num}: {e}")

    def _parse_simple_table_definition(self, line: str) -> None:
        """Parse: table_name (field_name type, field_name type pk, ...)"""
        paren_pos = line.index('(')
        table_name = line[:paren_pos].strip()

        fields_str = line[paren_pos + 1:line.rindex(')')].strip()
        field_definitions = [f.strip() for f in fields_str.split(',')]

        field_names = []
        field_types = []
        primary_key_fields = []

        for field_def in field_definitions:
            parts = field_def.strip().split()
            if len(parts) < 2:
                raise ValueError(f"Invalid field definition: {field_def}")

            field_name = parts[0]
            field_type_str = parts[1].lower()

            field_names.append(field_name)

            # Map type strings to FieldType enums

            if field_type_str not in self.type_mapping:
                raise ValueError(f"Unknown type: {field_type_str}")

            field_types.append(self.type_mapping[field_type_str])

            # Check for primary key annotation
            if len(parts) == 3 and parts[2].lower() == 'pk':
                primary_key_fields.append(field_name)

        # Create table
        tuple_desc = TupleDesc(field_types, field_names)
        data_file = f"{table_name}.dat"
        heap_file = HeapFile(data_file, tuple_desc)

        self.add_table_callback(heap_file, table_name, primary_key_fields)

    def _load_sql_schema(self, schema_path: Path) -> None:
        """Load SQL DDL schema (basic implementation)."""
        # Placeholder - would parse CREATE TABLE statements
        raise NotImplementedError("SQL schema loading not yet implemented")

    def _load_json_schema(self, schema_path: Path) -> None:
        """Load JSON schema format."""
        with open(schema_path, 'r') as f:
            schema_data = json.load(f)

        for table_data in schema_data.get("tables", []):
            metadata = TableMetadata.from_dict(table_data)
            db_file = HeapFile(metadata.file_path, metadata.tuple_desc)

            self.add_table_callback(
                db_file,
                metadata.table_name,
                metadata.primary_key_fields
            )
