import threading
from typing import Dict, Iterator
from pathlib import Path

from ..storage.file import DbFile, HeapFile
from ..core.tuple import TupleDesc
from ..core.exceptions import DbException
from ..core.types import Type


class Catalog:
    """
    The Catalog keeps track of all available tables in the database and their
    associated schemas.

    For now, this is a simple in-memory catalog. In a production system,
    this would persist metadata to disk and support more complex operations.

    The catalog manages:
    1. Table name -> DbFile mapping
    2. Table ID -> DbFile mapping
    3. Table ID -> primary key mapping
    4. Schema information for each table
    """

    def __init__(self):
        """Create a new, empty catalog."""
        # Maps table name to DbFile
        self._name_to_file: Dict[str, DbFile] = {}

        # Maps table ID to DbFile
        self._id_to_file: Dict[int, DbFile] = {}

        # Maps table ID to primary key field name
        self._id_to_primary_key: Dict[int, str] = {}

        # Thread safety
        self._lock = threading.RLock()

    def add_table(self, db_file: DbFile, name: str, primary_key_field: str = "") -> None:
        """
        Add a new table to the catalog.

        Args:
            db_file: The DbFile containing the table data
            name: The name of the table (may not be None)
            primary_key_field: The name of the primary key field
        """
        with self._lock:
            table_id = db_file.get_id()

            self._name_to_file[name] = db_file
            self._id_to_file[table_id] = db_file
            self._id_to_primary_key[table_id] = primary_key_field

    def get_table_id(self, name: str) -> int:
        """
        Return the id of the table with the specified name.

        Args:
            name: The table name

        Returns:
            The table ID

        Raises:
            DbException: If the table doesn't exist
        """
        with self._lock:
            if name not in self._name_to_file:
                raise DbException(f"Table '{name}' not found in catalog")

            return self._name_to_file[name].get_id()

    def get_tuple_desc(self, table_id: int) -> TupleDesc:
        """
        Return the tuple descriptor (schema) of the specified table.

        Args:
            table_id: The table ID

        Returns:
            The TupleDesc for the table

        Raises:
            DbException: If the table doesn't exist
        """
        db_file = self.get_db_file(table_id)
        return db_file.get_tuple_desc()

    def get_db_file(self, table_id: int) -> DbFile:
        """
        Return the DbFile that can be used to read the contents of the specified table.

        Args:
            table_id: The table ID

        Returns:
            The DbFile for the table

        Raises:
            DbException: If the table doesn't exist
        """
        with self._lock:
            if table_id not in self._id_to_file:
                raise DbException(
                    f"Table with ID {table_id} not found in catalog")

            return self._id_to_file[table_id]

    def get_primary_key(self, table_id: int) -> str:
        """
        Return the primary key field name for the specified table.

        Args:
            table_id: The table ID

        Returns:
            The primary key field name (may be empty string)
        """
        with self._lock:
            return self._id_to_primary_key.get(table_id, "")

    def get_table_name(self, table_id: int) -> str:
        """
        Return the name of the table with the specified ID.

        Args:
            table_id: The table ID

        Returns:
            The table name

        Raises:
            DbException: If the table doesn't exist
        """
        with self._lock:
            for name, db_file in self._name_to_file.items():
                if db_file.get_id() == table_id:
                    return name

            raise DbException(f"Table with ID {table_id} not found in catalog")

    def table_id_iterator(self) -> Iterator[int]:
        """
        Return an iterator over all table IDs in the catalog.

        Returns:
            Iterator over table IDs
        """
        with self._lock:
            return iter(list(self._id_to_file.keys()))

    def clear(self) -> None:
        """Delete all tables from the catalog."""
        with self._lock:
            self._name_to_file.clear()
            self._id_to_file.clear()
            self._id_to_primary_key.clear()

    def load_schema(self, catalog_file: str) -> None:
        """
        Read the schema from a file and create the appropriate tables.

        The catalog file format is:
        table_name (field_name type, field_name type, ...)

        Example:
        students (id int, name string, age int)
        courses (id int, title string, credits int)

        Args:
            catalog_file: Path to the catalog file
        """
        catalog_path = Path(catalog_file)
        if not catalog_path.exists():
            raise DbException(f"Catalog file not found: {catalog_file}")

        with open(catalog_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue  # Skip empty lines and comments

                try:
                    self._parse_table_definition(line)
                except Exception as e:
                    raise DbException(
                        f"Error parsing line {line_num} in {catalog_file}: {e}")

    def _parse_table_definition(self, line: str) -> None:
        """
        Parse a single table definition line.

        Format: table_name (field_name type, field_name type, ...)
        """
        # Extract table name
        paren_pos = line.index('(')
        table_name = line[:paren_pos].strip()

        # Extract field definitions
        fields_str = line[paren_pos + 1:line.rindex(')')].strip()
        field_definitions = [f.strip() for f in fields_str.split(',')]

        # Parse field types and names
        field_names = []
        field_types = []
        primary_key = ""

        for field_def in field_definitions:
            parts = field_def.strip().split()
            if len(parts) < 2:
                raise ValueError(f"Invalid field definition: {field_def}")

            field_name = parts[0]
            field_type_str = parts[1].lower()

            field_names.append(field_name)

            # Convert type string to Type enum
            if field_type_str == 'int':
                field_types.append(Type.INT_TYPE)
            elif field_type_str == 'string':
                field_types.append(Type.STRING_TYPE)
            else:
                raise ValueError(f"Unknown type: {field_type_str}")

            # Check for primary key annotation
            if len(parts) == 3 and parts[2].lower() == 'pk':
                primary_key = field_name

        # Create TupleDesc
        tuple_desc = TupleDesc(field_types, field_names)

        # Create HeapFile
        data_file = f"{table_name}.dat"
        heap_file = HeapFile(data_file, tuple_desc)

        # Add to catalog
        self.add_table(heap_file, table_name, primary_key)

        print(f"Added table: {table_name} with schema {tuple_desc}")
