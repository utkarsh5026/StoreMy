"""
Table management operations.
Handles CRUD operations for tables in the catalog.
"""
import time
import threading
from typing import Dict, List, Set, Optional
from pathlib import Path

from .table_info import TableMetadata, ConstraintInfo
from .schema_validator import SchemaValidator
from ..storage.interfaces import DbFile
from ..storage.heap import HeapFile
from ..core.exceptions import DbException


class TableManager:
    """Manages table operations in the catalog."""

    def __init__(self, validator: SchemaValidator):
        self.validator = validator
        self._lock = threading.RLock()

    def add_table(self, tables: dict[str, TableMetadata], table_ids: dict[int, TableMetadata],
                  db_files: Dict[int, DbFile], db_file: DbFile, table_name: str,
                  primary_key_fields: Optional[List[str]] = None,
                  table_comment: str = "") -> TableMetadata:
        """Add a new table with validation."""
        with self._lock:
            table_id = db_file.get_id()
            tuple_desc = db_file.get_tuple_desc()

            metadata = TableMetadata(
                table_name=table_name,
                table_id=table_id,
                file_path=str(
                    getattr(db_file, 'file_path', f"{table_name}.dat")),
                tuple_desc=tuple_desc,
                primary_key_fields=primary_key_fields or [],
                created_at=time.time(),
                modified_at=time.time(),
                table_comment=table_comment
            )

            # Add primary key constraint if specified
            if primary_key_fields:
                pk_constraint = ConstraintInfo(
                    constraint_name=f"pk_{table_name}",
                    constraint_type="PRIMARY_KEY",
                    table_name=table_name,
                    field_names=primary_key_fields
                )
                metadata.add_constraint(pk_constraint)

            if not self.validator.validate_table_creation(metadata, tables):
                errors = self.validator.get_validation_errors()
                raise DbException(
                    f"Table creation failed: {'; '.join(errors)}")

            # Add to collections
            tables[table_name] = metadata
            table_ids[table_id] = metadata
            db_files[table_id] = db_file

            return metadata

    def drop_table(self, tables: Dict[str, TableMetadata], table_ids: Dict[int, TableMetadata],
                   db_files: Dict[int, DbFile], table_name: str, cascade: bool = False) -> bool:
        """Drop a table from the catalog."""
        with self._lock:
            if table_name not in tables:
                return False

            metadata = tables[table_name]
            table_id = metadata.table_id

            # Check for foreign key dependencies
            dependencies = self._find_dependencies(table_name, tables)
            if dependencies and not cascade:
                raise DbException(
                    f"Cannot drop table '{table_name}': referenced by {dependencies}")

            if cascade and dependencies:
                # Drop dependent tables first
                for dep_table in dependencies:
                    self.drop_table(tables, table_ids, db_files,
                                    dep_table, cascade=True)

            # Remove from collections
            del tables[table_name]
            del table_ids[table_id]
            if table_id in db_files:
                del db_files[table_id]

            return True

    def get_or_create_db_file(self, table_id: int, table_ids: Dict[int, TableMetadata],
                              db_files: Dict[int, DbFile]) -> DbFile:
        """Get DbFile for a table, creating if necessary."""
        if table_id not in db_files:
            if table_id in table_ids:
                metadata = table_ids[table_id]
                db_file = HeapFile(metadata.file_path, metadata.tuple_desc)
                db_files[table_id] = db_file
                return db_file
            else:
                raise DbException(
                    f"Table with ID {table_id} not found in catalog")
        return db_files[table_id]

    def _find_dependencies(self, table_name: str, tables: Dict[str, TableMetadata]) -> Set[str]:
        """Find tables that depend on the given table via foreign keys."""
        dependencies = set()

        for name, metadata in tables.items():
            if name == table_name:
                continue

            for constraint in metadata.get_foreign_key_constraints():
                if constraint.reference_table == table_name:
                    dependencies.add(name)

        return dependencies

    def update_table_statistics(self, metadata: TableMetadata) -> None:
        """Update statistics for a table."""
        try:
            file_path = Path(metadata.file_path)
            if file_path.exists():
                metadata.file_size_bytes = file_path.stat().st_size
        except Exception:
            # Ignore statistics update failures
            pass
