from pathlib import Path
from .index_file import  IndexFile
from ...primitives import TransactionId, RecordId
from typing import List, Optional, TYPE_CHECKING
from ...core.types import Field, FieldType

class IndexManager:
    """
    Manages all indexes for the database.

    Responsibilities:
    1. Create/drop indexes
    2. Maintain index-table relationships
    3. Update indexes when heap data changes
    4. Query planning with index statistics

    Integration with existing components:
    - Catalog: Stores index metadata
    - BufferPool: Caches index pages
    - Query Operators: Use indexes for efficient access
    """

    def __init__(self, index_directory: str = "indexes"):
        """
        Create an index manager.

        Args:
            index_directory: Directory to store index files
        """
        self.index_dir = Path(index_directory)
        self.index_dir.mkdir(exist_ok=True)

        # Map: table_id -> {field_name -> IndexFile}
        self.table_indexes: dict[int, dict[str, IndexFile]] = {}

        # Map: index_name -> IndexFile (for named indexes)
        self.named_indexes: dict[str, IndexFile] = {}

    def create_index(self, table_id: int, field_name: str, field_type: FieldType,
                     index_type: str = "btree", index_name: str = None) -> IndexFile:
        """
        Create a new index on a table field.

        Args:
            table_id: ID of table to index
            field_name: Name of field to index
            field_type: Type of field being indexed
            index_type: Type of index ("btree", "hash")
            index_name: Optional name for the index

        Returns:
            The created IndexFile
        """
        # Generate index file path
        if index_name:
            file_path = self.index_dir / f"{index_name}.idx"
        else:
            file_path = self.index_dir / f"table_{table_id}_{field_name}.idx"

        # Create appropriate index file
        if index_type.lower() == "btree":
            from .btree.btree_file import BTreeFile
            index_file = BTreeFile(str(file_path), field_type)
        elif index_type.lower() == "hash":
            from .hash_file import HashFile
            index_file = HashFile(str(file_path), field_type)
        else:
            raise ValueError(f"Unknown index type: {index_type}")

        # Register index
        if table_id not in self.table_indexes:
            self.table_indexes[table_id] = {}
        self.table_indexes[table_id][field_name] = index_file

        if index_name:
            self.named_indexes[index_name] = index_file

        return index_file

    def get_index(self, table_id: int, field_name: str) -> Optional[IndexFile]:
        """Get index for a table field."""
        if table_id in self.table_indexes:
            return self.table_indexes[table_id].get(field_name)
        return None

    def get_named_index(self, index_name: str) -> Optional[IndexFile]:
        """Get index by name."""
        return self.named_indexes.get(index_name)

    def has_index(self, table_id: int, field_name: str) -> bool:
        """Check if an index exists for a table field."""
        return self.get_index(table_id, field_name) is not None

    def list_table_indexes(self, table_id: int) -> dict[str, IndexFile]:
        """Get all indexes for a table."""
        return self.table_indexes.get(table_id, {}).copy()

    def update_indexes_on_insert(self, tid: TransactionId, table_id: int,
                                 tuple_desc: TupleDesc, tuple_fields: List[Field],
                                 record_id: RecordId) -> None:
        """
        Update all indexes when a tuple is inserted.

        This should be called by HeapFile.add_tuple() to maintain index consistency.
        """
        if table_id not in self.table_indexes:
            return

        field_names = tuple_desc.field_names or []

        for field_name, index_file in self.table_indexes[table_id].items():
            # Find field index
            try:
                field_index = field_names.index(field_name)
                key = tuple_fields[field_index]
                index_file.insert_entry(tid, key, record_id)
            except ValueError:
                # Field not found in tuple - should not happen
                print(f"Warning: Field {field_name} not found in tuple")

    def update_indexes_on_delete(self, tid: TransactionId, table_id: int,
                                 tuple_desc: TupleDesc, tuple_fields: List[Field],
                                 record_id: RecordId) -> None:
        """
        Update all indexes when a tuple is deleted.

        This should be called by HeapFile.delete_tuple() to maintain index consistency.
        """
        if table_id not in self.table_indexes:
            return

        field_names = tuple_desc.field_names or []

        for field_name, index_file in self.table_indexes[table_id].items():
            # Find field index
            try:
                field_index = field_names.index(field_name)
                key = tuple_fields[field_index]
                index_file.delete_entry(tid, key, record_id)
            except ValueError:
                # Field not found in tuple - should not happen
                print(f"Warning: Field {field_name} not found in tuple")

    def drop_index(self, table_id: int, field_name: str) -> bool:
        """
        Drop an index.

        Returns:
            True if index was dropped, False if not found
        """
        if table_id not in self.table_indexes:
            return False

        if field_name not in self.table_indexes[table_id]:
            return False

        # Remove from mappings
        index_file = self.table_indexes[table_id][field_name]
        del self.table_indexes[table_id][field_name]

        # Remove from named indexes if present
        for name, file_obj in list(self.named_indexes.items()):
            if file_obj is index_file:
                del self.named_indexes[name]

        # Clean up empty table entry
        if not self.table_indexes[table_id]:
            del self.table_indexes[table_id]

        # Delete physical file
        try:
            Path(index_file.file_path).unlink()
        except FileNotFoundError:
            pass

        return True

    def rebuild_index(self, table_id: int, field_name: str) -> None:
        """
        Rebuild an index by scanning the entire table.

        Useful for:
        - Index corruption recovery
        - Changing index type
        - Initial index creation on existing data
        """
        index_file = self.get_index(table_id, field_name)
        if not index_file:
            raise ValueError(f"No index found for table {table_id}, field {field_name}")

        # Clear existing index
        # Implementation would scan heap file and rebuild index
        # This requires integration with HeapFile and query operators
        raise NotImplementedError("Index rebuilding requires heap file integration")

    def get_index_statistics(self, table_id: int, field_name: str) -> dict:
        """
        Get statistics about an index.

        Returns:
            Dictionary with index statistics
        """
        index_file = self.get_index(table_id, field_name)
        if not index_file:
            return {}

        return {
            "index_type": type(index_file).__name__,
            "key_type": index_file.key_type.value,
            "file_path": index_file.file_path,
            # Additional statistics would be implemented in specific index types
        }

    def close_all_indexes(self) -> None:
        """Close all open index files."""
        for table_indexes in self.table_indexes.values():
            for index_file in table_indexes.values():
                if hasattr(index_file, 'close'):
                    index_file.close()

    def __str__(self) -> str:
        total_indexes = sum(len(indexes) for indexes in self.table_indexes.values())
        return f"IndexManager({total_indexes} indexes on {len(self.table_indexes)} tables)"

    def __repr__(self) -> str:
        return self.__str__()
