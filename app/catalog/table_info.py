import time
from dataclasses import dataclass, asdict
from typing import List, Optional

from ..core.tuple import TupleDesc
from ..core.types import FieldType


@dataclass
class IndexInfo:
    """
    Information about an index on a table.

    🏷️ Represents metadata about a database index including its type,
    covered fields, and uniqueness constraints.
    """

    """🏷️ Unique name identifying this index"""
    index_name: str

    """📋 Name of the table this index belongs to"""
    table_name: str

    """🔑 List of field names covered by this index"""
    field_names: list[str]

    """🌳 Type of index (btree, hash, etc.)"""
    index_type: str = "btree"

    """⭐ Whether this index enforces uniqueness"""
    unique: bool = False

    """⏰ Unix timestamp when index was created"""
    created_at: float = 0.0

    def __post_init__(self):
        """
        🎬 Initialize index creation timestamp if not provided.
        """
        if self.created_at == 0:
            self.created_at = time.time()

    def to_dict(self) -> dict:
        """
        📦 Convert index info to dictionary format for serialization.

        Returns:
            dict: Dictionary representation of the index
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'IndexInfo':
        """
        📥 Create index info from dictionary representation.

        Args:
            data: Dictionary containing index attributes

        Returns:
            IndexInfo: New index info instance
        """
        return cls(**data)


@dataclass
class ConstraintInfo:
    """
    Information about table constraints.

    🔒 Represents metadata about database constraints including primary keys,
    foreign keys, unique constraints and check constraints.
    """
    constraint_name: str

    """🔒 Type of constraint (PRIMARY_KEY, FOREIGN_KEY, UNIQUE, CHECK)"""
    constraint_type: str

    """📋 Name of the table this constraint belongs to"""
    table_name: str

    """🔑 List of field names involved in constraint"""
    field_names: list[str]

    """🔗 Referenced table for foreign keys"""
    reference_table: Optional[str] = None

    """🔗 Referenced fields for foreign keys"""
    reference_fields: Optional[list[str]] = None

    """✅ Expression for check constraints"""
    check_expression: Optional[str] = None

    """⏰ Unix timestamp when constraint was created"""
    created_at: float = 0.0

    def __post_init__(self):
        """
        🎬 Initialize constraint creation timestamp if not provided.
        """
        if self.created_at == 0:
            self.created_at = time.time()

    def to_dict(self) -> dict:
        """
        📦 Convert constraint info to dictionary format for serialization.

        Returns:
            dict: Dictionary representation of the constraint
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'ConstraintInfo':
        """
        📥 Create constraint info from dictionary representation.

        Args:
            data: Dictionary containing constraint attributes

        Returns:
            ConstraintInfo: New constraint info instance
        """
        return cls(**data)


@dataclass
class TableMetadata:
    """
    Complete metadata for a database table.

    📚 Represents all metadata associated with a database table including:
    - Basic properties (name, id, file location)
    - Schema information (field types and names)
    - Constraints and indexes
    - Statistical information
    """

    """📋 Name of the table"""
    table_name: str

    """� Unique numeric identifier for the table"""
    table_id: int

    """📂 Path to the table's data file"""
    file_path: str

    """📝 Schema description (field types and names)"""
    tuple_desc: TupleDesc

    """🔑 List of fields forming primary key"""
    primary_key_fields: list[str]

    """⏰ Unix timestamp when table was created"""
    created_at: float

    """🔄 Unix timestamp of last modification"""
    modified_at: float

    """📊 Number of rows in the table"""
    row_count: int = 0

    """💾 Size of table data file in bytes"""
    file_size_bytes: int = 0

    """📇 Map of index name to index info"""
    indexes: Optional[dict[str, IndexInfo]] = None

    """🔐 Map of constraint name to constraint info"""
    constraints: Optional[dict[str, ConstraintInfo]] = None

    """💬 Optional description of the table"""
    table_comment: str = ""

    def __post_init__(self):
        """
        🎬 Initialize collections and timestamps if not provided.
        """
        if self.indexes is None:
            self.indexes = {}
        if self.constraints is None:
            self.constraints = {}
        if self.created_at == 0:
            self.created_at = time.time()
        if self.modified_at == 0:
            self.modified_at = time.time()

    def add_index(self, index_info: IndexInfo) -> None:
        """
        📇 Add an index to this table.

        Args:
            index_info: Index metadata to add
        """
        self.indexes[index_info.index_name] = index_info
        self.modified_at = time.time()

    def remove_index(self, index_name: str) -> bool:
        """
        🗑️ Remove an index from this table.

        Args:
            index_name: Name of index to remove

        Returns:
            bool: True if index was removed, False if not found
        """
        if index_name in self.indexes:
            del self.indexes[index_name]
            self.modified_at = time.time()
            return True
        return False

    def add_constraint(self, constraint_info: ConstraintInfo) -> None:
        """
        🔒 Add a constraint to this table.

        Args:
            constraint_info: Constraint metadata to add
        """
        self.constraints[constraint_info.constraint_name] = constraint_info
        self.modified_at = time.time()

    def remove_constraint(self, constraint_name: str) -> bool:
        """
        🗑️ Remove a constraint from this table.

        Args:
            constraint_name: Name of constraint to remove

        Returns:
            bool: True if constraint was removed, False if not found
        """
        if constraint_name in self.constraints:
            del self.constraints[constraint_name]
            self.modified_at = time.time()
            return True
        return False

    def get_primary_key_constraint(self) -> Optional[ConstraintInfo]:
        """
        🔑 Get the primary key constraint for this table.

        Returns:
            Optional[ConstraintInfo]: Primary key constraint if it exists
        """
        for constraint in self.constraints.values():
            if constraint.constraint_type == "PRIMARY_KEY":
                return constraint
        return None

    def get_foreign_key_constraints(self) -> List[ConstraintInfo]:
        """
        🔗 Get all foreign key constraints for this table.

        Returns:
            List[ConstraintInfo]: List of foreign key constraints
        """
        return [c for c in self.constraints.values()
                if c.constraint_type == "FOREIGN_KEY"]

    def update_statistics(self, row_count: int, file_size: int) -> None:
        """
        📊 Update table statistics.

        Args:
            row_count: New number of rows
            file_size: New file size in bytes
        """
        self.row_count = row_count
        self.file_size_bytes = file_size
        self.modified_at = time.time()

    def to_dict(self) -> dict:
        """
        📦 Convert to dictionary for JSON serialization.

        Returns:
            dict: Dictionary representation of table metadata
        """
        result = {
            "table_name": self.table_name,
            "table_id": self.table_id,
            "file_path": self.file_path,
            "primary_key_fields": self.primary_key_fields,
            "created_at": self.created_at,
            "modified_at": self.modified_at,
            "row_count": self.row_count,
            "file_size_bytes": self.file_size_bytes,
            "table_comment": self.table_comment,
            "indexes": {name: idx.to_dict() for name, idx in self.indexes.items()},
            "constraints": {name: c.to_dict() for name, c in self.constraints.items()},
            # Serialize TupleDesc
            "tuple_desc": {
                "field_types": [ft.value for ft in self.tuple_desc.field_types],
                "field_names": self.tuple_desc.field_names
            }
        }
        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'TableMetadata':
        """
        📥 Create from dictionary loaded from JSON.

        Args:
            data: Dictionary containing table metadata

        Returns:
            TableMetadata: New table metadata instance
        """
        # Reconstruct TupleDesc
        desc_data = data["tuple_desc"]
        field_types = [FieldType(ft) for ft in desc_data["field_types"]]
        tuple_desc = TupleDesc(field_types, desc_data["field_names"])

        # Reconstruct indexes
        indexes = {}
        for name, idx_data in data.get("indexes", {}).items():
            indexes[name] = IndexInfo.from_dict(idx_data)


        constraints = {}
        for name, c_data in data.get("constraints", {}).items():
            constraints[name] = ConstraintInfo.from_dict(c_data)

        return cls(
            table_name=data["table_name"],
            table_id=data["table_id"],
            file_path=data["file_path"],
            tuple_desc=tuple_desc,
            primary_key_fields=data["primary_key_fields"],
            created_at=data["created_at"],
            modified_at=data["modified_at"],
            row_count=data.get("row_count", 0),
            file_size_bytes=data.get("file_size_bytes", 0),
            indexes=indexes,
            constraints=constraints,
            table_comment=data.get("table_comment", "")
        )
