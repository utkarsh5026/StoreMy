"""
Tests for table_info.py module.
"""

import pytest
import time
from unittest.mock import patch

from app.catalog.table_info import IndexInfo, ConstraintInfo, TableMetadata
from app.core.tuple import TupleDesc
from app.core.types import FieldType


class TestIndexInfo:
    """Test cases for IndexInfo class."""

    def test_index_info_creation(self):
        """Test creating an IndexInfo instance."""
        index = IndexInfo(
            index_name="idx_user_email",
            table_name="users",
            field_names=["email"],
            index_type="btree",
            unique=True
        )

        assert index.index_name == "idx_user_email"
        assert index.table_name == "users"
        assert index.field_names == ["email"]
        assert index.index_type == "btree"
        assert index.unique is True
        assert index.created_at > 0

    def test_index_info_defaults(self):
        """Test IndexInfo with default values."""
        index = IndexInfo(
            index_name="idx_test",
            table_name="test_table",
            field_names=["id"]
        )

        assert index.index_type == "btree"
        assert index.unique is False
        assert index.created_at > 0

    def test_index_info_created_at_auto_set(self):
        """Test that created_at is automatically set."""
        with patch('time.time', return_value=1234567890.0):
            index = IndexInfo(
                index_name="test_idx",
                table_name="test_table",
                field_names=["field1"]
            )
            assert index.created_at == pytest.approx(1234567890.0)

    def test_index_info_to_dict(self):
        """Test IndexInfo serialization to dictionary."""
        index = IndexInfo(
            index_name="idx_name",
            table_name="table1",
            field_names=["field1", "field2"],
            index_type="hash",
            unique=True,
            created_at=1000.0
        )

        expected = {
            "index_name": "idx_name",
            "table_name": "table1",
            "field_names": ["field1", "field2"],
            "index_type": "hash",
            "unique": True,
            "created_at": 1000.0
        }

        assert index.to_dict() == expected

    def test_index_info_from_dict(self):
        """Test IndexInfo deserialization from dictionary."""
        data = {
            "index_name": "idx_name",
            "table_name": "table1",
            "field_names": ["field1", "field2"],
            "index_type": "hash",
            "unique": True,
            "created_at": 1000.0
        }

        index = IndexInfo.from_dict(data)

        assert index.index_name == "idx_name"
        assert index.table_name == "table1"
        assert index.field_names == ["field1", "field2"]
        assert index.index_type == "hash"
        assert index.unique is True
        assert index.created_at == pytest.approx(1000.0)

    def test_index_info_round_trip_serialization(self):
        """Test that serialization and deserialization work together."""
        original = IndexInfo(
            index_name="idx_test",
            table_name="test_table",
            field_names=["col1", "col2"],
            index_type="btree",
            unique=False
        )

        # Serialize and deserialize
        data = original.to_dict()
        recreated = IndexInfo.from_dict(data)

        assert recreated.index_name == original.index_name
        assert recreated.table_name == original.table_name
        assert recreated.field_names == original.field_names
        assert recreated.index_type == original.index_type
        assert recreated.unique == original.unique
        assert recreated.created_at == original.created_at


class TestConstraintInfo:
    """Test cases for ConstraintInfo class."""

    def test_constraint_info_primary_key(self):
        """Test creating a primary key constraint."""
        constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )

        assert constraint.constraint_name == "pk_users"
        assert constraint.constraint_type == "PRIMARY_KEY"
        assert constraint.table_name == "users"
        assert constraint.field_names == ["id"]
        assert constraint.reference_table is None
        assert constraint.reference_fields is None
        assert constraint.check_expression is None
        assert constraint.created_at > 0

    def test_constraint_info_foreign_key(self):
        """Test creating a foreign key constraint."""
        constraint = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )

        assert constraint.constraint_name == "fk_posts_user"
        assert constraint.constraint_type == "FOREIGN_KEY"
        assert constraint.table_name == "posts"
        assert constraint.field_names == ["user_id"]
        assert constraint.reference_table == "users"
        assert constraint.reference_fields == ["id"]
        assert constraint.check_expression is None

    def test_constraint_info_check_constraint(self):
        """Test creating a check constraint."""
        constraint = ConstraintInfo(
            constraint_name="chk_age_positive",
            constraint_type="CHECK",
            table_name="users",
            field_names=["age"],
            check_expression="age > 0"
        )

        assert constraint.constraint_name == "chk_age_positive"
        assert constraint.constraint_type == "CHECK"
        assert constraint.field_names == ["age"]
        assert constraint.check_expression == "age > 0"

    def test_constraint_info_unique_constraint(self):
        """Test creating a unique constraint."""
        constraint = ConstraintInfo(
            constraint_name="uq_email",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=["email"]
        )

        assert constraint.constraint_type == "UNIQUE"
        assert constraint.field_names == ["email"]

    def test_constraint_info_created_at_auto_set(self):
        """Test that created_at is automatically set."""
        with patch('time.time', return_value=9876543210.0):
            constraint = ConstraintInfo(
                constraint_name="test_constraint",
                constraint_type="PRIMARY_KEY",
                table_name="test_table",
                field_names=["id"]
            )
            assert constraint.created_at == pytest.approx(9876543210.0)

    def test_constraint_info_to_dict(self):
        """Test ConstraintInfo serialization to dictionary."""
        constraint = ConstraintInfo(
            constraint_name="fk_test",
            constraint_type="FOREIGN_KEY",
            table_name="child_table",
            field_names=["parent_id"],
            reference_table="parent_table",
            reference_fields=["id"],
            created_at=2000.0
        )

        expected = {
            "constraint_name": "fk_test",
            "constraint_type": "FOREIGN_KEY",
            "table_name": "child_table",
            "field_names": ["parent_id"],
            "reference_table": "parent_table",
            "reference_fields": ["id"],
            "check_expression": None,
            "created_at": 2000.0
        }

        assert constraint.to_dict() == expected

    def test_constraint_info_from_dict(self):
        """Test ConstraintInfo deserialization from dictionary."""
        data = {
            "constraint_name": "pk_test",
            "constraint_type": "PRIMARY_KEY",
            "table_name": "test_table",
            "field_names": ["id"],
            "reference_table": None,
            "reference_fields": None,
            "check_expression": None,
            "created_at": 1500.0
        }

        constraint = ConstraintInfo.from_dict(data)

        assert constraint.constraint_name == "pk_test"
        assert constraint.constraint_type == "PRIMARY_KEY"
        assert constraint.table_name == "test_table"
        assert constraint.field_names == ["id"]
        assert constraint.reference_table is None
        assert constraint.reference_fields is None
        assert constraint.check_expression is None
        assert constraint.created_at == pytest.approx(1500.0)

    def test_constraint_info_round_trip_serialization(self):
        """Test that serialization and deserialization work together."""
        original = ConstraintInfo(
            constraint_name="fk_complex",
            constraint_type="FOREIGN_KEY",
            table_name="orders",
            field_names=["customer_id", "product_id"],
            reference_table="customers",
            reference_fields=["id", "product_id"]
        )

        # Serialize and deserialize
        data = original.to_dict()
        recreated = ConstraintInfo.from_dict(data)

        assert recreated.constraint_name == original.constraint_name
        assert recreated.constraint_type == original.constraint_type
        assert recreated.table_name == original.table_name
        assert recreated.field_names == original.field_names
        assert recreated.reference_table == original.reference_table
        assert recreated.reference_fields == original.reference_fields
        assert recreated.check_expression == original.check_expression
        assert recreated.created_at == original.created_at


class TestTableMetadata:
    """Test cases for TableMetadata class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tuple_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING, FieldType.BOOLEAN],
            field_names=["id", "name", "active"]
        )

    def test_table_metadata_creation(self):
        """Test creating a TableMetadata instance."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1100.0,
            table_comment="User table"
        )

        assert metadata.table_name == "users"
        assert metadata.table_id == 1
        assert metadata.file_path == "/data/users.dat"
        assert metadata.tuple_desc == self.tuple_desc
        assert metadata.primary_key_fields == ["id"]
        assert metadata.created_at == pytest.approx(1000.0)
        assert metadata.modified_at == pytest.approx(1100.0)
        assert metadata.table_comment == "User table"
        assert metadata.row_count == 0
        assert metadata.file_size_bytes == 0
        assert isinstance(metadata.indexes, dict)
        assert isinstance(metadata.constraints, dict)

    def test_table_metadata_defaults(self):
        """Test TableMetadata with default values."""
        metadata = TableMetadata(
            table_name="test_table",
            table_id=2,
            file_path="/data/test.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=[],
            created_at=0,
            modified_at=0
        )

        assert metadata.row_count == 0
        assert metadata.file_size_bytes == 0
        assert metadata.indexes == {}
        assert metadata.constraints == {}
        assert metadata.table_comment == ""
        assert metadata.created_at > 0  # Auto-set in __post_init__
        assert metadata.modified_at > 0  # Auto-set in __post_init__

    def test_add_index(self):
        """Test adding an index to table metadata."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        index = IndexInfo(
            index_name="idx_name",
            table_name="users",
            field_names=["name"],
            index_type="btree",
            unique=True
        )

        original_modified = metadata.modified_at
        time.sleep(0.01)  # Ensure time difference
        metadata.add_index(index)

        assert "idx_name" in metadata.indexes
        assert metadata.indexes["idx_name"] == index
        assert metadata.modified_at > original_modified

    def test_remove_index(self):
        """Test removing an index from table metadata."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        index = IndexInfo(
            index_name="idx_name",
            table_name="users",
            field_names=["name"]
        )

        metadata.add_index(index)
        assert "idx_name" in metadata.indexes

        # Remove existing index
        result = metadata.remove_index("idx_name")
        assert result is True
        assert "idx_name" not in metadata.indexes

        # Try to remove non-existent index
        result = metadata.remove_index("non_existent")
        assert result is False

    def test_add_constraint(self):
        """Test adding a constraint to table metadata."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )

        original_modified = metadata.modified_at
        time.sleep(0.01)  # Ensure time difference
        metadata.add_constraint(constraint)

        assert "pk_users" in metadata.constraints
        assert metadata.constraints["pk_users"] == constraint
        assert metadata.modified_at > original_modified

    def test_remove_constraint(self):
        """Test removing a constraint from table metadata."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )

        metadata.add_constraint(constraint)
        assert "pk_users" in metadata.constraints

        # Remove existing constraint
        result = metadata.remove_constraint("pk_users")
        assert result is True
        assert "pk_users" not in metadata.constraints

        # Try to remove non-existent constraint
        result = metadata.remove_constraint("non_existent")
        assert result is False

    def test_get_primary_key_constraint(self):
        """Test getting the primary key constraint."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        # No primary key constraint initially
        assert metadata.get_primary_key_constraint() is None

        # Add primary key constraint
        pk_constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )
        metadata.add_constraint(pk_constraint)

        result = metadata.get_primary_key_constraint()
        assert result == pk_constraint

    def test_get_foreign_key_constraints(self):
        """Test getting foreign key constraints."""
        metadata = TableMetadata(
            table_name="posts",
            table_id=2,
            file_path="/data/posts.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        # No foreign key constraints initially
        assert metadata.get_foreign_key_constraints() == []

        # Add foreign key constraint
        fk_constraint = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )
        metadata.add_constraint(fk_constraint)

        # Add non-foreign key constraint
        pk_constraint = ConstraintInfo(
            constraint_name="pk_posts",
            constraint_type="PRIMARY_KEY",
            table_name="posts",
            field_names=["id"]
        )
        metadata.add_constraint(pk_constraint)

        fk_constraints = metadata.get_foreign_key_constraints()
        assert len(fk_constraints) == 1
        assert fk_constraints[0] == fk_constraint

    def test_update_statistics(self):
        """Test updating table statistics."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        original_modified = metadata.modified_at
        time.sleep(0.01)  # Ensure time difference

        metadata.update_statistics(100, 4096)

        assert metadata.row_count == 100
        assert metadata.file_size_bytes == 4096
        assert metadata.modified_at > original_modified

    def test_to_dict_serialization(self):
        """Test TableMetadata serialization to dictionary."""
        metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1100.0,
            row_count=50,
            file_size_bytes=2048,
            table_comment="Test table"
        )

        # Add index and constraint for complete test
        index = IndexInfo("idx_name", "users", ["name"])
        constraint = ConstraintInfo("pk_users", "PRIMARY_KEY", "users", ["id"])
        metadata.add_index(index)
        metadata.add_constraint(constraint)

        result = metadata.to_dict()

        assert result["table_name"] == "users"
        assert result["table_id"] == 1
        assert result["file_path"] == "/data/users.dat"
        assert result["primary_key_fields"] == ["id"]
        assert result["created_at"] == pytest.approx(1000.0)
        assert result["row_count"] == 50
        assert result["file_size_bytes"] == 2048
        assert result["table_comment"] == "Test table"

        # Check tuple_desc serialization
        assert "tuple_desc" in result
        assert result["tuple_desc"]["field_types"] == [
            "int", "string", "boolean"]
        assert result["tuple_desc"]["field_names"] == ["id", "name", "active"]

        # Check indexes and constraints
        assert "indexes" in result
        assert "constraints" in result
        assert "idx_name" in result["indexes"]
        assert "pk_users" in result["constraints"]

    def test_from_dict_deserialization(self):
        """Test TableMetadata deserialization from dictionary."""
        data = {
            "table_name": "users",
            "table_id": 1,
            "file_path": "/data/users.dat",
            "primary_key_fields": ["id"],
            "created_at": 1000.0,
            "modified_at": 1100.0,
            "row_count": 50,
            "file_size_bytes": 2048,
            "table_comment": "Test table",
            "tuple_desc": {
                "field_types": ["int", "string", "boolean"],
                "field_names": ["id", "name", "active"]
            },
            "indexes": {
                "idx_name": {
                    "index_name": "idx_name",
                    "table_name": "users",
                    "field_names": ["name"],
                    "index_type": "btree",
                    "unique": False,
                    "created_at": 2000.0
                }
            },
            "constraints": {
                "pk_users": {
                    "constraint_name": "pk_users",
                    "constraint_type": "PRIMARY_KEY",
                    "table_name": "users",
                    "field_names": ["id"],
                    "reference_table": None,
                    "reference_fields": None,
                    "check_expression": None,
                    "created_at": 1500.0
                }
            }
        }

        metadata = TableMetadata.from_dict(data)

        assert metadata.table_name == "users"
        assert metadata.table_id == 1
        assert metadata.file_path == "/data/users.dat"
        assert metadata.primary_key_fields == ["id"]
        assert metadata.created_at == pytest.approx(1000.0)
        assert metadata.modified_at == pytest.approx(1100.0)
        assert metadata.row_count == 50
        assert metadata.file_size_bytes == 2048
        assert metadata.table_comment == "Test table"

        # Check tuple_desc reconstruction
        assert metadata.tuple_desc.field_types == [
            FieldType.INT, FieldType.STRING, FieldType.BOOLEAN]
        assert metadata.tuple_desc.field_names == ["id", "name", "active"]

        # Check indexes and constraints reconstruction
        assert "idx_name" in metadata.indexes
        assert metadata.indexes["idx_name"].index_name == "idx_name"
        assert "pk_users" in metadata.constraints
        assert metadata.constraints["pk_users"].constraint_name == "pk_users"

    def test_round_trip_serialization(self):
        """Test that serialization and deserialization work together."""
        original = TableMetadata(
            table_name="complex_table",
            table_id=99,
            file_path="/data/complex.dat",
            tuple_desc=self.tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1100.0,
            row_count=200,
            file_size_bytes=8192,
            table_comment="Complex test table"
        )

        # Add complex constraints and indexes
        index1 = IndexInfo("idx_name", "complex_table",
                           ["name"], "btree", True)
        index2 = IndexInfo("idx_active", "complex_table",
                           ["active"], "hash", False)

        constraint1 = ConstraintInfo(
            "pk_complex", "PRIMARY_KEY", "complex_table", ["id"])
        constraint2 = ConstraintInfo(
            "uq_name", "UNIQUE", "complex_table", ["name"])

        original.add_index(index1)
        original.add_index(index2)
        original.add_constraint(constraint1)
        original.add_constraint(constraint2)

        # Serialize and deserialize
        data = original.to_dict()
        recreated = TableMetadata.from_dict(data)

        # Verify all fields match
        assert recreated.table_name == original.table_name
        assert recreated.table_id == original.table_id
        assert recreated.file_path == original.file_path
        assert recreated.primary_key_fields == original.primary_key_fields
        assert recreated.created_at == original.created_at
        assert recreated.modified_at == original.modified_at
        assert recreated.row_count == original.row_count
        assert recreated.file_size_bytes == original.file_size_bytes
        assert recreated.table_comment == original.table_comment

        # Verify tuple_desc
        assert recreated.tuple_desc.field_types == original.tuple_desc.field_types
        assert recreated.tuple_desc.field_names == original.tuple_desc.field_names

        # Verify indexes
        assert len(recreated.indexes) == len(original.indexes)
        for name, index in original.indexes.items():
            assert name in recreated.indexes
            recreated_index = recreated.indexes[name]
            assert recreated_index.index_name == index.index_name
            assert recreated_index.table_name == index.table_name
            assert recreated_index.field_names == index.field_names
            assert recreated_index.index_type == index.index_type
            assert recreated_index.unique == index.unique

        # Verify constraints
        assert len(recreated.constraints) == len(original.constraints)
        for name, constraint in original.constraints.items():
            assert name in recreated.constraints
            recreated_constraint = recreated.constraints[name]
            assert recreated_constraint.constraint_name == constraint.constraint_name
            assert recreated_constraint.constraint_type == constraint.constraint_type
            assert recreated_constraint.table_name == constraint.table_name
            assert recreated_constraint.field_names == constraint.field_names
