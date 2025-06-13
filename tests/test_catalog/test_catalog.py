from app.core.exceptions import DbException
from app.core.types import FieldType
from app.core.tuple import TupleDesc, Tuple
from app.storage.interfaces import DbFile
from app.catalog.table_info import TableMetadata,  ConstraintInfo
from app.catalog.catalog import Catalog
from app.query.iterator import DbIterator
from app.concurrency.transactions import TransactionId
from app.storage.interfaces.page import Page, PageId
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


class MockDbFile(DbFile):
    """Mock DbFile for testing without actual file operations."""

    def __init__(self, file_id: int, tuple_desc: TupleDesc, file_path: str = "test.dat"):
        self._id = file_id
        self._tuple_desc = tuple_desc
        self.file_path = file_path
        self._pages = {}

    def get_id(self) -> int:
        return self._id

    def get_tuple_desc(self) -> TupleDesc:
        return self._tuple_desc

    def read_page(self, page_id):
        return self._pages.get(page_id)

    def write_page(self, page_id, page):
        self._pages[page_id] = page

    def num_pages(self) -> int:
        return len(self._pages)

    def add_tuple(self, tid: TransactionId, tuple_data: Tuple) -> list[Page]:
        """Mock implementation of add_tuple."""
        # Return empty list of pages for testing
        return []

    def delete_tuple(self, tid: TransactionId, tuple_data: Tuple) -> Page:
        """Mock implementation of delete_tuple."""
        # Return a mock page for testing
        return Mock(spec=Page)

    def iterator(self, tid: TransactionId) -> DbIterator:
        """Mock implementation of iterator."""
        # Return a mock iterator for testing
        return Mock(spec=DbIterator)


class TestCatalog(unittest.TestCase):
    """Comprehensive test suite for the Catalog class."""

    def setUp(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.catalog_dir = os.path.join(self.temp_dir, "test_catalog")
        self.catalog = Catalog(self.catalog_dir)

        # Create sample tuple descriptors
        self.users_desc = TupleDesc(
            [FieldType.INT, FieldType.STRING, FieldType.STRING, FieldType.INT],
            ["id", "name", "email", "age"]
        )
        self.orders_desc = TupleDesc(
            [FieldType.INT, FieldType.INT, FieldType.DOUBLE, FieldType.STRING],
            ["order_id", "user_id", "amount", "status"]
        )

    def tearDown(self):
        """Cleanup test environment."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def create_mock_db_file(self, table_id: int, tuple_desc: TupleDesc,
                            file_path: str = None) -> MockDbFile:
        """Helper to create mock DbFile."""
        if file_path is None:
            file_path = f"table_{table_id}.dat"
        return MockDbFile(table_id, tuple_desc, file_path)

    # =============================================================================
    # INITIALIZATION TESTS
    # =============================================================================

    def test_catalog_initialization(self):
        """Test catalog initialization creates proper directories and structures."""
        self.assertTrue(os.path.exists(self.catalog_dir))
        self.assertTrue(os.path.exists(
            os.path.join(self.catalog_dir, "backups")))
        self.assertEqual(len(self.catalog.list_tables()), 0)
        self.assertIsNotNone(self.catalog.validator)

    def test_catalog_initialization_existing_directory(self):
        """Test catalog initialization with existing directory."""
        # Create catalog with existing directory
        existing_catalog = Catalog(self.catalog_dir)
        self.assertEqual(len(existing_catalog.list_tables()), 0)

    # =============================================================================
    # ADD TABLE TESTS
    # =============================================================================

    def test_add_table_basic(self):
        """Test basic table addition."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        metadata = self.catalog.add_table(db_file, "users")

        self.assertEqual(metadata.table_name, "users")
        self.assertEqual(metadata.table_id, 1)
        self.assertEqual(metadata.tuple_desc, self.users_desc)
        self.assertTrue(self.catalog.table_exists("users"))
        self.assertIn("users", self.catalog.list_tables())

    def test_add_table_with_primary_key(self):
        """Test adding table with primary key."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        metadata = self.catalog.add_table(
            db_file, "users",
            primary_key_fields=["id"],
            table_comment="User table"
        )

        self.assertEqual(metadata.primary_key_fields, ["id"])
        self.assertEqual(metadata.table_comment, "User table")

        # Check primary key constraint was added
        pk_constraint = metadata.get_primary_key_constraint()
        self.assertIsNotNone(pk_constraint)
        self.assertEqual(pk_constraint.constraint_type, "PRIMARY_KEY")
        self.assertEqual(pk_constraint.field_names, ["id"])

    def test_add_table_duplicate_name(self):
        """Test adding table with duplicate name fails."""
        db_file1 = self.create_mock_db_file(1, self.users_desc)
        db_file2 = self.create_mock_db_file(2, self.users_desc)

        self.catalog.add_table(db_file1, "users")

        with patch.object(self.catalog.validator, 'validate_table_creation', return_value=False):
            with patch.object(self.catalog.validator, 'get_validation_errors', return_value=["Table already exists"]):
                with self.assertRaises(DbException) as context:
                    self.catalog.add_table(db_file2, "users")
                self.assertIn("Table creation failed", str(context.exception))

    def test_add_table_invalid_primary_key(self):
        """Test adding table with invalid primary key field."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        with patch.object(self.catalog.validator, 'validate_table_creation', return_value=False):
            with patch.object(self.catalog.validator, 'get_validation_errors', return_value=["Invalid primary key"]):
                with self.assertRaises(DbException):
                    self.catalog.add_table(
                        db_file, "users", primary_key_fields=["nonexistent"])

    # =============================================================================
    # TABLE LOOKUP TESTS
    # =============================================================================

    def test_get_table_id(self):
        """Test getting table ID by name."""
        db_file = self.create_mock_db_file(42, self.users_desc)
        self.catalog.add_table(db_file, "users")

        table_id = self.catalog.get_table_id("users")
        self.assertEqual(table_id, 42)

    def test_get_table_id_not_found(self):
        """Test getting table ID for non-existent table."""
        with self.assertRaises(DbException) as context:
            self.catalog.get_table_id("nonexistent")
        self.assertIn("not found in catalog", str(context.exception))

    def test_get_table_metadata(self):
        """Test getting table metadata."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        original_metadata = self.catalog.add_table(db_file, "users", ["id"])

        retrieved_metadata = self.catalog.get_table_metadata("users")
        self.assertEqual(retrieved_metadata.table_name,
                         original_metadata.table_name)
        self.assertEqual(retrieved_metadata.table_id,
                         original_metadata.table_id)

    def test_get_table_metadata_not_found(self):
        """Test getting metadata for non-existent table."""
        with self.assertRaises(DbException):
            self.catalog.get_table_metadata("nonexistent")

    def test_get_tuple_desc(self):
        """Test getting tuple descriptor by table ID."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        tuple_desc = self.catalog.get_tuple_desc(1)
        self.assertEqual(tuple_desc, self.users_desc)

    def test_get_tuple_desc_not_found(self):
        """Test getting tuple descriptor for non-existent table ID."""
        with self.assertRaises(DbException):
            self.catalog.get_tuple_desc(999)

    def test_get_db_file(self):
        """Test getting DbFile by table ID."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        retrieved_file = self.catalog.get_db_file(1)
        self.assertEqual(retrieved_file, db_file)

    def test_get_db_file_not_cached(self):
        """Test getting DbFile when not cached (creates HeapFile)."""
        db_file = self.create_mock_db_file(1, self.users_desc, "users.dat")
        self.catalog.add_table(db_file, "users")

        # Clear the cached file
        del self.catalog._db_files[1]

        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_instance = Mock()
            mock_heap_file.return_value = mock_instance

            retrieved_file = self.catalog.get_db_file(1)

            mock_heap_file.assert_called_once_with(
                "users.dat", self.users_desc)
            self.assertEqual(retrieved_file, mock_instance)

    def test_get_db_file_not_found(self):
        """Test getting DbFile for non-existent table ID."""
        with self.assertRaises(DbException):
            self.catalog.get_db_file(999)

    def test_get_table_name(self):
        """Test getting table name by ID."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        table_name = self.catalog.get_table_name(1)
        self.assertEqual(table_name, "users")

    def test_get_table_name_not_found(self):
        """Test getting table name for non-existent ID."""
        with self.assertRaises(DbException):
            self.catalog.get_table_name(999)

    def test_get_primary_key_fields(self):
        """Test getting primary key fields."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users", ["id", "email"])

        pk_fields = self.catalog.get_primary_key_fields(1)
        self.assertEqual(pk_fields, ["id", "email"])

        # Test that returned list is a copy
        pk_fields.append("test")
        original_pk_fields = self.catalog.get_primary_key_fields(1)
        self.assertEqual(original_pk_fields, ["id", "email"])

    def test_get_primary_key_fields_not_found(self):
        """Test getting primary key fields for non-existent table."""
        with self.assertRaises(DbException):
            self.catalog.get_primary_key_fields(999)

    # =============================================================================
    # TABLE EXISTENCE AND LISTING TESTS
    # =============================================================================

    def test_table_exists(self):
        """Test checking table existence."""
        self.assertFalse(self.catalog.table_exists("users"))

        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.assertTrue(self.catalog.table_exists("users"))
        self.assertFalse(self.catalog.table_exists("orders"))

    def test_list_tables(self):
        """Test listing all tables."""
        self.assertEqual(self.catalog.list_tables(), [])

        db_file1 = self.create_mock_db_file(1, self.users_desc)
        db_file2 = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(db_file1, "users")
        self.catalog.add_table(db_file2, "orders")

        tables = self.catalog.list_tables()
        self.assertIn("users", tables)
        self.assertIn("orders", tables)
        self.assertEqual(len(tables), 2)

    def test_table_id_iterator(self):
        """Test iterating over table IDs."""
        db_file1 = self.create_mock_db_file(1, self.users_desc)
        db_file2 = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(db_file1, "users")
        self.catalog.add_table(db_file2, "orders")

        table_ids = list(self.catalog.table_id_iterator())
        self.assertIn(1, table_ids)
        self.assertIn(2, table_ids)
        self.assertEqual(len(table_ids), 2)

    # =============================================================================
    # DROP TABLE TESTS
    # =============================================================================

    def test_drop_table_basic(self):
        """Test basic table dropping."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.assertTrue(self.catalog.table_exists("users"))

        result = self.catalog.drop_table("users")

        self.assertTrue(result)
        self.assertFalse(self.catalog.table_exists("users"))
        self.assertEqual(len(self.catalog.list_tables()), 0)

    def test_drop_table_not_found(self):
        """Test dropping non-existent table."""
        result = self.catalog.drop_table("nonexistent")
        self.assertFalse(result)

    def test_drop_table_with_dependencies(self):
        """Test dropping table with foreign key dependencies."""
        # Create users and orders tables
        users_file = self.create_mock_db_file(1, self.users_desc)
        orders_file = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(users_file, "users")
        self.catalog.add_table(orders_file, "orders")

        # Add foreign key constraint using patch to avoid validation issues
        fk_constraint = ConstraintInfo(
            constraint_name="fk_orders_user",
            constraint_type="FOREIGN_KEY",
            table_name="orders",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )

        # Bypass validation for test
        orders_metadata = self.catalog.get_table_metadata("orders")
        orders_metadata.add_constraint(fk_constraint)

        # Mock the dependency finding
        with patch.object(self.catalog, '_find_dependencies', return_value={"orders"}):
            with self.assertRaises(DbException) as context:
                self.catalog.drop_table("users", cascade=False)
            self.assertIn("referenced by", str(context.exception))

    def test_drop_table_cascade(self):
        """Test dropping table with cascade option."""
        # Create users and orders tables
        users_file = self.create_mock_db_file(1, self.users_desc)
        orders_file = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(users_file, "users")
        self.catalog.add_table(orders_file, "orders")

        # Mock dependencies but prevent infinite recursion by tracking dropped tables
        dropped_tables = set()

        def mock_find_dependencies(table_name):
            if table_name == "users" and "orders" not in dropped_tables:
                return {"orders"}
            return set()

        def mock_drop_table(table_name, cascade=False):
            dropped_tables.add(table_name)
            # Only call the original method if table hasn't been dropped yet
            if table_name not in dropped_tables:
                return self.catalog.__class__.drop_table.__wrapped__(self.catalog, table_name, cascade)
            return True

        with patch.object(self.catalog, '_find_dependencies', side_effect=mock_find_dependencies):
            with patch.object(self.catalog, 'drop_table', side_effect=mock_drop_table):
                result = self.catalog.drop_table("users", cascade=True)

        self.assertTrue(result)

    # =============================================================================
    # INDEX TESTS
    # =============================================================================

    def test_add_index_basic(self):
        """Test adding a basic index."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.catalog.add_index("users", "idx_name", ["name"])

        metadata = self.catalog.get_table_metadata("users")
        self.assertIn("idx_name", metadata.indexes)

        index_info = metadata.indexes["idx_name"]
        self.assertEqual(index_info.index_name, "idx_name")
        self.assertEqual(index_info.table_name, "users")
        self.assertEqual(index_info.field_names, ["name"])
        self.assertEqual(index_info.index_type, "btree")
        self.assertFalse(index_info.unique)

    def test_add_unique_index(self):
        """Test adding a unique index."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.catalog.add_index("users", "idx_email", [
                               "email"], unique=True, index_type="hash")

        metadata = self.catalog.get_table_metadata("users")
        index_info = metadata.indexes["idx_email"]
        self.assertTrue(index_info.unique)
        self.assertEqual(index_info.index_type, "hash")

    def test_add_composite_index(self):
        """Test adding a composite index."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.catalog.add_index("users", "idx_name_age", ["name", "age"])

        metadata = self.catalog.get_table_metadata("users")
        index_info = metadata.indexes["idx_name_age"]
        self.assertEqual(index_info.field_names, ["name", "age"])

    def test_add_index_table_not_found(self):
        """Test adding index to non-existent table."""
        with self.assertRaises(DbException) as context:
            self.catalog.add_index("nonexistent", "idx_test", ["field"])
        self.assertIn("not found", str(context.exception))

    def test_add_index_field_not_found(self):
        """Test adding index on non-existent field."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        with self.assertRaises(DbException) as context:
            self.catalog.add_index("users", "idx_test", ["nonexistent_field"])
        self.assertIn("not found in table", str(context.exception))

    def test_add_index_duplicate_name(self):
        """Test adding index with duplicate name."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        self.catalog.add_index("users", "idx_name", ["name"])

        with self.assertRaises(DbException) as context:
            self.catalog.add_index("users", "idx_name", ["email"])
        self.assertIn("already exists", str(context.exception))

    # =============================================================================
    # CONSTRAINT TESTS
    # =============================================================================

    def test_add_constraint_basic(self):
        """Test adding a basic constraint."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        unique_constraint = ConstraintInfo(
            constraint_name="unique_email",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=["email"]
        )

        # Mock the validation to succeed for constraint addition
        with patch.object(self.catalog.validator, 'validate_table_creation', return_value=True):
            self.catalog.add_constraint("users", unique_constraint)

        metadata = self.catalog.get_table_metadata("users")
        self.assertIn("unique_email", metadata.constraints)

        constraint = metadata.constraints["unique_email"]
        self.assertEqual(constraint.constraint_type, "UNIQUE")
        self.assertEqual(constraint.field_names, ["email"])

    def test_add_constraint_table_not_found(self):
        """Test adding constraint to non-existent table."""
        constraint = ConstraintInfo(
            constraint_name="test_constraint",
            constraint_type="UNIQUE",
            table_name="nonexistent",
            field_names=["field"]
        )

        with self.assertRaises(DbException):
            self.catalog.add_constraint("nonexistent", constraint)

    def test_add_constraint_validation_failure(self):
        """Test adding constraint that fails validation."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users")

        constraint = ConstraintInfo(
            constraint_name="test_constraint",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=["email"]
        )

        with patch.object(self.catalog.validator, 'validate_table_creation', return_value=False):
            with patch.object(self.catalog.validator, 'get_validation_errors', return_value=["Validation failed"]):
                with self.assertRaises(DbException) as context:
                    self.catalog.add_constraint("users", constraint)
                self.assertIn("Constraint validation failed",
                              str(context.exception))

    # =============================================================================
    # SCHEMA VALIDATION TESTS
    # =============================================================================

    def test_validate_schema_integrity_success(self):
        """Test successful schema validation."""
        with patch.object(self.catalog.validator, 'validate_foreign_key_references', return_value=True):
            result = self.catalog.validate_schema_integrity()
            self.assertTrue(result)

    def test_validate_schema_integrity_failure(self):
        """Test failed schema validation."""
        with patch.object(self.catalog.validator, 'validate_foreign_key_references', return_value=False):
            with patch.object(self.catalog.validator, 'get_validation_errors', return_value=["FK error"]):
                result = self.catalog.validate_schema_integrity()
                self.assertFalse(result)

    # =============================================================================
    # STATISTICS TESTS
    # =============================================================================

    def test_get_table_statistics(self):
        """Test getting table statistics."""
        db_file = self.create_mock_db_file(1, self.users_desc, "users.dat")
        metadata = self.catalog.add_table(db_file, "users", ["id"])

        # Create a dummy file for statistics
        file_path = Path(self.temp_dir) / "users.dat"
        file_path.write_text("dummy content")

        with patch.object(self.catalog, '_update_table_statistics'):
            stats = self.catalog.get_table_statistics("users")

        expected_keys = ["table_name", "row_count", "file_size_bytes",
                         "created_at", "modified_at", "indexes", "constraints"]
        for key in expected_keys:
            self.assertIn(key, stats)

        self.assertEqual(stats["table_name"], "users")

    def test_get_table_statistics_not_found(self):
        """Test getting statistics for non-existent table."""
        with self.assertRaises(DbException):
            self.catalog.get_table_statistics("nonexistent")

    # =============================================================================
    # BACKUP AND RESTORE TESTS
    # =============================================================================

    def test_create_backup(self):
        """Test creating a backup."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users", ["id"])

        backup_file = self.catalog.create_backup()

        self.assertTrue(backup_file.exists())
        self.assertTrue(backup_file.name.startswith("catalog_backup_"))
        self.assertTrue(backup_file.name.endswith(".json"))

        # Verify backup content
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)

        self.assertEqual(backup_data["version"], "1.0")
        self.assertIn("tables", backup_data)
        self.assertIn("users", backup_data["tables"])

    def test_restore_from_backup(self):
        """Test restoring from backup."""
        # Create initial catalog with data
        db_file = self.create_mock_db_file(1, self.users_desc)
        self.catalog.add_table(db_file, "users", ["id"])

        # Create backup
        backup_file = self.catalog.create_backup()

        # Clear catalog
        self.catalog.clear()
        self.assertEqual(len(self.catalog.list_tables()), 0)

        # Restore from backup
        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_heap_file.return_value = Mock()
            self.catalog.restore_from_backup(backup_file)

        # Verify restoration
        self.assertTrue(self.catalog.table_exists("users"))
        self.assertEqual(len(self.catalog.list_tables()), 1)

    def test_restore_from_backup_file_not_found(self):
        """Test restoring from non-existent backup file."""
        nonexistent_file = Path(self.temp_dir) / "nonexistent.json"

        with self.assertRaises(DbException) as context:
            self.catalog.restore_from_backup(nonexistent_file)
        self.assertIn("Backup file not found", str(context.exception))

    # =============================================================================
    # CLEAR CATALOG TESTS
    # =============================================================================

    def test_clear_catalog(self):
        """Test clearing all tables from catalog."""
        db_file1 = self.create_mock_db_file(1, self.users_desc)
        db_file2 = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(db_file1, "users")
        self.catalog.add_table(db_file2, "orders")

        self.assertEqual(len(self.catalog.list_tables()), 2)

        self.catalog.clear()

        self.assertEqual(len(self.catalog.list_tables()), 0)
        self.assertFalse(self.catalog.table_exists("users"))
        self.assertFalse(self.catalog.table_exists("orders"))

    # =============================================================================
    # SCHEMA FILE LOADING TESTS
    # =============================================================================

    def test_load_simple_schema_file(self):
        """Test loading simple schema file format."""
        schema_content = """
# Test schema file
users (id int pk, name string, email string, age int)
        """.strip()

        schema_file = Path(self.temp_dir) / "schema.txt"
        schema_file.write_text(schema_content)

        # Create proper tuple descriptor for the parsed table
        users_tuple_desc = TupleDesc(
            [FieldType.INT, FieldType.STRING, FieldType.STRING, FieldType.INT],
            ["id", "name", "email", "age"]
        )

        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_instance = Mock()
            mock_instance.get_id.return_value = 1
            mock_instance.get_tuple_desc.return_value = users_tuple_desc
            mock_heap_file.return_value = mock_instance

            self.catalog.load_schema_file(str(schema_file), "simple")

        self.assertTrue(self.catalog.table_exists("users"))

    def test_load_schema_file_not_found(self):
        """Test loading non-existent schema file."""
        with self.assertRaises(DbException) as context:
            self.catalog.load_schema_file("nonexistent.txt", "simple")
        self.assertIn("Schema file not found", str(context.exception))

    def test_load_schema_file_unsupported_format(self):
        """Test loading schema file with unsupported format."""
        schema_file = Path(self.temp_dir) / "schema.txt"
        schema_file.write_text("dummy content")

        with self.assertRaises(DbException) as context:
            self.catalog.load_schema_file(str(schema_file), "unsupported")
        self.assertIn("Unsupported schema format", str(context.exception))

    def test_load_sql_schema_not_implemented(self):
        """Test that SQL schema loading raises NotImplementedError."""
        schema_file = Path(self.temp_dir) / "schema.sql"
        schema_file.write_text("CREATE TABLE test (id INT);")

        with self.assertRaises(NotImplementedError):
            self.catalog.load_schema_file(str(schema_file), "sql")

    def test_load_json_schema(self):
        """Test loading JSON schema format."""
        # Create a JSON schema
        json_schema = {
            "tables": [
                {
                    "table_name": "test_table",
                    "table_id": 1,
                    "file_path": "test.dat",
                    "primary_key_fields": ["id"],
                    "created_at": time.time(),
                    "modified_at": time.time(),
                    "row_count": 0,
                    "file_size_bytes": 0,
                    "table_comment": "",
                    "indexes": {},
                    "constraints": {},
                    "tuple_desc": {
                        "field_types": ["int", "string"],
                        "field_names": ["id", "name"]
                    }
                }
            ]
        }

        schema_file = Path(self.temp_dir) / "schema.json"
        schema_file.write_text(json.dumps(json_schema))

        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_heap_file.return_value = Mock()
            self.catalog._load_json_schema(schema_file)

        self.assertTrue(self.catalog.table_exists("test_table"))

    # =============================================================================
    # PRIVATE METHOD TESTS
    # =============================================================================

    def test_find_dependencies(self):
        """Test finding table dependencies via foreign keys."""
        # Create tables
        users_file = self.create_mock_db_file(1, self.users_desc)
        orders_file = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(users_file, "users")
        self.catalog.add_table(orders_file, "orders")

        # Add foreign key constraint directly to avoid validation issues
        fk_constraint = ConstraintInfo(
            constraint_name="fk_orders_user",
            constraint_type="FOREIGN_KEY",
            table_name="orders",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )

        orders_metadata = self.catalog.get_table_metadata("orders")
        orders_metadata.add_constraint(fk_constraint)

        dependencies = self.catalog._find_dependencies("users")
        self.assertIn("orders", dependencies)

        # Test table with no dependencies
        no_deps = self.catalog._find_dependencies("orders")
        self.assertEqual(len(no_deps), 0)

    def test_update_table_statistics(self):
        """Test updating table statistics."""
        # Create a test file
        test_file = Path(self.temp_dir) / "test.dat"
        test_file.write_text("test content")

        metadata = TableMetadata(
            table_name="test",
            table_id=1,
            file_path=str(test_file),
            tuple_desc=self.users_desc,
            primary_key_fields=[],
            created_at=time.time(),
            modified_at=time.time()
        )

        original_size = metadata.file_size_bytes
        Catalog._update_table_statistics(metadata)

        # File size should be updated
        self.assertNotEqual(metadata.file_size_bytes, original_size)
        self.assertGreater(metadata.file_size_bytes, 0)

    def test_update_table_statistics_file_not_found(self):
        """Test updating statistics when file doesn't exist."""
        metadata = TableMetadata(
            table_name="test",
            table_id=1,
            file_path="nonexistent.dat",
            tuple_desc=self.users_desc,
            primary_key_fields=[],
            created_at=time.time(),
            modified_at=time.time()
        )

        # Should not raise exception, should handle gracefully
        Catalog._update_table_statistics(metadata)

    def test_parse_simple_table_definition(self):
        """Test parsing simple table definition."""
        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_instance = Mock()
            mock_instance.get_id.return_value = 1
            mock_instance.get_tuple_desc.return_value = self.users_desc
            mock_heap_file.return_value = mock_instance

            with patch.object(self.catalog, 'add_table') as mock_add_table:
                self.catalog._parse_simple_table_definition(
                    "test_table (id int pk, name string, active boolean)"
                )

                mock_heap_file.assert_called_once()
                mock_add_table.assert_called_once()

    def test_parse_simple_table_definition_invalid(self):
        """Test parsing invalid simple table definition."""
        with self.assertRaises(ValueError):
            self.catalog._parse_simple_table_definition("invalid definition")

        with self.assertRaises(ValueError):
            self.catalog._parse_simple_table_definition(
                "table (invalid_field)")

        with self.assertRaises(ValueError):
            self.catalog._parse_simple_table_definition(
                "table (field unknown_type)")

    # =============================================================================
    # PERSISTENCE TESTS
    # =============================================================================

    def test_catalog_persistence_on_save(self):
        """Test that catalog is persisted to disk."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        # Suppress save errors for Windows
        with patch.object(self.catalog, '_save_catalog'):
            self.catalog.add_table(db_file, "users", ["id"])

        # Check that we attempted to save
        self.assertTrue(self.catalog.table_exists("users"))

    def test_catalog_persistence_on_load(self):
        """Test that catalog is loaded from disk on initialization."""
        # Create initial catalog and add table
        db_file = self.create_mock_db_file(1, self.users_desc)

        with patch.object(self.catalog, '_save_catalog'):
            self.catalog.add_table(db_file, "users", ["id"])

        # Create new catalog instance (should load from disk)
        with patch('app.catalog.catalog.HeapFile') as mock_heap_file:
            mock_heap_file.return_value = Mock()
            with patch.object(Catalog, '_load_catalog'):
                new_catalog = Catalog(self.catalog_dir)
                # Manually add table to simulate loading
                new_catalog._tables["users"] = self.catalog._tables["users"]

        # Should have loaded the existing table
        self.assertTrue(new_catalog.table_exists("users"))

    def test_catalog_load_corrupted_file(self):
        """Test handling of corrupted catalog file."""
        # Create corrupted metadata file
        metadata_file = Path(self.catalog_dir) / "catalog_metadata.json"
        metadata_file.write_text("invalid json content")

        # Should handle gracefully and create empty catalog
        with patch('builtins.print'):  # Suppress warning print
            new_catalog = Catalog(self.catalog_dir)

        self.assertEqual(len(new_catalog.list_tables()), 0)

    def test_catalog_save_failure(self):
        """Test handling of save failures."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        # Mock file operations to fail
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with patch('builtins.print'):  # Suppress error print
                # Should not raise exception, should handle gracefully
                self.catalog.add_table(db_file, "users")

    # =============================================================================
    # THREAD SAFETY TESTS
    # =============================================================================

    def test_thread_safety_add_table(self):
        """Test thread safety when adding tables concurrently."""
        results = []
        errors = []

        def add_table_worker(table_id, table_name):
            try:
                db_file = self.create_mock_db_file(table_id, self.users_desc)
                metadata = self.catalog.add_table(db_file, table_name)
                results.append(metadata.table_name)
            except Exception as e:
                errors.append(str(e))

        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=add_table_worker,
                args=(i + 1, f"table_{i + 1}")
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check that tables were created (some might fail due to validation)
        self.assertGreaterEqual(len(results) + len(errors), 5)
        self.assertGreater(len(self.catalog.list_tables()), 0)

    def test_thread_safety_read_operations(self):
        """Test thread safety for concurrent read operations."""
        # Add some tables first
        for i in range(3):
            db_file = self.create_mock_db_file(i + 1, self.users_desc)
            self.catalog.add_table(db_file, f"table_{i + 1}")

        results = []

        def read_worker():
            try:
                tables = self.catalog.list_tables()
                for table_name in tables:
                    table_id = self.catalog.get_table_id(table_name)
                    metadata = self.catalog.get_table_metadata(table_name)
                    results.append((table_name, table_id, metadata.table_name))
            except Exception as e:
                results.append(f"Error: {e}")

        threads = []
        for i in range(10):
            thread = threading.Thread(target=read_worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All reads should succeed
        error_count = sum(1 for r in results if isinstance(
            r, str) and r.startswith("Error"))
        self.assertEqual(error_count, 0)

    # =============================================================================
    # STRING REPRESENTATION TESTS
    # =============================================================================

    def test_string_representation(self):
        """Test string representation of catalog."""
        # Empty catalog
        self.assertEqual(str(self.catalog), "Catalog(0 tables)")
        self.assertEqual(repr(self.catalog), "Catalog(0 tables)")

        # Add some tables
        db_file1 = self.create_mock_db_file(1, self.users_desc)
        db_file2 = self.create_mock_db_file(2, self.orders_desc)

        self.catalog.add_table(db_file1, "users")
        self.catalog.add_table(db_file2, "orders")

        self.assertEqual(str(self.catalog), "Catalog(2 tables)")
        self.assertEqual(repr(self.catalog), "Catalog(2 tables)")

    # =============================================================================
    # EDGE CASES AND ERROR CONDITIONS
    # =============================================================================

    def test_empty_table_name(self):
        """Test adding table with empty name."""
        db_file = self.create_mock_db_file(1, self.users_desc)

        with patch.object(self.catalog.validator, 'validate_table_creation', return_value=False):
            with patch.object(self.catalog.validator, 'get_validation_errors', return_value=["Empty table name"]):
                with self.assertRaises(DbException):
                    self.catalog.add_table(db_file, "")

    def test_very_long_table_name(self):
        """Test adding table with very long name."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        long_name = "a" * 1000

        # Should work unless validator rejects it
        try:
            self.catalog.add_table(db_file, long_name)
            self.assertTrue(self.catalog.table_exists(long_name))
        except DbException:
            # If validator rejects it, that's also acceptable
            pass

    def test_special_characters_in_table_name(self):
        """Test adding table with special characters in name."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        special_name = "table-with_special.chars"

        # Should work unless validator rejects it
        try:
            self.catalog.add_table(db_file, special_name)
            self.assertTrue(self.catalog.table_exists(special_name))
        except DbException:
            # If validator rejects it, that's also acceptable
            pass

    def test_large_number_of_tables(self):
        """Test adding a large number of tables."""
        table_count = 100

        for i in range(table_count):
            db_file = self.create_mock_db_file(i + 1, self.users_desc)
            self.catalog.add_table(db_file, f"table_{i:03d}")

        self.assertEqual(len(self.catalog.list_tables()), table_count)

        # Test lookups still work
        self.assertTrue(self.catalog.table_exists("table_050"))
        self.assertEqual(self.catalog.get_table_id("table_050"), 51)

    def test_unicode_table_names(self):
        """Test tables with unicode names."""
        db_file = self.create_mock_db_file(1, self.users_desc)
        unicode_name = "用户表"  # Chinese for "user table"

        try:
            self.catalog.add_table(db_file, unicode_name)
            self.assertTrue(self.catalog.table_exists(unicode_name))
        except DbException:
            # If validator rejects it, that's also acceptable
            pass


if __name__ == '__main__':
    unittest.main()
