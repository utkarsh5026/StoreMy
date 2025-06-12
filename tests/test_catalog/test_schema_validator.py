"""
Tests for schema_validator.py module.
"""
from app.catalog.schema_validator import SchemaValidator
from app.catalog.table_info import TableMetadata, ConstraintInfo
from app.core.tuple import TupleDesc
from app.core.types import FieldType


class TestSchemaValidator:
    """Test cases for SchemaValidator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = SchemaValidator()

        # Create sample table metadata for testing
        self.users_tuple_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING, FieldType.STRING],
            field_names=["id", "name", "email"]
        )

        self.posts_tuple_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING, FieldType.INT],
            field_names=["id", "title", "user_id"]
        )

        self.users_metadata = TableMetadata(
            table_name="users",
            table_id=1,
            file_path="/data/users.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        self.posts_metadata = TableMetadata(
            table_name="posts",
            table_id=2,
            file_path="/data/posts.dat",
            tuple_desc=self.posts_tuple_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

    def test_validator_initialization(self):
        """Test SchemaValidator initialization."""
        validator = SchemaValidator()
        assert isinstance(validator.validation_errors, list)
        assert len(validator.validation_errors) == 0

    def test_validate_table_creation_success(self):
        """Test successful table creation validation."""
        existing_tables = {}

        result = self.validator.validate_table_creation(
            self.users_metadata, existing_tables)

        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_validate_table_creation_duplicate_name(self):
        """Test validation fails for duplicate table name."""
        existing_tables = {"users": self.users_metadata}

        # Try to create another table with the same name
        duplicate_metadata = TableMetadata(
            table_name="users",
            table_id=99,
            file_path="/data/users2.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=2000.0,
            modified_at=2000.0
        )

        result = self.validator.validate_table_creation(
            duplicate_metadata, existing_tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Table 'users' already exists" in errors[0]

    def test_validate_table_creation_duplicate_id(self):
        """Test validation fails for duplicate table ID."""
        existing_tables = {"users": self.users_metadata}

        # Try to create table with the same ID
        duplicate_id_metadata = TableMetadata(
            table_name="products",
            table_id=1,  # Same ID as users table
            file_path="/data/products.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=2000.0,
            modified_at=2000.0
        )

        result = self.validator.validate_table_creation(
            duplicate_id_metadata, existing_tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Table ID 1 already in use" in errors[0]

    def test_validate_table_creation_duplicate_field_names(self):
        """Test validation fails for duplicate field names within table."""
        duplicate_fields_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING, FieldType.STRING],
            field_names=["id", "name", "name"]  # Duplicate field name
        )

        metadata = TableMetadata(
            table_name="bad_table",
            table_id=99,
            file_path="/data/bad.dat",
            tuple_desc=duplicate_fields_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        result = self.validator.validate_table_creation(metadata, {})

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Duplicate field names in table schema" in errors[0]

    def test_validate_table_creation_invalid_primary_key_fields(self):
        """Test validation fails when primary key fields don't exist in schema."""
        metadata = TableMetadata(
            table_name="test_table",
            table_id=99,
            file_path="/data/test.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id", "non_existent_field"],  # Invalid field
            created_at=1000.0,
            modified_at=1000.0
        )

        result = self.validator.validate_table_creation(metadata, {})

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Primary key fields not in schema" in errors[0]
        assert "non_existent_field" in errors[0]

    def test_validate_table_creation_with_valid_constraints(self):
        """Test validation succeeds with valid constraints."""
        # Add valid primary key constraint
        pk_constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )
        self.users_metadata.add_constraint(pk_constraint)

        result = self.validator.validate_table_creation(
            self.users_metadata, {})

        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_validate_table_creation_with_invalid_constraint_fields(self):
        """Test validation fails when constraint references non-existent fields."""
        # Add constraint with invalid field
        invalid_constraint = ConstraintInfo(
            constraint_name="uq_invalid",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=["non_existent_field"]
        )
        self.users_metadata.add_constraint(invalid_constraint)

        result = self.validator.validate_table_creation(
            self.users_metadata, {})

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Constraint 'uq_invalid' references non-existent field 'non_existent_field'" in errors[0]

    def test_validate_table_creation_with_invalid_foreign_key_no_reference_table(self):
        """Test validation fails for foreign key without reference table."""
        invalid_fk = ConstraintInfo(
            constraint_name="fk_invalid",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table=None  # Missing reference table
        )
        self.posts_metadata.add_constraint(invalid_fk)

        result = self.validator.validate_table_creation(
            self.posts_metadata, {})

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Foreign key constraint 'fk_invalid' missing reference table" in errors[0]

    def test_validate_table_creation_with_foreign_key_invalid_reference_field(self):
        """Test validation fails for foreign key with invalid reference field."""
        existing_tables = {"users": self.users_metadata}

        invalid_fk = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["non_existent_field"]  # Invalid reference field
        )
        self.posts_metadata.add_constraint(invalid_fk)

        result = self.validator.validate_table_creation(
            self.posts_metadata, existing_tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "Foreign key 'fk_posts_user' references non-existent field 'non_existent_field'" in errors[0]

    def test_validate_table_creation_multiple_errors(self):
        """Test validation collects multiple errors."""
        existing_tables = {"users": self.users_metadata}

        # Create table with multiple issues
        bad_metadata = TableMetadata(
            table_name="users",  # Duplicate name
            table_id=1,  # Duplicate ID
            file_path="/data/bad.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["non_existent_field"],  # Invalid PK field
            created_at=1000.0,
            modified_at=1000.0
        )

        result = self.validator.validate_table_creation(
            bad_metadata, existing_tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 3
        assert any("already exists" in error for error in errors)
        assert any("already in use" in error for error in errors)
        assert any(
            "Primary key fields not in schema" in error for error in errors)

    def test_validate_foreign_key_references_success(self):
        """Test successful foreign key reference validation."""
        # Add valid foreign key constraint
        fk_constraint = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )
        self.posts_metadata.add_constraint(fk_constraint)

        tables = {
            "users": self.users_metadata,
            "posts": self.posts_metadata
        }

        result = self.validator.validate_foreign_key_references(tables)

        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_validate_foreign_key_references_missing_table(self):
        """Test validation fails when referenced table doesn't exist."""
        fk_constraint = ConstraintInfo(
            constraint_name="fk_posts_category",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["category_id"],
            reference_table="categories",  # Non-existent table
            reference_fields=["id"]
        )
        self.posts_metadata.add_constraint(fk_constraint)

        tables = {"posts": self.posts_metadata}

        result = self.validator.validate_foreign_key_references(tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "references non-existent table 'categories'" in errors[0]

    def test_validate_foreign_key_references_missing_field(self):
        """Test validation fails when referenced field doesn't exist."""
        fk_constraint = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["non_existent_id"]  # Non-existent field
        )
        self.posts_metadata.add_constraint(fk_constraint)

        tables = {
            "users": self.users_metadata,
            "posts": self.posts_metadata
        }

        result = self.validator.validate_foreign_key_references(tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 1
        assert "references non-existent field 'non_existent_id'" in errors[0]

    def test_validate_foreign_key_references_multiple_errors(self):
        """Test validation collects multiple foreign key errors."""
        # Add multiple invalid foreign keys
        fk1 = ConstraintInfo(
            constraint_name="fk_posts_user",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="non_existent_table",
            reference_fields=["id"]
        )

        fk2 = ConstraintInfo(
            constraint_name="fk_posts_category",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["category_id"],
            reference_table="users",
            reference_fields=["non_existent_field"]
        )

        self.posts_metadata.add_constraint(fk1)
        self.posts_metadata.add_constraint(fk2)

        tables = {
            "users": self.users_metadata,
            "posts": self.posts_metadata
        }

        result = self.validator.validate_foreign_key_references(tables)

        assert result is False
        errors = self.validator.get_validation_errors()
        assert len(errors) == 2
        assert any("non_existent_table" in error for error in errors)
        assert any("non_existent_field" in error for error in errors)

    def test_validate_foreign_key_references_ignores_non_fk_constraints(self):
        """Test that non-foreign key constraints are ignored during FK validation."""
        # Add non-FK constraints
        pk_constraint = ConstraintInfo(
            constraint_name="pk_users",
            constraint_type="PRIMARY_KEY",
            table_name="users",
            field_names=["id"]
        )

        unique_constraint = ConstraintInfo(
            constraint_name="uq_email",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=["email"]
        )

        self.users_metadata.add_constraint(pk_constraint)
        self.users_metadata.add_constraint(unique_constraint)

        tables = {"users": self.users_metadata}

        result = self.validator.validate_foreign_key_references(tables)

        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_get_validation_errors_returns_copy(self):
        """Test that get_validation_errors returns a copy of the errors list."""
        # Trigger some validation errors
        existing_tables = {"users": self.users_metadata}
        duplicate_metadata = TableMetadata(
            table_name="users",
            table_id=99,
            file_path="/data/users2.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=2000.0,
            modified_at=2000.0
        )

        self.validator.validate_table_creation(
            duplicate_metadata, existing_tables)

        errors1 = self.validator.get_validation_errors()
        errors2 = self.validator.get_validation_errors()

        # Should be equal but not the same object
        assert errors1 == errors2
        assert errors1 is not errors2

        # Modifying returned list shouldn't affect internal state
        errors1.append("test error")
        errors3 = self.validator.get_validation_errors()
        assert len(errors3) == 1  # Should still be original length

    def test_validation_errors_cleared_between_validations(self):
        """Test that validation errors are cleared between validation calls."""
        # First validation with errors
        existing_tables = {"users": self.users_metadata}
        duplicate_metadata = TableMetadata(
            table_name="users",
            table_id=99,
            file_path="/data/users2.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=2000.0,
            modified_at=2000.0
        )

        result1 = self.validator.validate_table_creation(
            duplicate_metadata, existing_tables)
        assert result1 is False
        assert len(self.validator.get_validation_errors()) > 0

        # Second validation should clear previous errors
        valid_metadata = TableMetadata(
            table_name="products",
            table_id=99,
            file_path="/data/products.dat",
            tuple_desc=self.users_tuple_desc,
            primary_key_fields=["id"],
            created_at=2000.0,
            modified_at=2000.0
        )

        result2 = self.validator.validate_table_creation(
            valid_metadata, existing_tables)
        assert result2 is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_complex_schema_validation_scenario(self):
        """Test a complex scenario with multiple tables and relationships."""
        # Create a more complex schema
        categories_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING],
            field_names=["id", "name"]
        )

        comments_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING,
                         FieldType.INT, FieldType.INT],
            field_names=["id", "content", "post_id", "user_id"]
        )

        categories_metadata = TableMetadata(
            table_name="categories",
            table_id=3,
            file_path="/data/categories.dat",
            tuple_desc=categories_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        comments_metadata = TableMetadata(
            table_name="comments",
            table_id=4,
            file_path="/data/comments.dat",
            tuple_desc=comments_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        # Add foreign key constraints
        posts_category_fk = ConstraintInfo(
            constraint_name="fk_posts_category",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["category_id"],
            reference_table="categories",
            reference_fields=["id"]
        )

        comments_post_fk = ConstraintInfo(
            constraint_name="fk_comments_post",
            constraint_type="FOREIGN_KEY",
            table_name="comments",
            field_names=["post_id"],
            reference_table="posts",
            reference_fields=["id"]
        )

        comments_user_fk = ConstraintInfo(
            constraint_name="fk_comments_user",
            constraint_type="FOREIGN_KEY",
            table_name="comments",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=["id"]
        )

        # Need to update posts schema to include category_id
        posts_with_category_desc = TupleDesc(
            field_types=[FieldType.INT, FieldType.STRING,
                         FieldType.INT, FieldType.INT],
            field_names=["id", "title", "user_id", "category_id"]
        )

        posts_updated_metadata = TableMetadata(
            table_name="posts",
            table_id=2,
            file_path="/data/posts.dat",
            tuple_desc=posts_with_category_desc,
            primary_key_fields=["id"],
            created_at=1000.0,
            modified_at=1000.0
        )

        posts_updated_metadata.add_constraint(posts_category_fk)
        comments_metadata.add_constraint(comments_post_fk)
        comments_metadata.add_constraint(comments_user_fk)

        tables = {
            "users": self.users_metadata,
            "posts": posts_updated_metadata,
            "categories": categories_metadata,
            "comments": comments_metadata
        }

        result = self.validator.validate_foreign_key_references(tables)

        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_edge_case_empty_field_names_in_constraint(self):
        """Test edge case with empty field names in constraint."""
        empty_constraint = ConstraintInfo(
            constraint_name="empty_constraint",
            constraint_type="UNIQUE",
            table_name="users",
            field_names=[]  # Empty field names
        )
        self.users_metadata.add_constraint(empty_constraint)

        result = self.validator.validate_table_creation(
            self.users_metadata, {})

        # Should succeed because no fields to validate
        assert result is True
        assert len(self.validator.get_validation_errors()) == 0

    def test_edge_case_none_reference_fields(self):
        """Test edge case with None reference fields in foreign key."""
        fk_with_none_refs = ConstraintInfo(
            constraint_name="fk_with_none",
            constraint_type="FOREIGN_KEY",
            table_name="posts",
            field_names=["user_id"],
            reference_table="users",
            reference_fields=None  # None reference fields
        )
        self.posts_metadata.add_constraint(fk_with_none_refs)

        tables = {
            "users": self.users_metadata,
            "posts": self.posts_metadata
        }

        result = self.validator.validate_foreign_key_references(tables)

        # Should succeed because no reference fields to validate
        assert result is True
        assert len(self.validator.get_validation_errors()) == 0
