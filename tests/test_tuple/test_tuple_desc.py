import pytest
from app.core.tuple.tuple_desc import TupleDesc
from app.core.types.type_enum import FieldType


class TestTupleDesc:
    """Comprehensive tests for TupleDesc implementation."""

    def test_init_valid_field_types_only(self):
        """Test initialization with only field types."""
        field_types = [FieldType.INT, FieldType.STRING, FieldType.BOOLEAN]
        td = TupleDesc(field_types)

        assert td.num_fields() == 3
        assert td.get_field_type(0) == FieldType.INT
        assert td.get_field_type(1) == FieldType.STRING
        assert td.get_field_type(2) == FieldType.BOOLEAN
        assert td.field_names is None

    def test_init_with_field_names(self):
        """Test initialization with field types and names."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id", "name"]
        td = TupleDesc(field_types, field_names)

        assert td.num_fields() == 2
        assert td.get_field_name(0) == "id"
        assert td.get_field_name(1) == "name"

    def test_init_single_field(self):
        """Test initialization with single field."""
        field_types = [FieldType.INT]
        td = TupleDesc(field_types)

        assert td.num_fields() == 1
        assert td.get_field_type(0) == FieldType.INT

    def test_init_all_field_types(self):
        """Test initialization with all available field types."""
        field_types = [
            FieldType.INT,
            FieldType.STRING,
            FieldType.BOOLEAN,
            FieldType.FLOAT,
            FieldType.DOUBLE
        ]
        td = TupleDesc(field_types)

        assert td.num_fields() == 5
        for i, expected_type in enumerate(field_types):
            assert td.get_field_type(i) == expected_type

    def test_init_empty_field_types_raises_error(self):
        """Test that empty field types list raises ValueError."""
        with pytest.raises(ValueError, match="TupleDesc must have at least one field"):
            TupleDesc([])

    def test_init_mismatched_names_length_raises_error(self):
        """Test that mismatched field names length raises ValueError."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id"]  # Wrong length

        with pytest.raises(ValueError, match="Number of field names \\(1\\) must match number of field types \\(2\\)"):
            TupleDesc(field_types, field_names)

    def test_init_more_names_than_types_raises_error(self):
        """Test with more names than types."""
        field_types = [FieldType.INT]
        field_names = ["id", "name", "extra"]

        with pytest.raises(ValueError, match="Number of field names \\(3\\) must match number of field types \\(1\\)"):
            TupleDesc(field_types, field_names)

    def test_field_types_are_copied(self):
        """Test that field types list is copied, not referenced."""
        original_types = [FieldType.INT, FieldType.STRING]
        td = TupleDesc(original_types)

        # Modify original list
        original_types.append(FieldType.BOOLEAN)

        # TupleDesc should be unaffected
        assert td.num_fields() == 2
        assert td.get_field_type(0) == FieldType.INT
        assert td.get_field_type(1) == FieldType.STRING

    def test_field_names_are_copied(self):
        """Test that field names list is copied, not referenced."""
        field_types = [FieldType.INT, FieldType.STRING]
        original_names = ["id", "name"]
        td = TupleDesc(field_types, original_names)

        # Modify original list
        original_names.append("extra")
        original_names[0] = "modified"

        # TupleDesc should be unaffected
        assert td.get_field_name(0) == "id"
        assert td.get_field_name(1) == "name"

    def test_get_field_type_valid_indices(self):
        """Test get_field_type with valid indices."""
        field_types = [FieldType.INT, FieldType.STRING, FieldType.BOOLEAN]
        td = TupleDesc(field_types)

        assert td.get_field_type(0) == FieldType.INT
        assert td.get_field_type(1) == FieldType.STRING
        assert td.get_field_type(2) == FieldType.BOOLEAN

    def test_get_field_type_invalid_indices(self):
        """Test get_field_type with invalid indices."""
        field_types = [FieldType.INT, FieldType.STRING]
        td = TupleDesc(field_types)

        with pytest.raises(IndexError, match="Field index -1 out of range"):
            td.get_field_type(-1)

        with pytest.raises(IndexError, match="Field index 2 out of range"):
            td.get_field_type(2)

        with pytest.raises(IndexError, match="Field index 100 out of range"):
            td.get_field_type(100)

    def test_get_field_name_with_names(self):
        """Test get_field_name when names are defined."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id", "name"]
        td = TupleDesc(field_types, field_names)

        assert td.get_field_name(0) == "id"
        assert td.get_field_name(1) == "name"

    def test_get_field_name_without_names(self):
        """Test get_field_name when names are not defined."""
        field_types = [FieldType.INT, FieldType.STRING]
        td = TupleDesc(field_types)

        assert td.get_field_name(0) is None
        assert td.get_field_name(1) is None

    def test_get_field_name_invalid_indices(self):
        """Test get_field_name with invalid indices."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id", "name"]
        td = TupleDesc(field_types, field_names)

        with pytest.raises(IndexError, match="Field index -1 out of range"):
            td.get_field_name(-1)

        with pytest.raises(IndexError, match="Field index 2 out of range"):
            td.get_field_name(2)

    def test_name_to_index_simple_names(self):
        """Test name_to_index with simple field names."""
        field_types = [FieldType.INT, FieldType.STRING, FieldType.BOOLEAN]
        field_names = ["id", "name", "active"]
        td = TupleDesc(field_types, field_names)

        assert td.name_to_index("id") == 0
        assert td.name_to_index("name") == 1
        assert td.name_to_index("active") == 2

    def test_name_to_index_qualified_names(self):
        """Test name_to_index with qualified field names."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id", "name"]
        td = TupleDesc(field_types, field_names)

        assert td.name_to_index("table.id") == 0
        assert td.name_to_index("users.name") == 1
        assert td.name_to_index("some_table.id") == 0

    def test_name_to_index_no_names_defined(self):
        """Test name_to_index when no names are defined."""
        field_types = [FieldType.INT, FieldType.STRING]
        td = TupleDesc(field_types)

        with pytest.raises(ValueError, match="Cannot lookup field by name - no field names defined"):
            td.name_to_index("id")

    def test_name_to_index_field_not_found(self):
        """Test name_to_index with non-existent field name."""
        field_types = [FieldType.INT, FieldType.STRING]
        field_names = ["id", "name"]
        td = TupleDesc(field_types, field_names)

        with pytest.raises(ValueError, match="Field 'nonexistent' not found in tuple descriptor"):
            td.name_to_index("nonexistent")

        with pytest.raises(ValueError, match="Field 'missing' not found in tuple descriptor"):
            td.name_to_index("table.missing")

    def test_get_size(self):
        """Test get_size calculation."""
        field_types = [FieldType.INT, FieldType.STRING, FieldType.BOOLEAN]
        td = TupleDesc(field_types)

        expected_size = (
            FieldType.INT.get_length() +
            FieldType.STRING.get_length() +
            FieldType.BOOLEAN.get_length()
        )
        assert td.get_size() == expected_size

    def test_get_size_single_field(self):
        """Test get_size with single field."""
        td = TupleDesc([FieldType.STRING])
        assert td.get_size() == FieldType.STRING.get_length()

    def test_get_size_all_field_types(self):
        """Test get_size with all field types."""
        field_types = [
            FieldType.INT,
            FieldType.STRING,
            FieldType.BOOLEAN,
            FieldType.FLOAT,
            FieldType.DOUBLE
        ]
        td = TupleDesc(field_types)

        expected_size = sum(ft.get_length() for ft in field_types)
        assert td.get_size() == expected_size

    def test_equals_same_types(self):
        """Test equals with same field types."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        td2 = TupleDesc([FieldType.INT, FieldType.STRING])

        assert td1.equals(td2)
        assert td2.equals(td1)

    def test_equals_different_types(self):
        """Test equals with different field types."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        td2 = TupleDesc([FieldType.STRING, FieldType.INT])  # Different order

        assert not td1.equals(td2)
        assert not td2.equals(td1)

    def test_equals_different_lengths(self):
        """Test equals with different lengths."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        td2 = TupleDesc([FieldType.INT])

        assert not td1.equals(td2)
        assert not td2.equals(td1)

    def test_equals_ignores_field_names(self):
        """Test that equals ignores field names."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING], ["id", "name"])
        td2 = TupleDesc([FieldType.INT, FieldType.STRING],
                        ["user_id", "full_name"])
        td3 = TupleDesc([FieldType.INT, FieldType.STRING])  # No names

        assert td1.equals(td2)
        assert td1.equals(td3)
        assert td2.equals(td3)

    def test_equals_non_tupledesc(self):
        """Test equals with non-TupleDesc objects."""
        td = TupleDesc([FieldType.INT])

        assert not td.equals("not a tupledesc")
        assert not td.equals(None)
        assert not td.equals(42)

    def test_combine_basic(self):
        """Test basic tuple descriptor combination."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        td2 = TupleDesc([FieldType.BOOLEAN, FieldType.FLOAT])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 4
        assert combined.get_field_type(0) == FieldType.INT
        assert combined.get_field_type(1) == FieldType.STRING
        assert combined.get_field_type(2) == FieldType.BOOLEAN
        assert combined.get_field_type(3) == FieldType.FLOAT

    def test_combine_with_names_both(self):
        """Test combination when both have field names."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING], ["id", "name"])
        td2 = TupleDesc([FieldType.BOOLEAN], ["active"])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 3
        assert combined.get_field_name(0) == "id"
        assert combined.get_field_name(1) == "name"
        assert combined.get_field_name(2) == "active"

    def test_combine_with_names_first_only(self):
        """Test combination when only first has field names."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING], ["id", "name"])
        td2 = TupleDesc([FieldType.BOOLEAN])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 3
        assert combined.get_field_name(0) == "id"
        assert combined.get_field_name(1) == "name"
        assert combined.get_field_name(2) is None

    def test_combine_with_names_second_only(self):
        """Test combination when only second has field names."""
        td1 = TupleDesc([FieldType.INT])
        td2 = TupleDesc([FieldType.STRING, FieldType.BOOLEAN],
                        ["name", "active"])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 3
        assert combined.get_field_name(0) is None
        assert combined.get_field_name(1) == "name"
        assert combined.get_field_name(2) == "active"

    def test_combine_no_names(self):
        """Test combination when neither has field names."""
        td1 = TupleDesc([FieldType.INT])
        td2 = TupleDesc([FieldType.STRING])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 2
        assert combined.field_names is None

    def test_combine_empty_tuples(self):
        """Test combining with single field tuples."""
        td1 = TupleDesc([FieldType.INT])
        td2 = TupleDesc([FieldType.STRING])

        combined = TupleDesc.combine(td1, td2)

        assert combined.num_fields() == 2
        assert combined.get_field_type(0) == FieldType.INT
        assert combined.get_field_type(1) == FieldType.STRING

    def test_python_eq_operator(self):
        """Test Python == operator."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        td2 = TupleDesc([FieldType.INT, FieldType.STRING])
        td3 = TupleDesc([FieldType.INT])

        assert td1 == td2
        assert td1 != td3
        assert td1 != "not a tupledesc"

    def test_str_representation_no_names(self):
        """Test string representation without field names."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        str_repr = str(td)

        assert "TupleDesc" in str_repr
        assert "int" in str_repr
        assert "string" in str_repr
        assert "field_0" in str_repr
        assert "field_1" in str_repr

    def test_str_representation_with_names(self):
        """Test string representation with field names."""
        td = TupleDesc([FieldType.INT, FieldType.STRING], ["id", "name"])
        str_repr = str(td)

        assert "TupleDesc" in str_repr
        assert "int(id)" in str_repr
        assert "string(name)" in str_repr

    def test_repr_representation(self):
        """Test repr representation."""
        td = TupleDesc([FieldType.INT], ["id"])
        assert repr(td) == str(td)

    def test_edge_case_duplicate_field_names(self):
        """Test with duplicate field names (allowed but not recommended)."""
        td = TupleDesc([FieldType.INT, FieldType.STRING], ["name", "name"])

        # name_to_index should return the first occurrence
        assert td.name_to_index("name") == 0

    def test_edge_case_empty_field_names(self):
        """Test with empty field names."""
        td = TupleDesc([FieldType.INT, FieldType.STRING], ["", "name"])

        assert td.get_field_name(0) == ""
        assert td.get_field_name(1) == "name"
        assert td.name_to_index("") == 0
        assert td.name_to_index("name") == 1

    def test_edge_case_none_field_names_in_list(self):
        """Test with None values in field names list."""
        # This would require modifying the constructor to allow None values in the list
        # For now, the constructor expects all strings if field_names is provided

    def test_large_number_of_fields(self):
        """Test with a large number of fields."""
        num_fields = 1000
        field_types = [FieldType.INT] * num_fields
        field_names = [f"field_{i}" for i in range(num_fields)]

        td = TupleDesc(field_types, field_names)

        assert td.num_fields() == num_fields
        assert td.get_field_type(0) == FieldType.INT
        assert td.get_field_type(num_fields - 1) == FieldType.INT
        assert td.get_field_name(0) == "field_0"
        assert td.get_field_name(num_fields - 1) == f"field_{num_fields - 1}"
        assert td.name_to_index("field_500") == 500
