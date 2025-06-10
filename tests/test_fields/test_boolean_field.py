import pytest
import struct
from app.core.types.fields.boolean_field import BoolField
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class TestBoolField:
    """Comprehensive tests for BoolField implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid boolean values."""
        true_field = BoolField(True)
        assert true_field.value is True

        false_field = BoolField(False)
        assert false_field.value is False

    def test_init_truthy_values(self):
        """Test initialization with truthy values."""
        assert BoolField(1).value is True
        assert BoolField(-1).value is True
        assert BoolField(42).value is True
        assert BoolField(3.14).value is True


        assert BoolField("hello").value is True
        assert BoolField("false").value is True

        assert BoolField([1, 2, 3]).value is True
        assert BoolField({"key": "value"}).value is True

    def test_init_falsy_values(self):
        """Test initialization with falsy values."""
        assert BoolField(0).value is False
        assert BoolField(0.0).value is False

        assert BoolField("").value is False
        assert BoolField([]).value is False
        assert BoolField({}).value is False
        assert BoolField(set()).value is False

    def test_init_none_raises_error(self):
        """Test that None values raise TypeError."""
        with pytest.raises(TypeError, match="BoolField cannot accept None value"):
            BoolField(None)

    def test_get_type(self):
        """Test that get_type returns correct FieldType."""
        field = BoolField(True)
        assert field.get_type() == FieldType.BOOLEAN

    def test_get_size(self):
        """Test that get_size returns correct size."""
        assert BoolField.get_size() == 1

    def test_serialize_true(self):
        """Test serialization of True value."""
        field = BoolField(True)
        data = field.serialize()
        assert len(data) == 1
        assert data == struct.pack('?', True)

    def test_serialize_false(self):
        """Test serialization of False value."""
        field = BoolField(False)
        data = field.serialize()
        assert len(data) == 1
        assert data == struct.pack('?', False)

    def test_deserialize_valid_true(self):
        """Test deserialization of True value."""
        data = struct.pack('?', True)
        field = BoolField.deserialize(data)
        assert field.value is True
        assert isinstance(field, BoolField)

    def test_deserialize_valid_false(self):
        """Test deserialization of False value."""
        data = struct.pack('?', False)
        field = BoolField.deserialize(data)
        assert field.value is False
        assert isinstance(field, BoolField)

    def test_deserialize_invalid_data_type(self):
        """Test deserialization with invalid data types."""
        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            BoolField.deserialize("not bytes")

        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            BoolField.deserialize(123)

    def test_deserialize_wrong_length(self):
        """Test deserialization with wrong data length."""
        with pytest.raises(ValueError, match="BoolField requires exactly 1 byte, got 0"):
            BoolField.deserialize(b'')

        with pytest.raises(ValueError, match="BoolField requires exactly 1 byte, got 2"):
            BoolField.deserialize(b'\x01\x02')

    def test_deserialize_corrupted_data(self):
        """Test deserialization with corrupted data."""
        try:
            BoolField.deserialize(b'\xFF')
        except ValueError:
            pass  # Expected if struct.unpack fails

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a perfect roundtrip."""
        original_true = BoolField(True)
        data = original_true.serialize()
        restored_true = BoolField.deserialize(data)
        assert original_true == restored_true

        original_false = BoolField(False)
        data = original_false.serialize()
        restored_false = BoolField.deserialize(data)
        assert original_false == restored_false

    def test_compare_equals(self):
        """Test EQUALS comparison."""
        true_field1 = BoolField(True)
        true_field2 = BoolField(True)
        false_field = BoolField(False)

        assert true_field1.compare(Predicate.EQUALS, true_field2) is True
        assert true_field1.compare(Predicate.EQUALS, false_field) is False
        assert false_field.compare(Predicate.EQUALS, false_field) is True

    def test_compare_not_equals(self):
        """Test NOT_EQUALS comparison."""
        true_field = BoolField(True)
        false_field = BoolField(False)

        assert true_field.compare(Predicate.NOT_EQUALS, false_field) is True
        assert true_field.compare(Predicate.NOT_EQUALS, true_field) is False
        assert false_field.compare(Predicate.NOT_EQUALS, false_field) is False

    def test_compare_unsupported_predicates(self):
        """Test that unsupported predicates raise ValueError."""
        true_field = BoolField(True)
        false_field = BoolField(False)

        unsupported_predicates = [
            Predicate.GREATER_THAN,
            Predicate.LESS_THAN,
            Predicate.GREATER_THAN_OR_EQ,
            Predicate.LESS_THAN_OR_EQ,
            Predicate.LIKE
        ]

        for predicate in unsupported_predicates:
            with pytest.raises(ValueError, match=f"Unsupported predicate {predicate}"):
                true_field.compare(predicate, false_field)

    def test_compare_wrong_field_type(self):
        """Test comparison with wrong field type raises TypeError."""
        from app.core.types.fields.int_field import IntField

        bool_field = BoolField(True)
        int_field = IntField(42)

        with pytest.raises(TypeError, match="Cannot compare BoolField with"):
            bool_field.compare(Predicate.EQUALS, int_field)

    def test_str_representation(self):
        """Test string representation."""
        assert str(BoolField(True)) == "True"
        assert str(BoolField(False)) == "False"

    def test_repr_representation(self):
        """Test repr representation."""
        assert repr(BoolField(True)) == "BoolField(True)"
        assert repr(BoolField(False)) == "BoolField(False)"

    def test_equality(self):
        """Test __eq__ method."""
        true_field1 = BoolField(True)
        true_field2 = BoolField(True)
        false_field = BoolField(False)

        # Same values should be equal
        assert true_field1 == true_field2
        assert false_field == BoolField(False)

        # Different values should not be equal
        assert true_field1 != false_field
        assert false_field != true_field1

        # Different types should not be equal
        assert true_field1 != True  # Not a BoolField
        assert false_field != False  # Not a BoolField
        assert true_field1 != "True"
        assert true_field1 != 1

    def test_hash(self):
        """Test __hash__ method."""
        true_field1 = BoolField(True)
        true_field2 = BoolField(True)
        false_field = BoolField(False)

        # Same values should have same hash
        assert hash(true_field1) == hash(true_field2)
        assert hash(false_field) == hash(BoolField(False))

        # Different values should have different hashes (not guaranteed but likely)
        assert hash(true_field1) != hash(false_field)

        # Should be usable in sets and dicts
        field_set = {true_field1, true_field2, false_field}
        assert len(field_set) == 2  # true_field1 and true_field2 are equal

        field_dict = {true_field1: "true", false_field: "false"}
        assert len(field_dict) == 2
        # true_field2 equals true_field1
        assert field_dict[true_field2] == "true"

    def test_edge_cases_with_different_truthy_values(self):
        """Test that different truthy values all create True BoolFields."""
        truthy_values = [1, -1, "hello", [1], {"a": 1}, object()]

        for value in truthy_values:
            field = BoolField(value)
            assert field.value is True
            # All should be equal to each other
            assert field == BoolField(True)

    def test_edge_cases_with_different_falsy_values(self):
        """Test that different falsy values all create False BoolFields."""
        falsy_values = [0, 0.0, "", [], {}, set()]

        for value in falsy_values:
            field = BoolField(value)
            assert field.value is False
            # All should be equal to each other
            assert field == BoolField(False)
