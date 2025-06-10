import pytest
import struct
from app.core.types.fields.int_field import IntField
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class TestIntField:
    """Comprehensive tests for IntField implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid integer values."""
        field = IntField(42)
        assert field.value == 42

        field = IntField(0)
        assert field.value == 0

        field = IntField(-123)
        assert field.value == -123

    def test_init_boundary_values(self):
        """Test initialization with boundary values."""
        min_field = IntField(IntField.MIN_VALUE)
        assert min_field.value == IntField.MIN_VALUE

        max_field = IntField(IntField.MAX_VALUE)
        assert max_field.value == IntField.MAX_VALUE

    def test_init_out_of_range_values(self):
        """Test that out-of-range values raise ValueError."""
        with pytest.raises(ValueError, match="Integer value .* out of range"):
            IntField(IntField.MAX_VALUE + 1)

        with pytest.raises(ValueError, match="Integer value .* out of range"):
            IntField(IntField.MIN_VALUE - 1)

    def test_init_invalid_types(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError, match="IntField requires int"):
            IntField("42")

        with pytest.raises(TypeError, match="IntField requires int"):
            IntField(None)

    def test_get_type(self):
        """Test that get_type returns correct FieldType."""
        field = IntField(42)
        assert field.get_type() == FieldType.INT

    def test_get_size(self):
        """Test that get_size returns correct size."""
        assert IntField.get_size() == 4

    def test_serialize(self):
        """Test serialization to bytes."""
        field = IntField(42)
        data = field.serialize()
        assert len(data) == 4
        assert data == struct.pack('<i', 42)

    def test_deserialize_valid(self):
        """Test deserialization from valid bytes."""
        data = struct.pack('<i', 42)
        field = IntField.deserialize(data)
        assert field.value == 42
        assert isinstance(field, IntField)

    def test_deserialize_invalid_data_type(self):
        """Test deserialization with invalid data types."""
        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            IntField.deserialize("not bytes")

    def test_deserialize_wrong_length(self):
        """Test deserialization with wrong data length."""
        with pytest.raises(ValueError, match="IntField requires exactly 4 bytes"):
            IntField.deserialize(b'')

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a perfect roundtrip."""
        test_values = [0, 1, -1, 42, -123, 1000000, -1000000]

        for value in test_values:
            original = IntField(value)
            data = original.serialize()
            restored = IntField.deserialize(data)
            assert original == restored

    def test_compare_operations(self):
        """Test all comparison operations."""
        field1 = IntField(42)
        field2 = IntField(42)
        field3 = IntField(100)

        assert field1.compare(Predicate.EQUALS, field2) is True
        assert field1.compare(Predicate.EQUALS, field3) is False
        assert field1.compare(Predicate.NOT_EQUALS, field3) is True
        assert field3.compare(Predicate.GREATER_THAN, field1) is True
        assert field1.compare(Predicate.LESS_THAN, field3) is True

    def test_compare_wrong_field_type(self):
        """Test comparison with wrong field type raises TypeError."""
        from app.core.types.fields.boolean_field import BoolField

        int_field = IntField(42)
        bool_field = BoolField(True)

        with pytest.raises(TypeError, match="Cannot compare IntField with"):
            int_field.compare(Predicate.EQUALS, bool_field)

    def test_str_and_repr(self):
        """Test string representations."""
        field = IntField(42)
        assert str(field) == "42"
        assert repr(field) == "IntField(42)"

    def test_equality_and_hash(self):
        """Test equality and hash methods."""
        field1 = IntField(42)
        field2 = IntField(42)
        field3 = IntField(100)

        assert field1 == field2
        assert field1 != field3
        assert field1 != 42  # Different type
        assert hash(field1) == hash(field2)

        field_set = {field1, field2, field3}
        assert len(field_set) == 2
