import pytest
import struct
import math
from app.core.types.fields.float_field import FloatField
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class TestFloatField:
    """Comprehensive tests for FloatField implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid float values."""
        field = FloatField(3.14)
        assert field.value == pytest.approx(3.14)

        field = FloatField(0.0)
        assert field.value == pytest.approx(0.0)

        field = FloatField(-2.5)
        assert field.value == pytest.approx(-2.5)

    def test_init_convertible_values(self):
        """Test initialization with values convertible to float."""
        field = FloatField(42)
        assert field.value == pytest.approx(42.0)

        field = FloatField("3.14")
        assert field.value == pytest.approx(3.14)

    def test_init_infinity_values(self):
        """Test initialization with infinity values."""
        field = FloatField(float('inf'))
        assert math.isinf(field.value)
        assert field.value > 0

        field = FloatField(float('-inf'))
        assert math.isinf(field.value)
        assert field.value < 0

    def test_init_none_raises_error(self):
        """Test that None values raise TypeError."""
        with pytest.raises(TypeError, match="FloatField cannot accept None value"):
            FloatField(None)

    def test_init_nan_raises_error(self):
        """Test that NaN values raise ValueError."""
        with pytest.raises(ValueError, match="FloatField does not support NaN values"):
            FloatField(float('nan'))

    def test_init_invalid_types(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError, match="FloatField requires numeric value"):
            FloatField([3.14])

        with pytest.raises(TypeError, match="FloatField requires numeric value"):
            FloatField("not_a_number")

    def test_get_type(self):
        """Test that get_type returns correct FieldType."""
        field = FloatField(3.14)
        assert field.get_type() == FieldType.FLOAT

    def test_get_size(self):
        """Test that get_size returns correct size."""
        assert FloatField.get_size() == 4

    def test_serialize(self):
        """Test serialization to bytes."""
        field = FloatField(3.14)
        data = field.serialize()
        assert len(data) == 4
        assert data == struct.pack('f', 3.14)

    def test_deserialize_valid(self):
        """Test deserialization from valid bytes."""
        data = struct.pack('f', 3.14)
        field = FloatField.deserialize(data)
        assert abs(field.value - 3.14) < FloatField.EPSILON
        assert isinstance(field, FloatField)

    def test_deserialize_invalid_data_type(self):
        """Test deserialization with invalid data types."""
        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            FloatField.deserialize("not bytes")

    def test_deserialize_wrong_length(self):
        """Test deserialization with wrong data length."""
        with pytest.raises(ValueError, match="FloatField requires exactly 4 bytes"):
            FloatField.deserialize(b'')

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a perfect roundtrip."""
        test_values = [0.0, 1.0, -1.0, 3.14, -2.5, 1e6, -1e-6]

        for value in test_values:
            original = FloatField(value)
            data = original.serialize()
            restored = FloatField.deserialize(data)
            assert original == restored

    def test_compare_operations(self):
        """Test comparison operations."""
        field1 = FloatField(3.14)
        field2 = FloatField(3.14)
        field3 = FloatField(3.15)

        assert field1.compare(Predicate.EQUALS, field2) is True
        assert field1.compare(Predicate.EQUALS, field3) is False
        assert field1.compare(Predicate.NOT_EQUALS, field3) is True
        assert field3.compare(Predicate.GREATER_THAN, field1) is True
        assert field1.compare(Predicate.LESS_THAN, field3) is True

    def test_compare_infinity(self):
        """Test comparisons with infinity values."""
        pos_inf = FloatField(float('inf'))
        neg_inf = FloatField(float('-inf'))
        regular = FloatField(3.14)

        assert pos_inf.compare(Predicate.GREATER_THAN, regular) is True
        assert neg_inf.compare(Predicate.LESS_THAN, regular) is True
        assert pos_inf.compare(
            Predicate.EQUALS, FloatField(float('inf'))) is True

    def test_compare_wrong_field_type(self):
        """Test comparison with wrong field type raises TypeError."""
        from app.core.types.fields.int_field import IntField

        float_field = FloatField(3.14)
        int_field = IntField(42)

        with pytest.raises(TypeError, match="Cannot compare FloatField with"):
            float_field.compare(Predicate.EQUALS, int_field)

    def test_str_representation(self):
        """Test string representation."""
        assert str(FloatField(3.14)) == "3.14"
        assert str(FloatField(float('inf'))) == "inf"
        assert str(FloatField(float('-inf'))) == "-inf"

    def test_repr_representation(self):
        """Test repr representation."""
        assert repr(FloatField(3.14)) == "FloatField(3.14)"

    def test_equality_and_hash(self):
        """Test equality and hash methods."""
        field1 = FloatField(3.14)
        field2 = FloatField(3.14)
        field3 = FloatField(3.15)

        assert field1 == field2
        assert field1 != field3
        assert field1 != 3.14  # Different type
        assert hash(field1) == hash(field2)

        field_set = {field1, field2, field3}
        assert len(field_set) == 2

    def test_precision_limits(self):
        """Test float precision limits."""
        base = FloatField(1.0)
        almost_equal = FloatField(1.0 + FloatField.EPSILON / 2)
        not_equal = FloatField(1.0 + FloatField.EPSILON * 2)

        assert base == almost_equal
        assert base != not_equal
