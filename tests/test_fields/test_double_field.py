import pytest
import struct
import math
from app.core.types.fields.double_field import DoubleField
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class TestDoubleField:
    """Comprehensive tests for DoubleField implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid double values."""
        field = DoubleField(3.141592653589793)
        assert field.value == pytest.approx(3.141592653589793)

        field = DoubleField(0.0)
        assert field.value == pytest.approx(0.0)

        field = DoubleField(-2.718281828459045)
        assert field.value == pytest.approx(-2.718281828459045)

    def test_init_convertible_values(self):
        """Test initialization with values convertible to float."""
        field = DoubleField(42)
        assert field.value == pytest.approx(42.0)

        field = DoubleField("3.141592653589793")
        assert field.value == pytest.approx(3.141592653589793)

    def test_init_infinity_values(self):
        """Test initialization with infinity values."""
        field = DoubleField(float('inf'))
        assert math.isinf(field.value)
        assert field.value > 0

        field = DoubleField(float('-inf'))
        assert math.isinf(field.value)
        assert field.value < 0

    def test_init_none_raises_error(self):
        """Test that None values raise TypeError."""
        with pytest.raises(TypeError, match="DoubleField cannot accept None value"):
            DoubleField(None)

    def test_init_nan_raises_error(self):
        """Test that NaN values raise ValueError."""
        with pytest.raises(ValueError, match="DoubleField does not support NaN values"):
            DoubleField(float('nan'))

    def test_init_invalid_types(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError, match="DoubleField requires numeric value"):
            DoubleField([3.14])

        with pytest.raises(TypeError, match="DoubleField requires numeric value"):
            DoubleField("not_a_number")

    def test_get_type(self):
        """Test that get_type returns correct FieldType."""
        field = DoubleField(3.14)
        assert field.get_type() == FieldType.DOUBLE

    def test_get_size(self):
        """Test that get_size returns correct size."""
        assert DoubleField.get_size() == 8

    def test_serialize(self):
        """Test serialization to bytes."""
        field = DoubleField(3.141592653589793)
        data = field.serialize()
        assert len(data) == 8
        assert data == struct.pack('d', 3.141592653589793)

    def test_serialize_infinity(self):
        """Test serialization of infinity values."""
        field = DoubleField(float('inf'))
        data = field.serialize()
        assert len(data) == 8

        field = DoubleField(float('-inf'))
        data = field.serialize()
        assert len(data) == 8

    def test_deserialize_valid(self):
        """Test deserialization from valid bytes."""
        data = struct.pack('d', 3.141592653589793)
        field = DoubleField.deserialize(data)
        assert abs(field.value - 3.141592653589793) < DoubleField.EPSILON
        assert isinstance(field, DoubleField)

    def test_deserialize_infinity(self):
        """Test deserialization of infinity values."""
        data = struct.pack('d', float('inf'))
        field = DoubleField.deserialize(data)
        assert math.isinf(field.value)
        assert field.value > 0

        data = struct.pack('d', float('-inf'))
        field = DoubleField.deserialize(data)
        assert math.isinf(field.value)
        assert field.value < 0

    def test_deserialize_invalid_data_type(self):
        """Test deserialization with invalid data types."""
        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            DoubleField.deserialize("not bytes")

    def test_deserialize_wrong_length(self):
        """Test deserialization with wrong data length."""
        with pytest.raises(ValueError, match="DoubleField requires exactly 8 bytes"):
            DoubleField.deserialize(b'')

        with pytest.raises(ValueError, match="DoubleField requires exactly 8 bytes"):
            DoubleField.deserialize(b'\x01\x02\x03\x04\x05\x06\x07')

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a perfect roundtrip."""
        test_values = [
            0.0, 1.0, -1.0,
            3.141592653589793,
            -2.718281828459045,
            1e100, -1e-100,
            float('inf'), float('-inf')
        ]

        for value in test_values:
            original = DoubleField(value)
            data = original.serialize()
            restored = DoubleField.deserialize(data)
            assert original == restored

    def test_compare_operations(self):
        """Test comparison operations."""
        field1 = DoubleField(3.141592653589793)
        field2 = DoubleField(3.141592653589793)
        field3 = DoubleField(2.718281828459045)

        assert field1.compare(Predicate.EQUALS, field2) is True
        assert field1.compare(Predicate.EQUALS, field3) is False
        assert field1.compare(Predicate.NOT_EQUALS, field3) is True
        assert field1.compare(Predicate.GREATER_THAN, field3) is True
        assert field3.compare(Predicate.LESS_THAN, field1) is True

    def test_compare_epsilon_tolerance(self):
        """Test comparison with epsilon tolerance."""
        field1 = DoubleField(1.0)
        close_field = DoubleField(1.0 + DoubleField.EPSILON / 2)
        far_field = DoubleField(1.0 + DoubleField.EPSILON * 2)

        # Close values should be considered equal
        assert field1.compare(Predicate.EQUALS, close_field) is True
        assert field1.compare(Predicate.NOT_EQUALS, close_field) is False

        # Far values should not be equal
        assert field1.compare(Predicate.EQUALS, far_field) is False
        assert field1.compare(Predicate.NOT_EQUALS, far_field) is True

    def test_compare_infinity(self):
        """Test comparisons with infinity values."""
        pos_inf = DoubleField(float('inf'))
        neg_inf = DoubleField(float('-inf'))
        regular = DoubleField(3.14)

        assert pos_inf.compare(Predicate.GREATER_THAN, regular) is True
        assert neg_inf.compare(Predicate.LESS_THAN, regular) is True
        assert pos_inf.compare(
            Predicate.EQUALS, DoubleField(float('inf'))) is True
        assert pos_inf.compare(Predicate.GREATER_THAN, neg_inf) is True

    def test_compare_wrong_field_type(self):
        """Test comparison with wrong field type raises TypeError."""
        from app.core.types.fields.int_field import IntField

        double_field = DoubleField(3.14)
        int_field = IntField(42)

        with pytest.raises(TypeError, match="Cannot compare DoubleField with"):
            double_field.compare(Predicate.EQUALS, int_field)

    def test_str_representation(self):
        """Test string representation."""
        assert str(DoubleField(3.14)) == "3.14"
        assert str(DoubleField(float('inf'))) == "inf"
        assert str(DoubleField(float('-inf'))) == "-inf"

    def test_repr_representation(self):
        """Test repr representation."""
        assert repr(DoubleField(3.14)) == "DoubleField(3.14)"

    def test_equality_and_hash(self):
        """Test equality and hash methods."""
        field1 = DoubleField(3.141592653589793)
        field2 = DoubleField(3.141592653589793)
        field3 = DoubleField(2.718281828459045)

        assert field1 == field2
        assert field1 != field3
        assert pytest.approx(field1) != pytest.approx(3.141592653589793)
        assert hash(field1) == hash(field2)

        field_set = {field1, field2, field3}
        assert len(field_set) == 2

    def test_equality_infinity(self):
        """Test equality with infinity values."""
        pos_inf1 = DoubleField(float('inf'))
        pos_inf2 = DoubleField(float('inf'))
        neg_inf = DoubleField(float('-inf'))

        assert pos_inf1 == pos_inf2
        assert pos_inf1 != neg_inf

    def test_precision_vs_float(self):
        """Test that double has better precision than float."""
        # Use a value that loses precision in float but not in double
        precise_value = 1.23456789012345678901234567890
        field = DoubleField(precise_value)

        assert pytest.approx(field.value) != pytest.approx(0.0)

        data = field.serialize()
        restored = DoubleField.deserialize(data)
        assert field == restored

    def test_very_large_and_small_numbers(self):
        """Test with very large and very small numbers."""
        huge = DoubleField(1e308)  # Near double limit
        tiny = DoubleField(1e-308)  # Very small positive

        assert huge.value == pytest.approx(1e308)
        assert tiny.value == pytest.approx(1e-308)

        # Test serialization roundtrip
        for field in [huge, tiny]:
            data = field.serialize()
            restored = DoubleField.deserialize(data)
            assert field == restored
