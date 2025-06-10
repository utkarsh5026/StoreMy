import pytest
import struct
from app.core.types.fields.string_field import StringField
from app.core.types.type_enum import FieldType
from app.core.types.predicate import Predicate


class TestStringField:
    """Comprehensive tests for StringField implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid string values."""
        field = StringField("hello")
        assert field.value == "hello"

        field = StringField("")
        assert field.value == ""

        field = StringField("Hello, 世界!")  # Unicode
        assert field.value == "Hello, 世界!"

    def test_init_convertible_values(self):
        """Test initialization with values convertible to string."""
        field = StringField(42)
        assert field.value == "42"

        field = StringField(3.14)
        assert field.value == "3.14"

    def test_init_none_raises_error(self):
        """Test that None values raise TypeError."""
        with pytest.raises(TypeError, match="StringField cannot accept None value"):
            StringField(None)

    def test_init_null_bytes_raises_error(self):
        """Test that strings with null bytes raise ValueError."""
        with pytest.raises(ValueError, match="StringField cannot contain null bytes"):
            StringField("hello\x00world")

    def test_init_too_long_string_raises_error(self):
        """Test that too long strings raise ValueError."""
        # Create a string that's too long when encoded as UTF-8
        long_string = "x" * (StringField.MAX_LENGTH_IN_BYTES + 1)
        with pytest.raises(ValueError, match="String too long"):
            StringField(long_string)

    def test_init_unicode_length_validation(self):
        """Test that Unicode string length is properly validated."""
        # Unicode characters can take multiple bytes
        # "🙂" takes 4 bytes in UTF-8
        max_emoji = "🙂" * (StringField.MAX_LENGTH_IN_BYTES // 4)
        StringField(max_emoji)  # Should work

        # One more emoji should fail
        with pytest.raises(ValueError, match="String too long"):
            StringField(max_emoji + "🙂")

    def test_init_invalid_conversion(self):
        """Test initialization with values that can't be converted to string."""
        class BadConversion:
            def __str__(self):
                raise ValueError("Can't convert")

        with pytest.raises(TypeError, match="StringField requires str"):
            StringField(BadConversion())

    def test_get_type(self):
        """Test that get_type returns correct FieldType."""
        field = StringField("hello")
        assert field.get_type() == FieldType.STRING

    def test_get_size(self):
        """Test that get_size returns correct size."""
        assert StringField.get_size() == 132

    def test_serialize(self):
        """Test serialization to bytes."""
        field = StringField("hello")
        data = field.serialize()
        assert len(data) == 132

        # Check length prefix
        length = struct.unpack('<i', data[:4])[0]
        assert length == 5  # "hello" is 5 bytes

        # Check content
        content = data[4:4+length]
        assert content == b'hello'

        # Check padding
        padding = data[4+length:]
        assert padding == b'\x00' * (128 - length)

    def test_serialize_empty_string(self):
        """Test serialization of empty string."""
        field = StringField("")
        data = field.serialize()
        assert len(data) == 132

        length = struct.unpack('<i', data[:4])[0]
        assert length == 0

        # All content should be null padding
        content = data[4:]
        assert content == b'\x00' * 128

    def test_serialize_unicode(self):
        """Test serialization of Unicode strings."""
        field = StringField("Hello, 世界!")
        data = field.serialize()
        assert len(data) == 132

        # UTF-8 encoding of "Hello, 世界!" should be longer than 11 chars
        expected_bytes = "Hello, 世界!".encode('utf-8')
        length = struct.unpack('<i', data[:4])[0]
        assert length == len(expected_bytes)

        content = data[4:4+length]
        assert content == expected_bytes

    def test_deserialize_valid(self):
        """Test deserialization from valid bytes."""
        original = StringField("hello")
        data = original.serialize()
        restored = StringField.deserialize(data)

        assert restored.value == "hello"
        assert isinstance(restored, StringField)
        assert restored == original

    def test_deserialize_empty_string(self):
        """Test deserialization of empty string."""
        original = StringField("")
        data = original.serialize()
        restored = StringField.deserialize(data)

        assert restored.value == ""
        assert restored == original

    def test_deserialize_unicode(self):
        """Test deserialization of Unicode strings."""
        original = StringField("Hello, 世界!")
        data = original.serialize()
        restored = StringField.deserialize(data)

        assert restored.value == "Hello, 世界!"
        assert restored == original

    def test_deserialize_invalid_data_type(self):
        """Test deserialization with invalid data types."""
        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            StringField.deserialize("not bytes")

        with pytest.raises(TypeError, match="Expected bytes or bytearray"):
            StringField.deserialize(123)

    def test_deserialize_wrong_length(self):
        """Test deserialization with wrong data length."""
        with pytest.raises(ValueError, match="StringField requires exactly 132 bytes"):
            StringField.deserialize(b'')

        with pytest.raises(ValueError, match="StringField requires exactly 132 bytes"):
            StringField.deserialize(b'\x00' * 131)

        with pytest.raises(ValueError, match="StringField requires exactly 132 bytes"):
            StringField.deserialize(b'\x00' * 133)

    def test_deserialize_invalid_length_field(self):
        """Test deserialization with invalid length field."""
        # Negative length
        data = struct.pack('<i', -1) + b'\x00' * 128
        with pytest.raises(ValueError, match="Invalid string length: -1"):
            StringField.deserialize(data)

        # Length too large
        data = struct.pack('<i', 129) + b'\x00' * 128
        with pytest.raises(ValueError, match="Invalid string length: 129"):
            StringField.deserialize(data)

    def test_deserialize_invalid_utf8(self):
        """Test deserialization with invalid UTF-8 data."""
        # Create data with invalid UTF-8 sequence
        length = 2
        data = struct.pack('<i', length) + b'\xff\xfe' + b'\x00' * 126
        with pytest.raises(ValueError, match="Invalid UTF-8 data"):
            StringField.deserialize(data)

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a perfect roundtrip."""
        test_strings = [
            "",
            "hello",
            "Hello, World!",
            "Hello, 世界!",
            "🙂😊🎉",
            "a" * 100,  # Long ASCII string
            "x" * StringField.MAX_LENGTH_IN_BYTES,  # Maximum length
        ]

        for string_value in test_strings:
            original = StringField(string_value)
            data = original.serialize()
            restored = StringField.deserialize(data)
            assert original == restored
            assert original.value == restored.value

    def test_compare_operations(self):
        """Test comparison operations."""
        field1 = StringField("hello")
        field2 = StringField("hello")
        field3 = StringField("world")

        assert field1.compare(Predicate.EQUALS, field2) is True
        assert field1.compare(Predicate.EQUALS, field3) is False
        assert field1.compare(Predicate.NOT_EQUALS, field3) is True
        assert field3.compare(Predicate.GREATER_THAN,
                              field1) is True  # lexicographic
        assert field1.compare(Predicate.LESS_THAN, field3) is True

    def test_compare_like(self):
        """Test LIKE comparison (substring matching)."""
        field1 = StringField("hello world")
        field2 = StringField("world")
        field3 = StringField("foo")

        # "world" in "hello world"
        assert field1.compare(Predicate.LIKE, field2) is True
        # "foo" not in "hello world"
        assert field1.compare(Predicate.LIKE, field3) is False

    def test_compare_case_sensitivity(self):
        """Test that comparisons are case sensitive."""
        field1 = StringField("Hello")
        field2 = StringField("hello")

        assert field1.compare(Predicate.EQUALS, field2) is False
        assert field1.compare(Predicate.NOT_EQUALS, field2) is True
        assert field1.compare(Predicate.LIKE, field2) is False

    def test_compare_unicode(self):
        """Test comparisons with Unicode strings."""
        field1 = StringField("café")
        field2 = StringField("café")
        field3 = StringField("cafe")

        assert field1.compare(Predicate.EQUALS, field2) is True
        assert field1.compare(Predicate.EQUALS, field3) is False

    def test_compare_wrong_field_type(self):
        """Test comparison with wrong field type raises TypeError."""
        from app.core.types.fields.int_field import IntField

        string_field = StringField("hello")
        int_field = IntField(42)

        with pytest.raises(TypeError, match="Cannot compare StringField with"):
            string_field.compare(Predicate.EQUALS, int_field)

    def test_str_and_repr(self):
        """Test string representations."""
        field = StringField("hello")
        assert str(field) == "hello"
        assert repr(field) == "StringField('hello')"

    def test_equality_and_hash(self):
        """Test equality and hash methods."""
        field1 = StringField("hello")
        field2 = StringField("hello")
        field3 = StringField("world")

        assert field1 == field2
        assert field1 != field3
        assert field1 != "hello"  # Different type
        assert hash(field1) == hash(field2)

        field_set = {field1, field2, field3}
        assert len(field_set) == 2

        field_dict = {field1: "greeting", field3: "planet"}
        assert len(field_dict) == 2
        assert field_dict[field2] == "greeting"  # field2 equals field1

    def test_edge_case_empty_string(self):
        """Test edge cases with empty strings."""
        empty = StringField("")
        assert empty.value == ""

        # Empty string should be less than any non-empty string
        non_empty = StringField("a")
        assert empty.compare(Predicate.LESS_THAN, non_empty) is True
        assert non_empty.compare(Predicate.GREATER_THAN, empty) is True

    def test_edge_case_whitespace(self):
        """Test edge cases with whitespace strings."""
        space = StringField(" ")
        tab = StringField("\t")
        newline = StringField("\n")

        # These should all be different
        assert space != tab
        assert space != newline
        assert tab != newline

    def test_edge_case_maximum_length(self):
        """Test with maximum length strings."""
        # ASCII characters (1 byte each)
        max_ascii = "x" * StringField.MAX_LENGTH_IN_BYTES
        field = StringField(max_ascii)
        assert len(field.value) == StringField.MAX_LENGTH_IN_BYTES

        # Test serialization roundtrip
        data = field.serialize()
        restored = StringField.deserialize(data)
        assert field == restored
