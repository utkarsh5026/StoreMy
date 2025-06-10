import pytest
from app.core.tuple.tuple import Tuple
from app.core.tuple.tuple_desc import TupleDesc
from app.core.tuple.record_id import RecordId
from app.core.types.type_enum import FieldType
from app.core.types.fields.int_field import IntField
from app.core.types.fields.string_field import StringField
from app.core.types.fields.boolean_field import BoolField
from app.core.types.fields.float_field import FloatField
from app.core.types.fields.double_field import DoubleField
from app.storage.page.heap_page_id import HeapPageId


class TestTuple:
    """Comprehensive tests for Tuple implementation."""

    def test_init_basic(self):
        """Test basic tuple initialization."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        assert tuple_obj.get_tuple_desc() == td
        assert tuple_obj.get_record_id() is None
        assert not tuple_obj.is_complete()

    def test_init_single_field(self):
        """Test initialization with single field."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        assert tuple_obj.get_tuple_desc().num_fields() == 1
        assert not tuple_obj.is_complete()

    def test_init_multiple_field_types(self):
        """Test initialization with all field types."""
        field_types = [
            FieldType.INT, FieldType.STRING, FieldType.BOOLEAN,
            FieldType.FLOAT, FieldType.DOUBLE
        ]
        td = TupleDesc(field_types)
        tuple_obj = Tuple(td)

        assert tuple_obj.get_tuple_desc().num_fields() == 5
        assert not tuple_obj.is_complete()

    def test_set_field_valid(self):
        """Test setting fields with valid types."""
        td = TupleDesc([FieldType.INT, FieldType.STRING, FieldType.BOOLEAN])
        tuple_obj = Tuple(td)

        int_field = IntField(42)
        string_field = StringField("hello")
        bool_field = BoolField(True)

        tuple_obj.set_field(0, int_field)
        tuple_obj.set_field(1, string_field)
        tuple_obj.set_field(2, bool_field)

        assert tuple_obj.get_field(0) == int_field
        assert tuple_obj.get_field(1) == string_field
        assert tuple_obj.get_field(2) == bool_field
        assert tuple_obj.is_complete()

    def test_set_field_wrong_type(self):
        """Test setting field with wrong type raises TypeError."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        string_field = StringField("hello")

        with pytest.raises(TypeError, match="Field 0 expects FieldType.INT, got FieldType.STRING"):
            tuple_obj.set_field(0, string_field)

    def test_set_field_invalid_index(self):
        """Test setting field with invalid index raises IndexError."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        int_field = IntField(42)

        with pytest.raises(IndexError, match="Field index -1 out of range"):
            tuple_obj.set_field(-1, int_field)

        with pytest.raises(IndexError, match="Field index 1 out of range"):
            tuple_obj.set_field(1, int_field)

    def test_get_field_valid(self):
        """Test getting fields that have been set."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        int_field = IntField(42)
        string_field = StringField("hello")

        tuple_obj.set_field(0, int_field)
        tuple_obj.set_field(1, string_field)

        assert tuple_obj.get_field(0) == int_field
        assert tuple_obj.get_field(1) == string_field

    def test_get_field_not_set(self):
        """Test getting field that hasn't been set raises ValueError."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        with pytest.raises(ValueError, match="Field 0 has not been set"):
            tuple_obj.get_field(0)

        with pytest.raises(ValueError, match="Field 1 has not been set"):
            tuple_obj.get_field(1)

    def test_get_field_invalid_index(self):
        """Test getting field with invalid index raises IndexError."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        with pytest.raises(IndexError, match="Field index -1 out of range"):
            tuple_obj.get_field(-1)

        with pytest.raises(IndexError, match="Field index 1 out of range"):
            tuple_obj.get_field(1)

    def test_is_complete_empty_tuple(self):
        """Test is_complete with empty tuple."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        assert not tuple_obj.is_complete()

    def test_is_complete_partial_tuple(self):
        """Test is_complete with partially filled tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))
        assert not tuple_obj.is_complete()

    def test_is_complete_full_tuple(self):
        """Test is_complete with fully filled tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("hello"))
        assert tuple_obj.is_complete()

    def test_record_id_operations(self):
        """Test record ID getting and setting."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        assert tuple_obj.get_record_id() is None

        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)

        tuple_obj.set_record_id(record_id)
        assert tuple_obj.get_record_id() == record_id

        tuple_obj.set_record_id(None)
        assert tuple_obj.get_record_id() is None

    def test_combine_basic(self):
        """Test basic tuple combination."""
        # First tuple
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple1 = Tuple(td1)
        tuple1.set_field(0, IntField(42))
        tuple1.set_field(1, StringField("hello"))

        # Second tuple
        td2 = TupleDesc([FieldType.BOOLEAN])
        tuple2 = Tuple(td2)
        tuple2.set_field(0, BoolField(True))

        # Combine
        combined = Tuple.combine(tuple1, tuple2)

        assert combined.get_tuple_desc().num_fields() == 3
        assert combined.get_field(0).value == 42
        assert combined.get_field(1).value == "hello"
        assert combined.get_field(2).value is True
        assert combined.is_complete()

    def test_combine_with_record_id(self):
        """Test that combination preserves first tuple's record ID."""
        td1 = TupleDesc([FieldType.INT])
        tuple1 = Tuple(td1)
        tuple1.set_field(0, IntField(1))

        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)
        tuple1.set_record_id(record_id)

        td2 = TupleDesc([FieldType.STRING])
        tuple2 = Tuple(td2)
        tuple2.set_field(0, StringField("test"))

        combined = Tuple.combine(tuple1, tuple2)
        assert combined.get_record_id() == record_id

    def test_combine_partial_tuples(self):
        """Test combining tuples with missing fields."""
        td1 = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple1 = Tuple(td1)
        tuple1.set_field(0, IntField(42))  # Leave field 1 unset

        td2 = TupleDesc([FieldType.BOOLEAN])
        tuple2 = Tuple(td2)
        tuple2.set_field(0, BoolField(False))

        combined = Tuple.combine(tuple1, tuple2)

        assert combined.get_field(0).value == 42
        assert combined.get_field(2).value is False
        assert not combined.is_complete()  # Field 1 is still unset

    def test_serialize_complete_tuple(self):
        """Test serialization of complete tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING, FieldType.BOOLEAN])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("hello"))
        tuple_obj.set_field(2, BoolField(True))

        data = tuple_obj.serialize()

        expected_size = (
            FieldType.INT.get_length() +
            FieldType.STRING.get_length() +
            FieldType.BOOLEAN.get_length()
        )
        assert len(data) == expected_size

    def test_serialize_incomplete_tuple_raises_error(self):
        """Test that serializing incomplete tuple raises ValueError."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))  # Leave field 1 unset

        with pytest.raises(ValueError, match="Cannot serialize incomplete tuple"):
            tuple_obj.serialize()

    def test_deserialize_valid(self):
        """Test deserialization from valid bytes."""
        # Create original tuple
        td = TupleDesc([FieldType.INT, FieldType.STRING, FieldType.BOOLEAN])
        original = Tuple(td)
        original.set_field(0, IntField(42))
        original.set_field(1, StringField("hello"))
        original.set_field(2, BoolField(True))

        # Serialize
        data = original.serialize()

        # Deserialize
        restored = Tuple.deserialize(data, td)

        assert restored.get_field(0).value == 42
        assert restored.get_field(1).value == "hello"
        assert restored.get_field(2).value is True
        assert restored.is_complete()

    def test_deserialize_all_field_types(self):
        """Test deserialization with all field types."""
        field_types = [
            FieldType.INT, FieldType.STRING, FieldType.BOOLEAN,
            FieldType.FLOAT, FieldType.DOUBLE
        ]
        td = TupleDesc(field_types)
        original = Tuple(td)

        original.set_field(0, IntField(42))
        original.set_field(1, StringField("test"))
        original.set_field(2, BoolField(False))
        original.set_field(3, FloatField(3.14))
        original.set_field(4, DoubleField(2.718281828))

        data = original.serialize()
        restored = Tuple.deserialize(data, td)

        assert restored.get_field(0).value == 42
        assert restored.get_field(1).value == "test"
        assert restored.get_field(2).value is False
        assert restored.get_field(3).value == pytest.approx(3.14)
        assert restored.get_field(4).value == pytest.approx(2.718281828)

    def test_deserialize_insufficient_data(self):
        """Test deserialization with insufficient data."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])

        # Create data that's too short
        short_data = b'\x2a\x00\x00\x00'  # Only 4 bytes for int, missing string

        with pytest.raises(ValueError, match="Insufficient data to deserialize field 1"):
            Tuple.deserialize(short_data, td)

    def test_deserialize_unknown_field_type(self):
        """Test deserialization with unknown field type."""
        # This would require creating a mock FieldType, which might be complex
        # For now, we test the existing field types only
        pass

    def test_str_representation_complete(self):
        """Test string representation of complete tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING, FieldType.BOOLEAN])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("hello"))
        tuple_obj.set_field(2, BoolField(True))

        str_repr = str(tuple_obj)
        assert "42" in str_repr
        assert "hello" in str_repr
        assert "True" in str_repr
        assert "\t" in str_repr  # Tab-separated

    def test_str_representation_incomplete(self):
        """Test string representation of incomplete tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))

        str_repr = str(tuple_obj)
        assert str_repr == "<incomplete tuple>"

    def test_repr_representation(self):
        """Test repr representation."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, IntField(42))

        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)
        tuple_obj.set_record_id(record_id)

        repr_str = repr(tuple_obj)
        assert "Tuple(" in repr_str
        assert "record_id=" in repr_str

    def test_equality_same_tuples(self):
        """Test equality with identical tuples."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])

        tuple1 = Tuple(td)
        tuple1.set_field(0, IntField(42))
        tuple1.set_field(1, StringField("hello"))

        tuple2 = Tuple(td)
        tuple2.set_field(0, IntField(42))
        tuple2.set_field(1, StringField("hello"))

        assert tuple1 == tuple2

    def test_equality_different_values(self):
        """Test inequality with different values."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])

        tuple1 = Tuple(td)
        tuple1.set_field(0, IntField(42))
        tuple1.set_field(1, StringField("hello"))

        tuple2 = Tuple(td)
        tuple2.set_field(0, IntField(43))  # Different value
        tuple2.set_field(1, StringField("hello"))

        assert tuple1 != tuple2

    def test_equality_different_schemas(self):
        """Test inequality with different schemas."""
        td1 = TupleDesc([FieldType.INT])
        td2 = TupleDesc([FieldType.STRING])

        tuple1 = Tuple(td1)
        tuple1.set_field(0, IntField(42))

        tuple2 = Tuple(td2)
        tuple2.set_field(0, StringField("42"))

        assert tuple1 != tuple2

    def test_equality_ignores_record_id(self):
        """Test that equality ignores record ID."""
        td = TupleDesc([FieldType.INT])

        tuple1 = Tuple(td)
        tuple1.set_field(0, IntField(42))
        tuple1.set_record_id(RecordId(HeapPageId(1, 0), 1))

        tuple2 = Tuple(td)
        tuple2.set_field(0, IntField(42))
        tuple2.set_record_id(RecordId(HeapPageId(2, 0), 2))

        assert tuple1 == tuple2  # Should be equal despite different record IDs

    def test_equality_different_types(self):
        """Test inequality with different types."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, IntField(42))

        assert tuple_obj != "not a tuple"
        assert tuple_obj != 42
        assert tuple_obj != None

    def test_hash_complete_tuple(self):
        """Test hashing of complete tuple."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("hello"))

        hash_value = hash(tuple_obj)
        assert isinstance(hash_value, int)

    def test_hash_incomplete_tuple_raises_error(self):
        """Test that hashing incomplete tuple raises ValueError."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, IntField(42))

        with pytest.raises(ValueError, match="Cannot hash incomplete tuple"):
            hash(tuple_obj)

    def test_hash_consistency(self):
        """Test that equal tuples have equal hashes."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])

        tuple1 = Tuple(td)
        tuple1.set_field(0, IntField(42))
        tuple1.set_field(1, StringField("hello"))

        tuple2 = Tuple(td)
        tuple2.set_field(0, IntField(42))
        tuple2.set_field(1, StringField("hello"))

        assert tuple1 == tuple2
        assert hash(tuple1) == hash(tuple2)

    def test_hash_usable_in_collections(self):
        """Test that tuples can be used in sets and dicts."""
        td = TupleDesc([FieldType.INT])

        tuple1 = Tuple(td)
        tuple1.set_field(0, IntField(42))

        tuple2 = Tuple(td)
        tuple2.set_field(0, IntField(42))  # Same as tuple1

        tuple3 = Tuple(td)
        tuple3.set_field(0, IntField(43))  # Different from tuple1

        # Test in set
        tuple_set = {tuple1, tuple2, tuple3}
        assert len(tuple_set) == 2  # tuple1 and tuple2 are equal

        # Test in dict
        tuple_dict = {tuple1: "first", tuple3: "third"}
        assert len(tuple_dict) == 2
        assert tuple_dict[tuple2] == "first"  # tuple2 equals tuple1

    def test_serialize_deserialize_roundtrip(self):
        """Test complete serialize/deserialize roundtrip."""
        td = TupleDesc([FieldType.INT, FieldType.STRING, FieldType.BOOLEAN])
        original = Tuple(td)

        original.set_field(0, IntField(12345))
        original.set_field(1, StringField("test string"))
        original.set_field(2, BoolField(False))

        data = original.serialize()
        restored = Tuple.deserialize(data, td)

        assert original == restored

    def test_edge_case_empty_string(self):
        """Test with empty string field."""
        td = TupleDesc([FieldType.STRING])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, StringField(""))

        assert tuple_obj.get_field(0).value == ""
        assert tuple_obj.is_complete()

    def test_edge_case_large_values(self):
        """Test with large field values."""
        td = TupleDesc([FieldType.INT, FieldType.STRING])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(2147483647))  # Max int32
        tuple_obj.set_field(1, StringField("x" * 100))  # Long string

        assert tuple_obj.get_field(0).value == 2147483647
        assert tuple_obj.get_field(1).value == "x" * 100

    def test_edge_case_single_field_tuple(self):
        """Test tuple with single field."""
        td = TupleDesc([FieldType.BOOLEAN])
        tuple_obj = Tuple(td)
        tuple_obj.set_field(0, BoolField(True))

        assert tuple_obj.is_complete()
        assert tuple_obj.get_field(0).value is True

    def test_edge_case_overwrite_field(self):
        """Test overwriting a field that's already set."""
        td = TupleDesc([FieldType.INT])
        tuple_obj = Tuple(td)

        tuple_obj.set_field(0, IntField(42))
        assert tuple_obj.get_field(0).value == 42

        tuple_obj.set_field(0, IntField(100))  # Overwrite
        assert tuple_obj.get_field(0).value == 100
