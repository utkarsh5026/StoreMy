import pytest
from app.core.tuple import RecordId
from app.storage.page.heap_page_id import HeapPageId


class TestRecordId:
    """Comprehensive tests for RecordId implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid values."""
        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)

        assert record_id.get_page_id() == page_id
        assert record_id.get_tuple_number() == 5

    def test_init_zero_tuple_number(self):
        """Test initialization with zero tuple numbers (valid)."""
        page_id = HeapPageId(0, 0)
        record_id = RecordId(page_id, 0)

        assert record_id.get_tuple_number() == 0

    def test_init_large_tuple_number(self):
        """Test initialization with large tuple number."""
        page_id = HeapPageId(999, 100)
        record_id = RecordId(page_id, 999999)

        assert record_id.get_tuple_number() == 999999

    def test_init_negative_tuple_number_raises_error(self):
        """Test that negative tuple numbers raise ValueError."""
        page_id = HeapPageId(1, 0)

        with pytest.raises(ValueError, match="Tuple number must be non-negative, got -1"):
            RecordId(page_id, -1)

        with pytest.raises(ValueError, match="Tuple number must be non-negative, got -100"):
            RecordId(page_id, -100)

    def test_get_page_id(self):
        """Test get_page_id method."""
        page_id = HeapPageId(5, 10)
        record_id = RecordId(page_id, 3)

        returned_page_id = record_id.get_page_id()
        assert returned_page_id == page_id
        assert returned_page_id is page_id  # Should be the same object

    def test_get_tuple_number(self):
        """Test get_tuple_number method."""
        page_id = HeapPageId(1, 2)
        record_id = RecordId(page_id, 42)

        assert record_id.get_tuple_number() == 42

    def test_equality_same_values(self):
        """Test equality with same values."""
        page_id1 = HeapPageId(1, 0)
        page_id2 = HeapPageId(1, 0)  # Same values, different objects

        record_id1 = RecordId(page_id1, 5)
        record_id2 = RecordId(page_id2, 5)

        assert record_id1 == record_id2

    def test_equality_different_page_id(self):
        """Test inequality with different page IDs."""
        page_id1 = HeapPageId(1, 0)
        page_id2 = HeapPageId(2, 0)

        record_id1 = RecordId(page_id1, 5)
        record_id2 = RecordId(page_id2, 5)

        assert record_id1 != record_id2

    def test_equality_different_tuple_number(self):
        """Test inequality with different tuple numbers."""
        page_id = HeapPageId(1, 0)

        record_id1 = RecordId(page_id, 5)
        record_id2 = RecordId(page_id, 6)

        assert record_id1 != record_id2

    def test_equality_different_types(self):
        """Test inequality with different types."""
        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)

        assert record_id != "not a record id"
        assert record_id != 42
        assert record_id != None
        assert record_id != page_id

    def test_equality_same_object(self):
        """Test equality with the same object."""
        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)

        assert record_id == record_id

    def test_hash_consistency(self):
        """Test that equal objects have equal hashes."""
        page_id1 = HeapPageId(1, 0)
        page_id2 = HeapPageId(1, 0)

        record_id1 = RecordId(page_id1, 5)
        record_id2 = RecordId(page_id2, 5)

        assert record_id1 == record_id2
        assert hash(record_id1) == hash(record_id2)

    def test_hash_different_objects(self):
        """Test that different objects have different hashes (likely but not guaranteed)."""
        page_id1 = HeapPageId(1, 0)
        page_id2 = HeapPageId(2, 0)

        record_id1 = RecordId(page_id1, 5)
        record_id2 = RecordId(page_id2, 5)
        record_id3 = RecordId(page_id1, 6)

        assert hash(record_id1) != hash(record_id2)
        assert hash(record_id1) != hash(record_id3)

    def test_hash_usable_in_collections(self):
        """Test that RecordId can be used in sets and dicts."""
        page_id1 = HeapPageId(1, 0)
        page_id2 = HeapPageId(2, 0)

        record_id1 = RecordId(page_id1, 5)
        record_id2 = RecordId(page_id1, 5)  # Equal to record_id1
        record_id3 = RecordId(page_id2, 5)

        # Test in a set
        record_set = {record_id1, record_id2, record_id3}
        assert len(record_set) == 2  # record_id1 and record_id2 are equal

        # Test in dict
        record_dict = {record_id1: "first", record_id3: "third"}
        assert len(record_dict) == 2
        # record_id2 equals record_id1
        assert record_dict[record_id2] == "first"

    def test_str_representation(self):
        """Test string representation."""
        page_id = HeapPageId(1, 2)
        record_id = RecordId(page_id, 5)

        str_repr = str(record_id)
        assert "RecordId" in str_repr
        assert "page=" in str_repr
        assert "tuple=5" in str_repr

    def test_repr_representation(self):
        """Test repr representation."""
        page_id = HeapPageId(1, 2)
        record_id = RecordId(page_id, 5)

        repr_str = repr(record_id)
        assert repr_str == str(record_id)  # They should be the same

    def test_edge_case_maximum_values(self):
        """Test with very large values."""
        page_id = HeapPageId(2**31 - 1, 2**31 - 1)  # Maximum int values
        record_id = RecordId(page_id, 2**63 - 1)  # Maximum tuple number

        assert record_id.get_tuple_number() == 2**63 - 1
        assert record_id.get_page_id() == page_id

    def test_edge_case_zero_values(self):
        """Test with minimum valid values."""
        page_id = HeapPageId(0, 0)
        record_id = RecordId(page_id, 0)

        assert record_id.get_tuple_number() == 0
        assert record_id.get_page_id() == page_id

    def test_page_id_immutability(self):
        """Test that the page_id reference is maintained."""
        page_id = HeapPageId(1, 0)
        record_id = RecordId(page_id, 5)

        # The returned page_id should be the same object
        assert record_id.get_page_id() is page_id

        # Modifying the original page_id affects the record_id (this is expected behavior)
        original_page_number = page_id.get_page_number()
        assert record_id.get_page_id().get_page_number() == original_page_number

    def test_multiple_record_ids_same_page(self):
        """Test multiple RecordIds pointing to the same page."""
        page_id = HeapPageId(1, 0)

        record_id1 = RecordId(page_id, 0)
        record_id2 = RecordId(page_id, 1)
        record_id3 = RecordId(page_id, 999)

        # All should have the same page but different tuple numbers
        assert record_id1.get_page_id() == record_id2.get_page_id()
        assert record_id2.get_page_id() == record_id3.get_page_id()

        assert record_id1.get_tuple_number() != record_id2.get_tuple_number()
        assert record_id2.get_tuple_number() != record_id3.get_tuple_number()

        # They should all be different
        assert record_id1 != record_id2
        assert record_id2 != record_id3
        assert record_id1 != record_id3
