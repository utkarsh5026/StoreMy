import pytest
from app.storage.page import HeapPageId


class TestHeapPageId:
    """Comprehensive tests for HeapPageId implementation."""

    def test_init_valid_values(self):
        """Test initialization with valid values."""
        page_id = HeapPageId(1, 0)
        assert page_id.get_table_id() == 1
        assert page_id.get_page_number() == 0

        page_id2 = HeapPageId(999, 12345)
        assert page_id2.get_table_id() == 999
        assert page_id2.get_page_number() == 12345

    def test_init_zero_values(self):
        """Test initialization with zero values (valid boundary case)."""
        page_id = HeapPageId(0, 0)
        assert page_id.get_table_id() == 0
        assert page_id.get_page_number() == 0

    def test_init_large_values(self):
        """Test initialization with large values."""
        max_val = 2**31 - 1  # Maximum positive int32
        page_id = HeapPageId(max_val, max_val)
        assert page_id.get_table_id() == max_val
        assert page_id.get_page_number() == max_val

    def test_init_negative_table_id_raises_error(self):
        """Test that negative table_id raises ValueError."""
        with pytest.raises(ValueError, match="Table ID must be non-negative, got -1"):
            HeapPageId(-1, 0)

        with pytest.raises(ValueError, match="Table ID must be non-negative, got -100"):
            HeapPageId(-100, 5)

        with pytest.raises(ValueError, match="Table ID must be non-negative, got -2147483648"):
            HeapPageId(-2147483648, 0)

    def test_init_negative_page_number_raises_error(self):
        """Test that negative page_number raises ValueError."""
        with pytest.raises(ValueError, match="Page number must be non-negative, got -1"):
            HeapPageId(0, -1)

        with pytest.raises(ValueError, match="Page number must be non-negative, got -500"):
            HeapPageId(5, -500)

        with pytest.raises(ValueError, match="Page number must be non-negative, got -2147483648"):
            HeapPageId(1, -2147483648)

    def test_init_both_negative_raises_error(self):
        """Test that both negative values raise appropriate error."""
        # Should raise error for table_id first
        with pytest.raises(ValueError, match="Table ID must be non-negative, got -1"):
            HeapPageId(-1, -1)

    def test_get_table_id(self):
        """Test get_table_id method."""
        page_id = HeapPageId(42, 100)
        assert page_id.get_table_id() == 42

        page_id2 = HeapPageId(0, 999)
        assert page_id2.get_table_id() == 0

    def test_get_page_number(self):
        """Test get_page_number method."""
        page_id = HeapPageId(42, 100)
        assert page_id.get_page_number() == 100

        page_id2 = HeapPageId(999, 0)
        assert page_id2.get_page_number() == 0

    def test_serialize(self):
        """Test serialization to list of integers."""
        page_id = HeapPageId(42, 100)
        serialized = page_id.serialize()

        assert isinstance(serialized, list)
        assert len(serialized) == 2
        assert serialized == [42, 100]

    def test_serialize_zero_values(self):
        """Test serialization with zero values."""
        page_id = HeapPageId(0, 0)
        serialized = page_id.serialize()
        assert serialized == [0, 0]

    def test_serialize_large_values(self):
        """Test serialization with large values."""
        page_id = HeapPageId(999999, 888888)
        serialized = page_id.serialize()
        assert serialized == [999999, 888888]

    def test_equality_same_values(self):
        """Test equality with same values."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(1, 2)

        assert page_id1 == page_id2
        assert page_id2 == page_id1

    def test_equality_same_object(self):
        """Test equality with same object."""
        page_id = HeapPageId(1, 2)
        assert page_id == page_id

    def test_equality_different_table_id(self):
        """Test inequality with different table IDs."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(2, 2)

        assert page_id1 != page_id2
        assert page_id2 != page_id1

    def test_equality_different_page_number(self):
        """Test inequality with different page numbers."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(1, 3)

        assert page_id1 != page_id2
        assert page_id2 != page_id1

    def test_equality_both_different(self):
        """Test inequality with both table_id and page_number different."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(3, 4)

        assert page_id1 != page_id2
        assert page_id2 != page_id1

    def test_equality_different_types(self):
        """Test inequality with different types."""
        page_id = HeapPageId(1, 2)

        assert page_id != "not a page id"
        assert page_id != 42
        assert page_id != None
        assert page_id != [1, 2]
        assert page_id != (1, 2)

    def test_hash_consistency(self):
        """Test that equal objects have equal hashes."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(1, 2)

        assert page_id1 == page_id2
        assert hash(page_id1) == hash(page_id2)

    def test_hash_different_objects(self):
        """Test that different objects have different hashes."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(2, 2)
        page_id3 = HeapPageId(1, 3)
        page_id4 = HeapPageId(2, 3)

        hashes = {hash(page_id1), hash(page_id2),
                  hash(page_id3), hash(page_id4)}
        # All hashes should be different (high probability)
        assert len(hashes) == 4

    def test_hash_zero_values(self):
        """Test hash with zero values."""
        page_id = HeapPageId(0, 0)
        hash_value = hash(page_id)
        assert isinstance(hash_value, int)

    def test_hash_usable_in_collections(self):
        """Test that HeapPageId can be used in sets and dicts."""
        page_id1 = HeapPageId(1, 2)
        page_id2 = HeapPageId(1, 2)  # Equal to page_id1
        page_id3 = HeapPageId(2, 3)

        # Test in set
        page_set = {page_id1, page_id2, page_id3}
        assert len(page_set) == 2  # page_id1 and page_id2 are equal

        # Test in dict
        page_dict = {page_id1: "first", page_id3: "third"}
        assert len(page_dict) == 2
        assert page_dict[page_id2] == "first"  # page_id2 equals page_id1

    def test_str_representation(self):
        """Test string representation."""
        page_id = HeapPageId(42, 100)
        str_repr = str(page_id)

        assert "HeapPageId" in str_repr
        assert "table=42" in str_repr
        assert "page=100" in str_repr

    def test_str_representation_zero_values(self):
        """Test string representation with zero values."""
        page_id = HeapPageId(0, 0)
        str_repr = str(page_id)

        assert "HeapPageId" in str_repr
        assert "table=0" in str_repr
        assert "page=0" in str_repr

    def test_repr_representation(self):
        """Test repr representation."""
        page_id = HeapPageId(42, 100)
        assert repr(page_id) == str(page_id)

    def test_edge_case_single_table_multiple_pages(self):
        """Test multiple pages for the same table."""
        table_id = 5
        page_ids = [HeapPageId(table_id, i) for i in range(100)]

        # All should have same table_id but different page_numbers
        for i, page_id in enumerate(page_ids):
            assert page_id.get_table_id() == table_id
            assert page_id.get_page_number() == i

        # All should be different from each other
        for i, page_id1 in enumerate(page_ids):
            for j, page_id2 in enumerate(page_ids):
                if i != j:
                    assert page_id1 != page_id2

    def test_edge_case_multiple_tables_same_page_number(self):
        """Test same page number across different tables."""
        page_number = 10
        page_ids = [HeapPageId(i, page_number) for i in range(100)]

        # All should have same page_number but different table_ids
        for i, page_id in enumerate(page_ids):
            assert page_id.get_table_id() == i
            assert page_id.get_page_number() == page_number

        # All should be different from each other
        for i, page_id1 in enumerate(page_ids):
            for j, page_id2 in enumerate(page_ids):
                if i != j:
                    assert page_id1 != page_id2

    def test_immutability(self):
        """Test that HeapPageId is immutable after creation."""
        page_id = HeapPageId(42, 100)

        # Should not have any setter methods
        assert not hasattr(page_id, 'set_table_id')
        assert not hasattr(page_id, 'set_page_number')

        # Values should remain constant
        original_table_id = page_id.get_table_id()
        original_page_number = page_id.get_page_number()

        # After multiple calls, values should remain the same
        for _ in range(10):
            assert page_id.get_table_id() == original_table_id
            assert page_id.get_page_number() == original_page_number

    def test_serialize_immutability(self):
        """Test that serialization returns a new list each time."""
        page_id = HeapPageId(42, 100)

        serialized1 = page_id.serialize()
        serialized2 = page_id.serialize()

        # Should be equal but not the same object
        assert serialized1 == serialized2
        assert serialized1 is not serialized2

        # Modifying one shouldn't affect the other
        serialized1[0] = 999
        assert serialized2 == [42, 100]

    def test_boundary_values(self):
        """Test with boundary values."""
        # Test with 0 (minimum valid values)
        page_id_min = HeapPageId(0, 0)
        assert page_id_min.get_table_id() == 0
        assert page_id_min.get_page_number() == 0

        # Test with large values
        large_val = 2**20  # 1 million
        page_id_large = HeapPageId(large_val, large_val)
        assert page_id_large.get_table_id() == large_val
        assert page_id_large.get_page_number() == large_val
