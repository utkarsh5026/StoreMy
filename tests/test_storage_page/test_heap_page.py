import pytest
import math
from app.storage.page.heap_page import HeapPage
from app.storage.page.heap_page_id import HeapPageId
from app.core.tuple import Tuple, TupleDesc
from app.core.types import FieldType
from app.core.types.fields import IntField, StringField, BoolField
from app.primitives import TransactionId, RecordId


class MockTuple:
    """Mock tuple for testing when we don't need to full tuple functionality."""

    def __init__(self, tuple_desc, record_id=None, complete=True):
        self.tuple_desc = tuple_desc
        self.record_id = record_id
        self.complete = complete

    def get_tuple_desc(self):
        return self.tuple_desc

    def get_record_id(self):
        return self.record_id

    def set_record_id(self, record_id):
        self.record_id = record_id

    def is_complete(self):
        return self.complete

    def serialize(self):
        return b'\x00' * self.tuple_desc.get_size()


class TestHeapPage:
    """Comprehensive tests for HeapPage implementation."""

    def setup_method(self):
        """Set up common test data."""
        self.page_id = HeapPageId(1, 0)
        self.tuple_desc = TupleDesc([FieldType.INT, FieldType.STRING])
        self.transaction_id = TransactionId()

    def test_init_new_page_valid(self):
        """Test initialization of new page with valid tuple descriptor."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        assert page.get_id() == self.page_id
        assert page.tuple_desc == self.tuple_desc
        assert page.is_dirty() is None
        assert page.before_image_data is not None
        assert page.get_num_empty_slots() == page.num_slots

    def test_init_new_page_without_tuple_desc_raises_error(self):
        """Test that creating new page without tuple_desc raises ValueError."""
        with pytest.raises(ValueError, match="Must provide tuple_desc for new page"):
            HeapPage(self.page_id)

    def test_init_with_data_not_implemented(self):
        """Test that initialization with data raises NotImplementedError."""
        data = b'\x00' * HeapPage.PAGE_SIZE_IN_BYTES

        with pytest.raises(NotImplementedError, match="Page deserialization requires catalog integration"):
            HeapPage(self.page_id, data=data)

    def test_calculate_num_slots(self):
        """Test slot calculation with different tuple sizes."""
        # Small tuple: 4 bytes (int)
        small_td = TupleDesc([FieldType.INT])
        small_page = HeapPage(self.page_id, tuple_desc=small_td)
        expected_small = (HeapPage.PAGE_SIZE_IN_BYTES * 8) // (FieldType.INT.get_length() * 8 + 1)
        assert small_page.num_slots == expected_small

        # Large tuple: 4 + 132 = 136 bytes (int and string)
        large_td = TupleDesc([FieldType.INT, FieldType.STRING])
        large_page = HeapPage(self.page_id, tuple_desc=large_td)
        expected_large = (HeapPage.PAGE_SIZE_IN_BYTES * 8) // ((FieldType.STRING.get_length() + FieldType.INT.get_length()) * 8 + 1)
        assert large_page.num_slots == expected_large

        # Very large tuple that fits only a few times
        # 20 * 128 = 2560 bytes
        very_large_td = TupleDesc([FieldType.STRING] * 20)
        very_large_page = HeapPage(self.page_id, tuple_desc=very_large_td)
        expected_very_large = (
            HeapPage.PAGE_SIZE_IN_BYTES * 8) // (20 * FieldType.STRING.get_length() * 8 + 1)
        assert very_large_page.num_slots == expected_very_large

    def test_calculate_header_size(self):
        """Test header size calculation."""
        # Create pages with different numbers of slots
        td = TupleDesc([FieldType.INT])
        page = HeapPage(self.page_id, tuple_desc=td)

        expected_header_size = math.ceil(page.num_slots / 8.0)
        assert page.header_size == expected_header_size
        assert len(page.header) == expected_header_size

    def test_get_id(self):
        """Test get_id method."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        assert page.get_id() == self.page_id
        assert page.get_id() is self.page_id

    def test_is_dirty_initially_clean(self):
        """Test that new page is initially clean."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        assert page.is_dirty() is None

    def test_mark_dirty_true(self):
        """Test marking page as dirty."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        page.mark_dirty(True, self.transaction_id)
        assert page.is_dirty() == self.transaction_id

    def test_mark_dirty_false(self):
        """Test marking page as clean."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # First mark it dirty
        page.mark_dirty(True, self.transaction_id)
        assert page.is_dirty() == self.transaction_id

        # Then mark it clean
        page.mark_dirty(False, None)
        assert page.is_dirty() is None

    def test_mark_dirty_different_transactions(self):
        """Test marking page dirty with different transactions."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tx1 = TransactionId()
        tx2 = TransactionId()

        page.mark_dirty(True, tx1)
        assert page.is_dirty() == tx1

        page.mark_dirty(True, tx2)
        assert page.is_dirty() == tx2

    def test_is_slot_used_initially_false(self):
        """Test that slots are initially unused."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        for slot in range(page.num_slots):
            assert not page._is_slot_used(slot)

    def test_is_slot_used_out_of_range(self):
        """Test _is_slot_used with out of range indices."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        assert not page._is_slot_used(-1)
        assert not page._is_slot_used(page.num_slots)
        assert not page._is_slot_used(page.num_slots + 100)

    def test_set_slot_used_valid(self):
        """Test setting slot as used/unused."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Initially unused
        assert not page._is_slot_used(0)

        # Mark as used
        page._set_slot_used(0, True)
        assert page._is_slot_used(0)

        # Mark as unused
        page._set_slot_used(0, False)
        assert not page._is_slot_used(0)

    def test_set_slot_used_multiple_slots(self):
        """Test setting multiple slots as used."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Mark the first 5 slots as used
        for i in range(5):
            page._set_slot_used(i, True)
            assert page._is_slot_used(i)

        # Check that other slots are still unused
        for i in range(5, page.num_slots):
            assert not page._is_slot_used(i)

    def test_set_slot_used_out_of_range_raises_error(self):
        """Test that setting slot used with invalid index raises IndexError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        with pytest.raises(IndexError, match=f"Slot -1 out of range"):
            page._set_slot_used(-1, True)

        with pytest.raises(IndexError, match=f"Slot {page.num_slots} out of range"):
            page._set_slot_used(page.num_slots, True)

    def test_get_num_empty_slots_initially_all_empty(self):
        """Test that initially all slots are empty."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        assert page.get_num_empty_slots() == page.num_slots

    def test_get_num_empty_slots_after_marking_used(self):
        """Test empty slot count after marking some as used."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Mark 3 slots as used
        for i in range(3):
            page._set_slot_used(i, True)

        assert page.get_num_empty_slots() == page.num_slots - 3

    def test_add_tuple_valid(self):
        """Test adding a valid tuple."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create a complete tuple
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))

        initial_empty = page.get_num_empty_slots()
        page.add_tuple(tuple_obj)

        # Should have one less empty slot
        assert page.get_num_empty_slots() == initial_empty - 1

        # Tuple should have a record ID
        record_id = tuple_obj.get_record_id()
        assert record_id is not None
        assert record_id.get_page_id() == self.page_id
        assert 0 <= record_id.get_tuple_number() < page.num_slots

        # Slot should be marked as used
        assert page._is_slot_used(record_id.get_tuple_number())

        # Tuple should be in the page's tuple list
        assert page.tuples[record_id.get_tuple_number()] == tuple_obj

    def test_add_tuple_wrong_schema_raises_error(self):
        """Test that adding tuple with wrong schema raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with different schema
        wrong_td = TupleDesc([FieldType.BOOLEAN])
        tuple_obj = Tuple(wrong_td)
        tuple_obj.set_field(0, BoolField(True))

        with pytest.raises(ValueError, match="Tuple schema doesn't match page schema"):
            page.add_tuple(tuple_obj)

    def test_add_tuple_incomplete_raises_error(self):
        """Test that adding incomplete tuple raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create incomplete tuple
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        # Don't set field 1

        with pytest.raises(ValueError, match="Cannot add incomplete tuple to page"):
            page.add_tuple(tuple_obj)

    def test_add_tuple_page_full_raises_error(self):
        """Test that adding tuple to full page raises RuntimeError."""
        # Create a page with very large tuples so it has very few slots
        large_td = TupleDesc([FieldType.STRING] * 30)  # Very large tuple
        page = HeapPage(self.page_id, tuple_desc=large_td)

        # Fill all slots with mock tuples
        for i in range(page.num_slots):
            page._set_slot_used(i, True)
            page.tuples[i] = MockTuple(large_td)

        # Try to add one more tuple
        tuple_obj = MockTuple(large_td)

        with pytest.raises(RuntimeError, match="No empty slots available on page"):
            page.add_tuple(tuple_obj)

    def test_add_multiple_tuples(self):
        """Test adding multiple tuples."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuples = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test{i}"))

            page.add_tuple(tuple_obj)
            tuples.append(tuple_obj)

        # Check that all tuples have valid record IDs
        used_slots = set()
        for tuple_obj in tuples:
            record_id = tuple_obj.get_record_id()
            assert record_id is not None
            assert record_id.get_page_id() == self.page_id

            slot = record_id.get_tuple_number()
            assert slot not in used_slots  # No duplicates
            used_slots.add(slot)

            assert page._is_slot_used(slot)
            assert page.tuples[slot] == tuple_obj

    def test_delete_tuple_valid(self):
        """Test deleting a valid tuple."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add a tuple first
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        page.add_tuple(tuple_obj)

        initial_empty = page.get_num_empty_slots()
        slot_number = tuple_obj.get_record_id().get_tuple_number()

        # Delete the tuple
        page.delete_tuple(tuple_obj)

        # Should have one more empty slot
        assert page.get_num_empty_slots() == initial_empty + 1

        # Tuple should no longer have a record ID
        assert tuple_obj.get_record_id() is None

        # Slot should be marked as unused
        assert not page._is_slot_used(slot_number)

        # Tuple should be removed from page's tuple list
        assert page.tuples[slot_number] is None

    def test_delete_tuple_no_record_id_raises_error(self):
        """Test that deleting tuple without record ID raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))

        with pytest.raises(ValueError, match="Tuple has no RecordId - cannot delete"):
            page.delete_tuple(tuple_obj)

    def test_delete_tuple_wrong_page_raises_error(self):
        """Test that deleting tuple from wrong page raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with record ID pointing to different page
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))

        wrong_page_id = HeapPageId(2, 0)
        wrong_record_id = RecordId(wrong_page_id, 0)
        tuple_obj.set_record_id(wrong_record_id)

        with pytest.raises(ValueError, match="Tuple RecordId points to different page"):
            page.delete_tuple(tuple_obj)

    def test_delete_tuple_empty_slot_raises_error(self):
        """Test that deleting tuple from empty slot raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with record ID pointing to empty slot
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))

        record_id = RecordId(self.page_id, 0)
        tuple_obj.set_record_id(record_id)

        with pytest.raises(ValueError, match="Slot 0 is already empty"):
            page.delete_tuple(tuple_obj)

    def test_iterator_empty_page(self):
        """Test iterator on empty page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuples = list(page.iterator())
        assert len(tuples) == 0

    def test_iterator_with_tuples(self):
        """Test iterator with tuples on page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add some tuples
        added_tuples = []
        for i in range(min(3, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test{i}"))

            page.add_tuple(tuple_obj)
            added_tuples.append(tuple_obj)

        # Get tuples from iterator
        iterated_tuples = list(page.iterator())

        # Should get the same tuples (order might be different)
        assert len(iterated_tuples) == len(added_tuples)
        for tuple_obj in added_tuples:
            assert tuple_obj in iterated_tuples

    def test_iterator_with_gaps(self):
        """Test iterator when there are gaps in used slots."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add tuples, then delete some to create gaps
        tuples = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test{i}"))

            page.add_tuple(tuple_obj)
            tuples.append(tuple_obj)

        # Delete every other tuple
        for i in range(0, len(tuples), 2):
            page.delete_tuple(tuples[i])

        # Iterator should only return non-deleted tuples
        iterated_tuples = list(page.iterator())
        expected_tuples = [tuples[i] for i in range(1, len(tuples), 2)]

        assert len(iterated_tuples) == len(expected_tuples)
        for tuple_obj in expected_tuples:
            assert tuple_obj in iterated_tuples

    def test_get_page_data_empty_page(self):
        """Test serialization of empty page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        data = page.get_page_data()

        assert isinstance(data, bytes)
        assert len(data) == HeapPage.PAGE_SIZE_IN_BYTES

    def test_get_page_data_with_tuples(self):
        """Test serialization of page with tuples."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add a tuple
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        page.add_tuple(tuple_obj)

        data = page.get_page_data()

        assert isinstance(data, bytes)
        assert len(data) == HeapPage.PAGE_SIZE_IN_BYTES

    def test_create_empty_page_data(self):
        """Test creating empty page data."""
        data = HeapPage.create_empty_page_data()

        assert isinstance(data, bytes)
        assert len(data) == HeapPage.PAGE_SIZE_IN_BYTES
        assert data == b'\x00' * HeapPage.PAGE_SIZE_IN_BYTES

    def test_str_representation(self):
        """Test string representation."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add some tuples
        for i in range(min(3, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test{i}"))
            page.add_tuple(tuple_obj)

        str_repr = str(page)

        assert "HeapPage" in str_repr
        assert str(self.page_id) in str_repr
        assert "3/" in str_repr  # 3 used slots
        assert str(page.num_slots) in str_repr

    # def test_before_image_operations(self):
    #     """Test before image handling."""
    #     page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
    #
    #     # Before image should be set automatically
    #     assert page.before_image_data is not None
    #
    #     # Get before image
    #     before_image = page.get_before_image()
    #     assert isinstance(before_image, HeapPage)
    #
    #     # Modify page
    #     tuple_obj = Tuple(self.tuple_desc)
    #     tuple_obj.set_field(0, IntField(42))
    #     tuple_obj.set_field(1, StringField("test"))
    #     page.add_tuple(tuple_obj)
    #
    #     # Before image should still reflect original state
    #     before_image = page.get_before_image()
    #     # This will fail because get_before_image tries to deserialize, which is not implemented
    #     # In a real test, we'd need to mock or implement deserialization

    def test_set_before_image(self):
        """Test setting before image."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Modify page
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        page.add_tuple(tuple_obj)

        # Set new before image
        page.set_before_image()

        # Before image data should be updated
        assert page.before_image_data is not None

    def test_page_size_constraint(self):
        """Test that page data doesn't exceed page size."""
        # Create page with maximum possible tuples
        small_td = TupleDesc([FieldType.INT])  # 4 byte tuples
        page = HeapPage(self.page_id, tuple_desc=small_td)

        # Fill the page completely
        for i in range(page.num_slots):
            tuple_obj = Tuple(small_td)
            tuple_obj.set_field(0, IntField(i))
            page.add_tuple(tuple_obj)

        # Get page data - should not exceed page size
        data = page.get_page_data()
        assert len(data) == HeapPage.PAGE_SIZE_IN_BYTES

    def test_edge_case_single_slot_page(self):
        """Test page that can only hold a single tuple."""
        # Create a very large tuple descriptor that allows only one tuple per page
        large_fields = [FieldType.STRING] * 25  # 25 * 128 = 3200 bytes
        large_td = TupleDesc(large_fields)
        page = HeapPage(self.page_id, tuple_desc=large_td)

        # Should have at least 1 slot, but probably not many
        assert page.num_slots >= 1

        # Should be able to add at least one tuple
        mock_tuple = MockTuple(large_td)
        page.add_tuple(mock_tuple)

        assert page.get_num_empty_slots() == page.num_slots - 1

    def test_edge_case_bit_vector_boundary(self):
        """Test header bit vector at byte boundaries."""
        # Create page where the number of slots is exactly a multiple of 8
        # This tests boundary conditions in the bit vector implementation
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Test setting slots at byte boundaries
        if page.num_slots >= 8:
            page._set_slot_used(7, True)  # Last bit of first byte
            page._set_slot_used(8, True)  # First bit of second byte

            assert page._is_slot_used(7)
            assert page._is_slot_used(8)
            assert not page._is_slot_used(6)
            assert not page._is_slot_used(9)

    def test_edge_case_maximum_slots(self):
        """Test with the maximum number of slots possible."""
        # Use the smallest possible tuple (just an int)
        min_td = TupleDesc([FieldType.INT])
        page = HeapPage(self.page_id, tuple_desc=min_td)

        # Should have calculated maximum slots correctly
        expected_slots = (HeapPage.PAGE_SIZE_IN_BYTES * 8) // (4 * 8 + 1)
        assert page.num_slots == expected_slots

        # Should be able to add tuples up to the limit
        added_count = 0
        try:
            for i in range(page.num_slots):
                tuple_obj = Tuple(min_td)
                tuple_obj.set_field(0, IntField(i))
                page.add_tuple(tuple_obj)
                added_count += 1
        except RuntimeError:
            # If we get "no empty slots" error, we should have added exactly num_slots
            assert added_count == page.num_slots

        # Should now be full
        assert page.get_num_empty_slots() == 0

    def test_tuple_record_id_assignment(self):
        """Test that tuples get correct record IDs when added."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuples = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test{i}"))

            # Should have no record ID before adding
            assert tuple_obj.get_record_id() is None

            page.add_tuple(tuple_obj)
            tuples.append(tuple_obj)

            # Should have record ID after adding
            record_id = tuple_obj.get_record_id()
            assert record_id is not None
            assert record_id.get_page_id() == self.page_id
            assert 0 <= record_id.get_tuple_number() < page.num_slots

    def test_add_delete_add_reuses_slots(self):
        """Test that deleting and adding reuses slots."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add a tuple
        tuple1 = Tuple(self.tuple_desc)
        tuple1.set_field(0, IntField(1))
        tuple1.set_field(1, StringField("first"))
        page.add_tuple(tuple1)

        slot1 = tuple1.get_record_id().get_tuple_number()

        # Delete the tuple
        page.delete_tuple(tuple1)

        # Add another tuple - should reuse the same slot
        tuple2 = Tuple(self.tuple_desc)
        tuple2.set_field(0, IntField(2))
        tuple2.set_field(1, StringField("second"))
        page.add_tuple(tuple2)

        slot2 = tuple2.get_record_id().get_tuple_number()

        # Should reuse the same slot (first available slot algorithm)
        assert slot2 == slot1
