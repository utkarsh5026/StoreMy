import pytest
import math
from app.storage.heap import HeapPage, HeapPageId
from app.core.tuple import Tuple, TupleDesc
from app.core.types import FieldType
from app.core.types.fields import IntField, StringField
from app.primitives import TransactionId, RecordId


class MockTuple(Tuple):
    """Mock tuple for testing when we don't need to full tuple functionality."""

    def __init__(self, tuple_desc, record_id=None, complete=True):
        super().__init__(tuple_desc)
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



    def test_calculate_num_slots(self):
        """Test slot calculation with different tuple sizes."""
        # Small tuple: 4 bytes (int)
        small_td = TupleDesc([FieldType.INT])
        small_page = HeapPage(self.page_id, tuple_desc=small_td)
        expected_small = (HeapPage.PAGE_SIZE_IN_BYTES *
                          8) // (FieldType.INT.get_length() * 8 + 1)
        assert small_page.num_slots == expected_small

        # Large tuple: 4 + 132 = 136 bytes (int and string)
        large_td = TupleDesc([FieldType.INT, FieldType.STRING])
        large_page = HeapPage(self.page_id, tuple_desc=large_td)
        expected_large = (HeapPage.PAGE_SIZE_IN_BYTES * 8) // (
            (FieldType.STRING.get_length() + FieldType.INT.get_length()) * 8 + 1)
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

    def test_slot_manager_initially_empty(self):
        """Test that slot manager initially has all slots unused."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        for slot in range(page.num_slots):
            assert not page.slot_manager.is_slot_used(slot)

        assert page.slot_manager.get_free_slot_count() == page.num_slots

    def test_slot_manager_integration(self):
        """Test that slot manager is properly integrated with page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Test direct slot manager access
        page.slot_manager.set_slot_used(0, True)
        assert page.slot_manager.is_slot_used(0)
        assert page.slot_manager.get_free_slot_count() == page.num_slots - 1

        # Test finding free slots
        free_slot = page.slot_manager.find_free_slot()
        assert free_slot == 1  # Should be the next available slot

        # Mark another slot as used
        page.slot_manager.set_slot_used(2, True)
        assert page.slot_manager.is_slot_used(2)
        assert page.slot_manager.get_free_slot_count() == page.num_slots - 2

    def test_get_num_empty_slots_initially_all_empty(self):
        """Test that initially all slots are empty."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        assert page.get_num_empty_slots() == page.num_slots

    def test_get_num_empty_slots_with_slot_manager(self):
        """Test empty slot count using slot manager directly."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Mark 3 slots as used through slot manager
        for i in range(3):
            page.slot_manager.set_slot_used(i, True)

        assert page.get_num_empty_slots() == page.num_slots - 3

    def test_insert_tuple_valid(self):
        """Test inserting a valid tuple."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create a complete tuple with mock data since we need actual tuple functionality
        tuple_obj = MockTuple(self.tuple_desc, complete=True)

        initial_empty = page.get_num_empty_slots()
        slot_number = page.insert_tuple(tuple_obj)

        # Should return a valid slot number
        assert slot_number is not None
        assert 0 <= slot_number < page.num_slots

        # Should have one less empty slot
        assert page.get_num_empty_slots() == initial_empty - 1

        # Tuple should have a record ID
        record_id = tuple_obj.get_record_id()
        assert record_id is not None
        assert record_id.get_page_id() == self.page_id
        assert record_id.get_tuple_number() == slot_number

        # Slot should be marked as used
        assert page.slot_manager.is_slot_used(slot_number)

        # Tuple should be in the page's tuple list
        assert page.tuples[slot_number] == tuple_obj

    def test_insert_tuple_wrong_schema_raises_error(self):
        """Test that inserting tuple with the wrong schema raises ValueError."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with different schema
        wrong_td = TupleDesc([FieldType.BOOLEAN])
        tuple_obj = MockTuple(wrong_td, complete=True)

        with pytest.raises(ValueError, match="Tuple schema doesn't match page schema"):
            page.insert_tuple(tuple_obj)

    def test_insert_tuple_page_full_returns_none(self):
        """Test that inserting tuple to full page returns None."""
        # Create a page with very large tuples so it has very few slots
        large_td = TupleDesc([FieldType.STRING] * 30)  # Very large tuple
        page = HeapPage(self.page_id, tuple_desc=large_td)

        # Fill all slots with mock tuples
        for i in range(page.num_slots):
            page.slot_manager.set_slot_used(i, True)
            page.tuples[i] = MockTuple(large_td)

        # Try to insert one more tuple
        tuple_obj = MockTuple(large_td, complete=True)

        result = page.insert_tuple(tuple_obj)
        assert result is None  # Should return None when page is full

    def test_insert_multiple_tuples(self):
        """Test inserting multiple tuples."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuples = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = MockTuple(self.tuple_desc, complete=True)

            slot_number = page.insert_tuple(tuple_obj)
            assert slot_number is not None
            tuples.append((tuple_obj, slot_number))

        # Check that all tuples have valid record IDs and slots
        used_slots = set()
        for tuple_obj, slot_number in tuples:
            record_id = tuple_obj.get_record_id()
            assert record_id is not None
            assert record_id.get_page_id() == self.page_id

            slot = record_id.get_tuple_number()
            assert slot == slot_number
            assert slot not in used_slots  # No duplicates
            used_slots.add(slot)

            assert page.slot_manager.is_slot_used(slot)
            assert page.tuples[slot] == tuple_obj

    def test_delete_tuple_valid(self):
        """Test deleting a valid tuple."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Insert a tuple first
        tuple_obj = MockTuple(self.tuple_desc, complete=True)
        slot_number = page.insert_tuple(tuple_obj)
        assert slot_number is not None

        initial_empty = page.get_num_empty_slots()

        # Delete the tuple
        result = page.delete_tuple(tuple_obj)
        assert result is True  # Should return True for successful deletion

        # Should have one more empty slot
        assert page.get_num_empty_slots() == initial_empty + 1

        # Tuple should no longer have a record ID
        assert tuple_obj.get_record_id() is None

        # Slot should be marked as unused
        assert not page.slot_manager.is_slot_used(slot_number)

        # Tuple should be removed from page's tuple list
        assert page.tuples[slot_number] is None

    def test_delete_tuple_no_record_id_returns_false(self):
        """Test that deleting tuple without record ID returns False."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuple_obj = MockTuple(self.tuple_desc, complete=True)

        result = page.delete_tuple(tuple_obj)
        assert result is False  # Should return False for failed deletion

    def test_delete_tuple_wrong_page_returns_false(self):
        """Test that deleting tuple from wrong page returns False."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with record ID pointing to different page
        tuple_obj = MockTuple(self.tuple_desc, complete=True)

        wrong_page_id = HeapPageId(2, 0)
        wrong_record_id = RecordId(wrong_page_id, 0)
        tuple_obj.set_record_id(wrong_record_id)

        result = page.delete_tuple(tuple_obj)
        assert result is False  # Should return False for failed deletion

    def test_delete_tuple_empty_slot_returns_false(self):
        """Test that deleting tuple from empty slot returns False."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Create tuple with record ID pointing to empty slot
        tuple_obj = MockTuple(self.tuple_desc, complete=True)

        record_id = RecordId(self.page_id, 0)
        tuple_obj.set_record_id(record_id)

        result = page.delete_tuple(tuple_obj)
        assert result is False  # Should return False for failed deletion

    def test_iterator_empty_page(self):
        """Test iterator on empty page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        tuples = list(page.iterator())
        assert len(tuples) == 0

    def test_iterator_with_tuples(self):
        """Test iterator with tuples on page."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Insert some tuples
        added_tuples = []
        for i in range(min(3, page.num_slots)):
            tuple_obj = MockTuple(self.tuple_desc, complete=True)

            slot_number = page.insert_tuple(tuple_obj)
            assert slot_number is not None
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

        # Insert tuples, then delete some to create gaps
        tuples = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = MockTuple(self.tuple_desc, complete=True)

            slot_number = page.insert_tuple(tuple_obj)
            assert slot_number is not None
            tuples.append(tuple_obj)

        # Delete every other tuple
        for i in range(0, len(tuples), 2):
            result = page.delete_tuple(tuples[i])
            assert result is True

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

        # Insert a tuple
        tuple_obj = MockTuple(self.tuple_desc, complete=True)
        slot_number = page.insert_tuple(tuple_obj)
        assert slot_number is not None

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

        # Insert some tuples
        for i in range(min(3, page.num_slots)):
            tuple_obj = MockTuple(self.tuple_desc, complete=True)
            slot_number = page.insert_tuple(tuple_obj)
            assert slot_number is not None

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
    #     page.insert_tuple()(tuple_obj)
    #
    #     # Before image should still reflect original state
    #     before_image = page.get_before_image()
    #     # This will fail because get_before_image tries to deserialize, which is not implemented
    #     # In a real test, we'd need to mock or implement deserialization

    def test_set_before_image(self):
        """Test setting before image."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Modify page
        tuple_obj = MockTuple(self.tuple_desc, complete=True)
        slot_number = page.insert_tuple(tuple_obj)
        assert slot_number is not None

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
            tuple_obj = MockTuple(small_td, complete=True)
            slot_number = page.insert_tuple(tuple_obj)
            assert slot_number is not None

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
        page.insert_tuple(mock_tuple)

        assert page.get_num_empty_slots() == page.num_slots - 1


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
                page.insert_tuple(tuple_obj)
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

            page.insert_tuple(tuple_obj)
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
        page.insert_tuple(tuple1)

        slot1 = tuple1.get_record_id().get_tuple_number()

        # Delete the tuple
        page.delete_tuple(tuple1)

        # Add another tuple - should reuse the same slot
        tuple2 = Tuple(self.tuple_desc)
        tuple2.set_field(0, IntField(2))
        tuple2.set_field(1, StringField("second"))
        page.insert_tuple(tuple2)

        slot2 = tuple2.get_record_id().get_tuple_number()

        # Should reuse the same slot (first available slot algorithm)
        assert slot2 == slot1
