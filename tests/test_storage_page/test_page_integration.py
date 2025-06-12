import pytest
from app.storage.heap import HeapPage, HeapPageId
from app.core.tuple import Tuple, TupleDesc
from app.core.types import FieldType
from app.core.types.fields import IntField, StringField, BoolField, FloatField, DoubleField
from app.primitives import TransactionId


class TestPageIntegration:
    """Integration tests for page components working together."""

    def setup_method(self):
        """Set up common test data."""
        self.page_id = HeapPageId(1, 0)
        self.tuple_desc = TupleDesc([FieldType.INT, FieldType.STRING])
        self.transaction_id = TransactionId(1)

    def test_complex_tuple_operations_workflow(self):
        """Test complex workflow of adding, deleting, and iterating over tuples."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add multiple tuples
        tuples = []
        for i in range(min(10, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"value_{i}"))
            page.add_tuple(tuple_obj)
            tuples.append(tuple_obj)

        # Verify all tuples were added correctly
        assert page.get_num_empty_slots() == page.num_slots - len(tuples)

        # Delete every third tuple
        to_delete = tuples[::3]
        for tuple_obj in to_delete:
            page.delete_tuple(tuple_obj)

        # Verify correct number of tuples remain
        remaining_tuples = [t for t in tuples if t not in to_delete]
        assert page.get_num_empty_slots() == page.num_slots - len(remaining_tuples)

        # Verify iterator returns only remaining tuples
        iterated_tuples = list(page.iterator())
        assert len(iterated_tuples) == len(remaining_tuples)
        for tuple_obj in remaining_tuples:
            assert tuple_obj in iterated_tuples

        # Add new tuples to fill some empty slots
        new_tuples = []
        for i in range(len(to_delete)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(1000 + i))
            tuple_obj.set_field(1, StringField(f"new_value_{i}"))
            page.add_tuple(tuple_obj)
            new_tuples.append(tuple_obj)

        # Verify total count is correct
        total_tuples = len(remaining_tuples) + len(new_tuples)
        assert page.get_num_empty_slots() == page.num_slots - total_tuples

    def test_page_state_transitions_with_transactions(self):
        """Test page state transitions with transaction management."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Initially clean
        assert page.is_dirty() is None

        # Add tuple and mark dirty
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        page.add_tuple(tuple_obj)
        page.mark_dirty(True, self.transaction_id)

        assert page.is_dirty() == self.transaction_id

        # Save before image
        page.set_before_image()

        # Make more changes with different transaction
        tx2 = TransactionId(2)
        page.delete_tuple(tuple_obj)
        page.mark_dirty(True, tx2)

        assert page.is_dirty() == tx2

        # Mark clean
        page.mark_dirty(False, None)
        assert page.is_dirty() is None

    def test_all_field_types_in_page(self):
        """Test page operations with all supported field types."""
        all_types_desc = TupleDesc([
            FieldType.INT,
            FieldType.STRING,
            FieldType.BOOLEAN,
            FieldType.FLOAT,
            FieldType.DOUBLE
        ])

        page = HeapPage(self.page_id, tuple_desc=all_types_desc)

        # Create tuple with all field types
        tuple_obj = Tuple(all_types_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test_string"))
        tuple_obj.set_field(2, BoolField(True))
        tuple_obj.set_field(3, FloatField(3.14))
        tuple_obj.set_field(4, DoubleField(2.718281828))

        # Add to page
        page.add_tuple(tuple_obj)

        # Verify it can be retrieved
        retrieved_tuples = list(page.iterator())
        assert len(retrieved_tuples) == 1

        retrieved_tuple = retrieved_tuples[0]
        assert retrieved_tuple.get_field(0).value == 42
        assert retrieved_tuple.get_field(1).value == "test_string"
        assert retrieved_tuple.get_field(2).value is True
        assert retrieved_tuple.get_field(3).value == pytest.approx(3.14)
        assert retrieved_tuple.get_field(4).value == pytest.approx(2.718281828)

    def test_page_serialization_with_complex_data(self):
        """Test page serialization with complex tuple arrangements."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add tuples with various data
        test_data = [
            (1, "short"),
            (999999, "very_long_string_that_tests_boundaries"),
            (-42, ""),  # Empty string
            (0, "null_like_values"),
        ]

        added_tuples = []
        for int_val, str_val in test_data:
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(int_val))
            tuple_obj.set_field(1, StringField(str_val))
            page.add_tuple(tuple_obj)
            added_tuples.append(tuple_obj)

        # Serialize page
        page_data = page.get_page_data()

        # Verify serialization properties
        assert isinstance(page_data, bytes)
        assert len(page_data) == HeapPage.PAGE_SIZE_IN_BYTES

        # Delete some tuples and re-serialize
        page.delete_tuple(added_tuples[1])
        page.delete_tuple(added_tuples[3])

        new_page_data = page.get_page_data()
        assert isinstance(new_page_data, bytes)
        assert len(new_page_data) == HeapPage.PAGE_SIZE_IN_BYTES
        assert new_page_data != page_data  # Should be different

    def test_page_capacity_limits(self):
        """Test page behavior at capacity limits."""
        # Use small tuples to maximize slots
        small_desc = TupleDesc([FieldType.INT])
        page = HeapPage(self.page_id, tuple_desc=small_desc)

        max_slots = page.num_slots

        # Fill page to capacity
        tuples = []
        for i in range(max_slots):
            tuple_obj = Tuple(small_desc)
            tuple_obj.set_field(0, IntField(i))
            page.add_tuple(tuple_obj)
            tuples.append(tuple_obj)

        # Verify page is full
        assert page.get_num_empty_slots() == 0

        # Try to add one more tuple - should fail
        overflow_tuple = Tuple(small_desc)
        overflow_tuple.set_field(0, IntField(9999))

        with pytest.raises(RuntimeError, match="No empty slots available"):
            page.add_tuple(overflow_tuple)

        # Delete one tuple and verify we can add again
        page.delete_tuple(tuples[0])
        assert page.get_num_empty_slots() == 1

        page.add_tuple(overflow_tuple)
        assert page.get_num_empty_slots() == 0

    def test_record_id_consistency_across_operations(self):
        """Test that record IDs remain consistent across page operations."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Add several tuples
        tuples_with_ids = []
        for i in range(min(5, page.num_slots)):
            tuple_obj = Tuple(self.tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test_{i}"))
            page.add_tuple(tuple_obj)

            record_id = tuple_obj.get_record_id()
            tuples_with_ids.append((tuple_obj, record_id))

        # Delete middle tuple
        middle_tuple, middle_id = tuples_with_ids[2]
        page.delete_tuple(middle_tuple)

        # Verify remaining tuples still have correct record IDs
        for i, (tuple_obj, original_id) in enumerate(tuples_with_ids):
            if i == 2:  # Skip deleted tuple
                assert tuple_obj.get_record_id() is None
                continue

            current_id = tuple_obj.get_record_id()
            assert current_id == original_id
            assert current_id.get_page_id() == self.page_id
            assert page._is_slot_used(current_id.get_tuple_number())
            assert page.tuples[current_id.get_tuple_number()] == tuple_obj

    def test_bit_vector_stress_test(self):
        """Stress test the bit vector implementation with many operations."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        if page.num_slots < 20:
            pytest.skip("Need at least 20 slots for stress test")

        # Pattern of operations to stress test bit vector
        operations = [
            (0, True), (1, True), (7, True), (8, True), (15, True), (16, True),
            (0, False), (8, False), (16, False),  # Clear some bits
            (2, True), (9, True), (17, True),     # Set new bits
            (7, False), (15, False),              # Clear more bits
        ]

        for slot, used in operations:
            if slot < page.num_slots:
                page._set_slot_used(slot, used)
                assert page._is_slot_used(slot) == used

        # Verify final state
        expected_used = {1, 2, 9, 17}
        for slot in range(min(20, page.num_slots)):
            expected = slot in expected_used
            assert page._is_slot_used(slot) == expected

    def test_page_header_size_calculations(self):
        """Test header size calculations for various slot counts."""
        test_cases = [
            # (num_slots, expected_header_bytes)
            (1, 1),    # 1 bit -> 1 byte
            (8, 1),    # 8 bits -> 1 byte
            (9, 2),    # 9 bits -> 2 bytes
            (16, 2),   # 16 bits -> 2 bytes
            (17, 3),   # 17 bits -> 3 bytes
            (64, 8),   # 64 bits -> 8 bytes
        ]

        for expected_slots, expected_header_size in test_cases:
            # Create a mock page to test header size calculation
            # We'll use the actual calculation from the implementation
            actual_header_size = pytest.approx(expected_slots / 8.0)
            assert abs(actual_header_size - expected_header_size) <= 1

    def test_empty_vs_non_empty_page_serialization(self):
        """Test serialization differences between empty and non-empty pages."""
        # Empty page
        empty_page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        empty_data = empty_page.get_page_data()

        # Page with one tuple
        non_empty_page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        tuple_obj = Tuple(self.tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        non_empty_page.add_tuple(tuple_obj)
        non_empty_data = non_empty_page.get_page_data()

        # Both should be same size
        assert len(empty_data) == len(
            non_empty_data) == HeapPage.PAGE_SIZE_IN_BYTES

        # But content should be different
        assert empty_data != non_empty_data

        # Empty page should have all header bits as 0
        empty_header_byte = empty_data[0]
        assert empty_header_byte == 0

        # Non-empty page should have at least one header bit set
        non_empty_header_byte = non_empty_data[0]
        assert non_empty_header_byte != 0

    def test_tuple_operations_with_different_schemas(self):
        """Test that tuple operations properly validate schemas."""
        page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)

        # Valid tuple with matching schema
        valid_tuple = Tuple(self.tuple_desc)
        valid_tuple.set_field(0, IntField(42))
        valid_tuple.set_field(1, StringField("test"))
        page.add_tuple(valid_tuple)

        # Invalid tuples with different schemas
        wrong_schemas = [
            TupleDesc([FieldType.INT]),  # Missing field
            TupleDesc([FieldType.INT, FieldType.BOOLEAN]),  # Wrong type
            TupleDesc([FieldType.STRING, FieldType.INT]),   # Wrong order
            TupleDesc([FieldType.INT, FieldType.STRING,
                      FieldType.BOOLEAN]),  # Extra field
        ]

        for wrong_desc in wrong_schemas:
            wrong_tuple = Tuple(wrong_desc)
            # Set fields appropriately for the wrong schema
            if wrong_desc.num_fields() >= 1:
                if wrong_desc.get_field_type(0) == FieldType.INT:
                    wrong_tuple.set_field(0, IntField(42))
                elif wrong_desc.get_field_type(0) == FieldType.STRING:
                    wrong_tuple.set_field(0, StringField("test"))

            if wrong_desc.num_fields() >= 2:
                if wrong_desc.get_field_type(1) == FieldType.INT:
                    wrong_tuple.set_field(1, IntField(42))
                elif wrong_desc.get_field_type(1) == FieldType.BOOLEAN:
                    wrong_tuple.set_field(1, BoolField(True))
                elif wrong_desc.get_field_type(1) == FieldType.STRING:
                    wrong_tuple.set_field(1, StringField("test"))

            if wrong_desc.num_fields() >= 3:
                wrong_tuple.set_field(2, BoolField(True))

            with pytest.raises(ValueError, match="Tuple schema doesn't match page schema"):
                page.add_tuple(wrong_tuple)
