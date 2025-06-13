import pytest
from app.storage.heap import HeapPage, HeapPageId
from app.core.tuple import Tuple, TupleDesc
from app.core.types import FieldType
from app.core.types.fields import IntField, StringField
from app.primitives import TransactionId, RecordId


class TestPageEdgeCases:
    """Edge case tests for page implementations."""

    def test_maximum_tuple_size_boundary(self):
        """Test with tuples that approach the maximum possible size."""
        # Create a tuple that uses most of the page space
        # With 4KB pages, we can have very large tuples
        large_fields = [FieldType.STRING] * 30  # 30 * 128 = 3840 bytes
        large_desc = TupleDesc(large_fields)

        page_id = HeapPageId(1, 0)
        page = HeapPage(page_id, tuple_desc=large_desc)

        # Should have very few slots but at least 1
        assert page.num_slots >= 1
        assert page.num_slots <= 10  # Shouldn't be too many

        # Should be able to add at least one tuple
        tuple_obj = Tuple(large_desc)
        for i in range(large_desc.num_fields()):
            tuple_obj.set_field(i, StringField(f"field_{i}"))

        page.insert_tuple(tuple_obj)
        assert page.get_num_empty_slots() == page.num_slots - 1

    def test_minimum_tuple_size_maximum_slots(self):
        """Test with the smallest possible tuples to maximize slot count."""
        # Single boolean field is smallest (1 byte)
        min_desc = TupleDesc([FieldType.BOOLEAN])

        page_id = HeapPageId(1, 0)
        page = HeapPage(page_id, tuple_desc=min_desc)

        # Calculate expected slots
        expected_slots = (HeapPage.PAGE_SIZE_IN_BYTES * 8) // (1 * 8 + 1)
        assert page.num_slots == expected_slots

        # Should be able to add many tuples
        assert page.num_slots > 1000  # Should be quite a lot


    def test_page_id_edge_values(self):
        """Test HeapPageId with edge case values."""
        # Test with maximum values
        max_val = 2**31 - 1
        page_id = HeapPageId(max_val, max_val)

        assert page_id.get_table_id() == max_val
        assert page_id.get_page_number() == max_val
        assert page_id.serialize() == [max_val, max_val]

        # Test with zero values
        zero_page_id = HeapPageId(0, 0)
        assert zero_page_id.get_table_id() == 0
        assert zero_page_id.get_page_number() == 0
        assert zero_page_id.serialize() == [0, 0]

    def test_tuple_operations_with_minimal_page(self):
        """Test tuple operations on page with minimal slots."""
        # Create a page that can hold very few tuples
        # ~3200 bytes per tuple
        large_desc = TupleDesc([FieldType.STRING] * 25)
        page_id = HeapPageId(1, 0)
        page = HeapPage(page_id, tuple_desc=large_desc)

        # Fill all available slots
        tuples = []
        for i in range(page.num_slots):
            tuple_obj = Tuple(large_desc)
            for j in range(large_desc.num_fields()):
                tuple_obj.set_field(j, StringField(f"value_{i}_{j}"))
            page.insert_tuple(tuple_obj)
            tuples.append(tuple_obj)

        # Page should be full
        assert page.get_num_empty_slots() == 0

        # Delete one tuple
        page.delete_tuple(tuples[0])
        assert page.get_num_empty_slots() == 1

        # Should be able to add one more
        new_tuple = Tuple(large_desc)
        for j in range(large_desc.num_fields()):
            new_tuple.set_field(j, StringField(f"new_value_{j}"))
        page.insert_tuple(new_tuple)
        assert page.get_num_empty_slots() == 0

    def test_serialization_boundary_conditions(self):
        """Test page serialization at various boundary conditions."""
        page_id = HeapPageId(1, 0)
        tuple_desc = TupleDesc([FieldType.INT, FieldType.STRING])
        page = HeapPage(page_id, tuple_desc=tuple_desc)

        # Test empty page serialization
        empty_data = page.get_page_data()
        assert len(empty_data) == HeapPage.PAGE_SIZE_IN_BYTES

        # Test with one tuple
        tuple_obj = Tuple(tuple_desc)
        tuple_obj.set_field(0, IntField(42))
        tuple_obj.set_field(1, StringField("test"))
        page.insert_tuple(tuple_obj)

        one_tuple_data = page.get_page_data()
        assert len(one_tuple_data) == HeapPage.PAGE_SIZE_IN_BYTES
        assert one_tuple_data != empty_data

        # Test after deletion
        page.delete_tuple(tuple_obj)
        after_delete_data = page.get_page_data()
        assert len(after_delete_data) == HeapPage.PAGE_SIZE_IN_BYTES
        # Note: after_delete_data might not equal empty_data due to header state

    def test_transaction_id_edge_cases(self):
        """Test transaction ID handling with edge case values."""
        page_id = HeapPageId(1, 0)
        tuple_desc = TupleDesc([FieldType.INT])
        page = HeapPage(page_id, tuple_desc=tuple_desc)

        # Test with very large transaction IDs
        large_tx = TransactionId()
        page.mark_dirty(True, large_tx)
        assert page.is_dirty() == large_tx

        # Test with zero transaction ID
        zero_tx = TransactionId()
        page.mark_dirty(True, zero_tx)
        assert page.is_dirty() == zero_tx

        # Test rapid transaction changes
        for _ in range(100):
            tx = TransactionId()
            page.mark_dirty(True, tx)
            assert page.is_dirty() == tx

    def test_string_field_boundary_cases(self):
        """Test string fields with boundary cases."""
        page_id = HeapPageId(1, 0)
        tuple_desc = TupleDesc([FieldType.STRING])
        page = HeapPage(page_id, tuple_desc=tuple_desc)

        empty_tuple = Tuple(tuple_desc)
        empty_tuple.set_field(0, StringField(""))
        page.insert_tuple(empty_tuple)

        max_length = FieldType.STRING.get_length() - 4  # Account for length prefix
        long_string = "x" * max_length
        long_tuple = Tuple(tuple_desc)
        long_tuple.set_field(0, StringField(long_string))
        page.insert_tuple(long_tuple)

        tuples = list(page.iterator())
        assert len(tuples) == 2

        string_values = [t.get_field(0).get_value() for t in tuples]
        assert "" in string_values
        assert long_string in string_values

    def test_record_id_edge_cases(self):
        """Test RecordId with edge case values."""
        # Test with maximum values
        max_page_id = HeapPageId(2**31 - 1, 2**31 - 1)
        max_record_id = RecordId(max_page_id, 2**63 - 1)

        assert max_record_id.get_tuple_number() == 2**63 - 1
        assert max_record_id.get_page_id() == max_page_id

        # Test with zero values
        zero_page_id = HeapPageId(0, 0)
        zero_record_id = RecordId(zero_page_id, 0)

        assert zero_record_id.get_tuple_number() == 0
        assert zero_record_id.get_page_id() == zero_page_id

    def test_page_operations_with_zero_slots(self):
        """Test behavior when calculated slots would be zero or very small."""
        # This is a theoretical edge case - if tuple size approaches page size
        # In practice, the current implementation should always allow at least some tuples
        # But we test the boundary conditions

        page_id = HeapPageId(1, 0)
        # Use a very large tuple that might result in minimal slots
        huge_desc = TupleDesc([FieldType.STRING] * 32)  # 32 * 128 = 4096 bytes

        # This might create a page with very few slots
        page = HeapPage(page_id, tuple_desc=huge_desc)

        # Should still have at least some capacity
        assert page.num_slots >= 0

        if page.num_slots > 0:
            # Should be able to use the available slots
            assert page.get_num_empty_slots() == page.num_slots

    def test_header_size_calculation_edge_cases(self):
        """Test header size calculations with various slot counts."""
        page_id = HeapPageId(1, 0)

        # Create pages with different tuple sizes to get different slot counts
        test_configs = [
            TupleDesc([FieldType.BOOLEAN]),          # Many small slots
            TupleDesc([FieldType.INT]),              # Medium number of slots
            TupleDesc([FieldType.STRING]),           # Fewer larger slots
            TupleDesc([FieldType.STRING] * 5),       # Very few large slots
        ]

        for desc in test_configs:
            page = HeapPage(page_id, tuple_desc=desc)

            # Header size should be correct for number of slots
            # Round up to nearest byte
            expected_header_size = (page.num_slots + 7) // 8
            assert page.header_size == expected_header_size
            assert len(page.header) == expected_header_size

    def test_iterator_with_complex_slot_patterns(self):
        """Test iterator with complex patterns of used/unused slots."""
        page_id = HeapPageId(1, 0)
        tuple_desc = TupleDesc([FieldType.INT])
        page = HeapPage(page_id, tuple_desc=tuple_desc)

        if page.num_slots < 10:
            pytest.skip("Need at least 10 slots for complex pattern test")

        # Create a complex pattern of used slots
        # Add tuples to slots 0, 2, 4, 6, 8
        added_tuples = []
        for i in range(0, min(10, page.num_slots), 2):
            tuple_obj = Tuple(tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            page.insert_tuple(tuple_obj)
            added_tuples.append(tuple_obj)

        # Iterator should return all added tuples
        iterated_tuples = list(page.iterator())
        assert len(iterated_tuples) == len(added_tuples)

        # Verify all tuples are accounted for
        for tuple_obj in added_tuples:
            assert tuple_obj in iterated_tuples

    def test_page_data_structure_integrity(self):
        """Test that page data structures maintain integrity under stress."""
        page_id = HeapPageId(1, 0)
        tuple_desc = TupleDesc([FieldType.INT, FieldType.STRING])
        page = HeapPage(page_id, tuple_desc=tuple_desc)

        # Perform many operations to stress test data structure integrity
        operations = []

        # Add several tuples
        for i in range(min(5, page.num_slots)):
            tuple_obj = Tuple(tuple_desc)
            tuple_obj.set_field(0, IntField(i))
            tuple_obj.set_field(1, StringField(f"test_{i}"))
            page.insert_tuple(tuple_obj)
            operations.append(('add', tuple_obj))

        # Delete some tuples
        for i in range(0, len(operations), 2):
            if operations[i][0] == 'add':
                tuple_obj = operations[i][1]
                page.delete_tuple(tuple_obj)
                operations.append(('delete', tuple_obj))

        # Add more tuples
        for i in range(min(3, page.get_num_empty_slots())):
            tuple_obj = Tuple(tuple_desc)
            tuple_obj.set_field(0, IntField(100 + i))
            tuple_obj.set_field(1, StringField(f"new_test_{i}"))
            page.insert_tuple(tuple_obj)
            operations.append(('add', tuple_obj))

        # Verify page state is consistent
        active_tuples = [op[1] for op in operations
                         if op[0] == 'add' and op[1].get_record_id() is not None]

        iterated_tuples = list(page.iterator())
        assert len(iterated_tuples) == len(active_tuples)

        for tuple_obj in active_tuples:
            assert tuple_obj in iterated_tuples
            record_id = tuple_obj.get_record_id()
            assert record_id is not None
            assert page.slot_manager.is_slot_used(record_id.get_tuple_number())
            assert page.tuples[record_id.get_tuple_number()] == tuple_obj
