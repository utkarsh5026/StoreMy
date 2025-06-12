import pytest
from app.query.iterator.tuple_iterator import TupleIterator
from app.core.tuple import Tuple, TupleDesc
from app.core.types import FieldType
from app.core.types.fields import IntField, StringField, BoolField, FloatField, DoubleField


class TestTupleIterator:
    """Comprehensive tests for TupleIterator implementation."""

    def setup_method(self):
        """Set up common test data."""
        self.int_str_desc = TupleDesc([FieldType.INT, FieldType.STRING])
        self.single_int_desc = TupleDesc([FieldType.INT])
        self.all_types_desc = TupleDesc([
            FieldType.INT, FieldType.STRING, FieldType.BOOLEAN,
            FieldType.FLOAT, FieldType.DOUBLE
        ])

    def _create_simple_tuple(self, value: int, text: str = None) -> Tuple:
        """Helper to create a simple tuple for testing."""
        tuple_obj = Tuple(self.int_str_desc)
        tuple_obj.set_field(0, IntField(value))
        tuple_obj.set_field(1, StringField(text or f"text_{value}"))
        return tuple_obj

    def _create_single_int_tuple(self, value: int) -> Tuple:
        """Helper to create a single int tuple for testing."""
        tuple_obj = Tuple(self.single_int_desc)
        tuple_obj.set_field(0, IntField(value))
        return tuple_obj

    def _create_all_types_tuple(self, i: int) -> Tuple:
        """Helper to create tuple with all field types."""
        tuple_obj = Tuple(self.all_types_desc)
        tuple_obj.set_field(0, IntField(i))
        tuple_obj.set_field(1, StringField(f"str_{i}"))
        tuple_obj.set_field(2, BoolField(i % 2 == 0))
        tuple_obj.set_field(3, FloatField(float(i) + 0.5))
        tuple_obj.set_field(4, DoubleField(float(i) + 0.25))
        return tuple_obj

    # Test constructor edge cases
    def test_init_empty_tuple_list(self):
        """Test initialization with an empty tuple list."""
        iterator = TupleIterator(self.int_str_desc, [])

        assert iterator.get_tuple_desc() == self.int_str_desc
        assert iterator.tuples == []

    def test_init_single_tuple(self):
        """Test initialization with single tuple."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])

        assert iterator.get_tuple_desc() == self.int_str_desc
        assert len(iterator.tuples) == 1
        assert iterator.tuples[0] == tuple_obj

    def test_init_multiple_tuples(self):
        """Test initialization with multiple tuples."""
        tuples = [
            self._create_simple_tuple(1),
            self._create_simple_tuple(2),
            self._create_simple_tuple(3)
        ]
        iterator = TupleIterator(self.int_str_desc, tuples)

        assert len(iterator.tuples) == 3
        for i, original in enumerate(tuples):
            assert iterator.tuples[i] == original

    def test_init_incompatible_tuple_schema_raises_error(self):
        """Test that tuples with incompatible schema raise ValueError."""
        wrong_tuple = self._create_single_int_tuple(42)  # Wrong schema

        with pytest.raises(ValueError, match="Tuple 0 has incompatible schema"):
            TupleIterator(self.int_str_desc, [wrong_tuple])

    def test_init_mixed_compatible_incompatible_schemas(self):
        """Test with mix of compatible and incompatible tuples."""
        good_tuple = self._create_simple_tuple(1)
        bad_tuple = self._create_single_int_tuple(2)

        with pytest.raises(ValueError, match="Tuple 1 has incompatible schema"):
            TupleIterator(self.int_str_desc, [good_tuple, bad_tuple])

    def test_init_all_field_types(self):
        """Test initialization with tuples containing all field types."""
        tuples = [self._create_all_types_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.all_types_desc, tuples)

        assert len(iterator.tuples) == 3

    def test_defensive_copy_behavior(self):
        """Test that modifying original list doesn't affect iterator."""
        original_tuples = [self._create_simple_tuple(
            1), self._create_simple_tuple(2)]
        iterator = TupleIterator(self.int_str_desc, original_tuples)

        # Modify an original list
        original_tuples.append(self._create_simple_tuple(3))
        original_tuples.pop(0)

        # Iterator should still have original contents
        assert len(iterator.tuples) == 2
        assert iterator.tuples[0].get_field(0).value == 1
        assert iterator.tuples[1].get_field(0).value == 2

    # Test iterator lifecycle
    def test_iterator_not_opened_has_next_raises_error(self):
        """Test that calling has_next() before open() raises RuntimeError."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        with pytest.raises(RuntimeError, match="Iterator not open"):
            iterator.has_next()

    def test_iterator_not_opened_next_raises_error(self):
        """Test that calling next() before open() raises RuntimeError."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        with pytest.raises(RuntimeError, match="Iterator not open"):
            iterator.next()

    def test_iterator_not_opened_rewind_works(self):
        """Test that rewind() can be called before open()."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        # This should not raise an error
        iterator.rewind()

    def test_open_close_lifecycle(self):
        """Test proper open/close lifecycle."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        # Initially not open
        assert not iterator._is_open

        # Open
        iterator.open()
        assert iterator._is_open
        assert iterator.iterator is not None

        # Close
        iterator.close()
        assert not iterator._is_open
        assert iterator.iterator is None

    def test_double_open(self):
        """Test that calling open() twice works correctly."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        iterator.open()
        first_iterator = iterator.iterator

        iterator.open()  # Should work without error
        assert iterator._is_open
        assert iterator.iterator is not None

    def test_double_close(self):
        """Test that calling close() twice works correctly."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        iterator.open()
        iterator.close()
        iterator.close()  # Should work without error
        assert not iterator._is_open

    # Test iteration behavior
    def test_empty_iterator_has_next_false(self):
        """Test that empty iterator returns False for has_next()."""
        iterator = TupleIterator(self.int_str_desc, [])
        iterator.open()

        assert not iterator.has_next()

    def test_empty_iterator_next_raises_stop_iteration(self):
        """Test that empty iterator raises StopIteration for next()."""
        iterator = TupleIterator(self.int_str_desc, [])
        iterator.open()

        with pytest.raises(StopIteration, match="No more tuples"):
            iterator.next()

    def test_single_tuple_iteration(self):
        """Test iteration over single tuple."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])
        iterator.open()

        # Should have one tuple
        assert iterator.has_next()

        # Get the tuple
        retrieved = iterator.next()
        assert retrieved == tuple_obj

        # Should be exhausted now
        assert not iterator.has_next()

        # Next call should raise StopIteration
        with pytest.raises(StopIteration):
            iterator.next()

    def test_multiple_tuple_iteration(self):
        """Test iteration over multiple tuples."""
        tuples = [self._create_simple_tuple(i) for i in range(5)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        retrieved_tuples = []
        while iterator.has_next():
            retrieved_tuples.append(iterator.next())

        assert len(retrieved_tuples) == 5
        for i, retrieved in enumerate(retrieved_tuples):
            assert retrieved == tuples[i]

    def test_has_next_multiple_calls(self):
        """Test that multiple has_next() calls don't advance iterator."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])
        iterator.open()

        # Multiple has_next() calls should all return True
        assert iterator.has_next()
        assert iterator.has_next()
        assert iterator.has_next()

        # Should still be able to get the tuple
        retrieved = iterator.next()
        assert retrieved == tuple_obj

        # Now should be exhausted
        assert not iterator.has_next()
        assert not iterator.has_next()

    def test_alternating_has_next_next_calls(self):
        """Test alternating has_next() and next() calls."""
        tuples = [self._create_simple_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        for i, expected_tuple in enumerate(tuples):
            assert iterator.has_next()
            retrieved = iterator.next()
            assert retrieved == expected_tuple

        assert not iterator.has_next()

    # Test rewind functionality
    def test_rewind_empty_iterator(self):
        """Test rewind on empty iterator."""
        iterator = TupleIterator(self.int_str_desc, [])
        iterator.open()

        iterator.rewind()
        assert not iterator.has_next()

    def test_rewind_single_tuple(self):
        """Test rewind with single tuple."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])
        iterator.open()

        # Consume the tuple
        assert iterator.has_next()
        retrieved1 = iterator.next()
        assert not iterator.has_next()

        # Rewind and consume again
        iterator.rewind()
        assert iterator.has_next()
        retrieved2 = iterator.next()
        assert not iterator.has_next()

        # Should be the same tuple both times
        assert retrieved1 == retrieved2 == tuple_obj

    def test_rewind_multiple_tuples(self):
        """Test rewind with multiple tuples."""
        tuples = [self._create_simple_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        # First iteration
        first_pass = []
        while iterator.has_next():
            first_pass.append(iterator.next())

        # Rewind and second iteration
        iterator.rewind()
        second_pass = []
        while iterator.has_next():
            second_pass.append(iterator.next())

        # Should be identical
        assert first_pass == second_pass == tuples

    def test_rewind_partial_consumption(self):
        """Test rewind after partial consumption."""
        tuples = [self._create_simple_tuple(i) for i in range(5)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        # Consume first two tuples
        first = iterator.next()
        second = iterator.next()
        assert first == tuples[0]
        assert second == tuples[1]

        # Rewind and start over
        iterator.rewind()
        restarted_first = iterator.next()
        restarted_second = iterator.next()

        assert restarted_first == tuples[0]
        assert restarted_second == tuples[1]

    def test_multiple_rewinds(self):
        """Test multiple consecutive rewinds."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])
        iterator.open()

        # Multiple rewinds should work
        iterator.rewind()
        iterator.rewind()
        iterator.rewind()

        # Should still be able to iterate
        assert iterator.has_next()
        retrieved = iterator.next()
        assert retrieved == tuple_obj

    # Test behavior after close
    def test_has_next_after_close_raises_error(self):
        """Test that has_next() after close() raises RuntimeError."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])
        iterator.open()
        iterator.close()

        with pytest.raises(RuntimeError, match="Iterator not open"):
            iterator.has_next()

    def test_next_after_close_raises_error(self):
        """Test that next() after close() raises RuntimeError."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])
        iterator.open()
        iterator.close()

        with pytest.raises(RuntimeError, match="Iterator not open"):
            iterator.next()

    def test_rewind_after_close_works(self):
        """Test that rewind() after close() works."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])
        iterator.open()
        iterator.close()

        # This should not raise an error
        iterator.rewind()

    # Test read_next method specifically
    def test_read_next_empty_iterator(self):
        """Test read_next on empty iterator."""
        iterator = TupleIterator(self.int_str_desc, [])
        iterator.open()

        result = iterator.read_next()
        assert result is None

    def test_read_next_without_open(self):
        """Test read_next when iterator is not open."""
        iterator = TupleIterator(
            self.int_str_desc, [self._create_simple_tuple(1)])

        result = iterator.read_next()
        assert result is None

    def test_read_next_single_tuple(self):
        """Test read_next with single tuple."""
        tuple_obj = self._create_simple_tuple(42)
        iterator = TupleIterator(self.int_str_desc, [tuple_obj])
        iterator.open()

        # First call should return the tuple
        result1 = iterator.read_next()
        assert result1 == tuple_obj

        # Second call should return None
        result2 = iterator.read_next()
        assert result2 is None

    def test_read_next_multiple_tuples(self):
        """Test read_next with multiple tuples."""
        tuples = [self._create_simple_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        # Should return tuples in order
        for expected_tuple in tuples:
            result = iterator.read_next()
            assert result == expected_tuple

        # Should return None after exhaustion
        result = iterator.read_next()
        assert result is None

    # Test get_tuple_desc
    def test_get_tuple_desc_returns_original(self):
        """Test that get_tuple_desc returns the original TupleDesc."""
        iterator = TupleIterator(self.int_str_desc, [])

        returned_desc = iterator.get_tuple_desc()
        assert returned_desc == self.int_str_desc
        assert returned_desc is self.int_str_desc  # Should be same object

    def test_get_tuple_desc_all_field_types(self):
        """Test get_tuple_desc with all field types."""
        iterator = TupleIterator(self.all_types_desc, [])

        returned_desc = iterator.get_tuple_desc()
        assert returned_desc == self.all_types_desc
        assert returned_desc.num_fields() == 5

    # Test edge cases and error conditions
    def test_large_number_of_tuples(self):
        """Test with large number of tuples."""
        num_tuples = 1000
        tuples = [self._create_simple_tuple(i) for i in range(num_tuples)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        count = 0
        while iterator.has_next():
            tuple_obj = iterator.next()
            assert tuple_obj.get_field(0).value == count
            count += 1

        assert count == num_tuples

    def test_tuple_with_all_field_types_iteration(self):
        """Test iteration over tuples with all field types."""
        tuples = [self._create_all_types_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.all_types_desc, tuples)
        iterator.open()

        for i, expected_tuple in enumerate(tuples):
            assert iterator.has_next()
            retrieved = iterator.next()

            # Verify all fields
            assert retrieved.get_field(0).value == i
            assert retrieved.get_field(1).value == f"str_{i}"
            assert retrieved.get_field(2).value == (i % 2 == 0)
            assert retrieved.get_field(
                3).value == pytest.approx(float(i) + 0.5)
            assert retrieved.get_field(
                4).value == pytest.approx(float(i) + 0.25)

    def test_inheritance_from_abstract_iterator(self):
        """Test that TupleIterator properly inherits from AbstractDbIterator."""
        from app.query.iterator.abstract_iterator import AbstractDbIterator

        iterator = TupleIterator(self.int_str_desc, [])
        assert isinstance(iterator, AbstractDbIterator)

        # Should have _is_open and _next_tuple attributes from parent
        assert hasattr(iterator, '_is_open')
        assert hasattr(iterator, '_next_tuple')

    def test_integration_with_abstract_iterator_methods(self):
        """Test integration with AbstractDbIterator's has_next and next methods."""
        tuples = [self._create_simple_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.int_str_desc, tuples)
        iterator.open()

        # These methods come from AbstractDbIterator and should work correctly
        retrieved_tuples = []
        while iterator.has_next():
            retrieved_tuples.append(iterator.next())

        assert len(retrieved_tuples) == 3
        for i, retrieved in enumerate(retrieved_tuples):
            assert retrieved == tuples[i]

    def test_state_consistency_after_operations(self):
        """Test that iterator state remains consistent after various operations."""
        tuples = [self._create_simple_tuple(i) for i in range(3)]
        iterator = TupleIterator(self.int_str_desc, tuples)

        # Initial state
        assert not iterator._is_open
        assert iterator.iterator is None

        # After open
        iterator.open()
        assert iterator._is_open
        assert iterator.iterator is not None

        # After consuming one tuple
        iterator.next()
        assert iterator._is_open
        assert iterator.iterator is not None

        # After rewind
        iterator.rewind()
        assert iterator._is_open  # Should still be open
        assert iterator.iterator is not None

        # After close
        iterator.close()
        assert not iterator._is_open
        assert iterator.iterator is None
