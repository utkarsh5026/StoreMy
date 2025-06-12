from typing import List, Optional, Iterator
from .abstract_iterator import AbstractDbIterator
from ...core.tuple import Tuple, TupleDesc


class TupleIterator(AbstractDbIterator):
    """
    Implements a DbIterator by wrapping a list of tuples.

    This is useful for:
    1. **Testing**: Create test data sets easily
    2. **Materialized results**: Store intermediate results in memory
    3. **Small data sets**: When all tuples fit in memory
    4. **Subqueries**: Results from nested queries

    The iterator validates that all tuples conform to the provided schema.
    """

    def __init__(self, tuple_desc: TupleDesc, tuples: List[Tuple]):
        """
        Create an iterator over the specified tuples.

        Args:
            tuple_desc: Schema that all tuples must conform to
            tuples: List of tuples to iterate over

        Raises:
            ValueError: If any tuple doesn't match the schema
        """
        super().__init__()
        self.tuple_desc = tuple_desc
        self.tuples = tuples.copy()  # Defensive copy
        self.iterator: Optional[Iterator[Tuple]] = None

        for i, tuple_obj in enumerate(self.tuples):
            if not tuple_obj.get_tuple_desc().equals(tuple_desc):
                raise ValueError(f"Tuple {i} has incompatible schema")

    def open(self) -> None:
        """Initialize the Python iterator."""
        super().open()
        self.iterator = iter(self.tuples)

    def close(self) -> None:
        """Clean up the iterator."""
        self.iterator = None
        super().close()

    def rewind(self) -> None:
        """Reset to the beginning of the tuple list."""
        self.iterator = iter(self.tuples)

    def get_tuple_desc(self) -> TupleDesc:
        """Return the schema for tuples in this iterator."""
        return self.tuple_desc

    def read_next(self) -> Optional[Tuple]:
        """Return the next tuple from the list."""
        if not self.iterator:
            return None

        try:
            return next(self.iterator)
        except StopIteration:
            return None