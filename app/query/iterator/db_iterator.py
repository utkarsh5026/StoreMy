from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...core.tuple import Tuple, TupleDesc


class DbIterator(ABC):
    """
    DbIterator is the iterator interface that all SimpleDB operators should implement.

    This is the foundation of the query processing system.
    Every operator
    (scan, filter, join, etc.) implements this interface, allowing them to be
    composed into query execution trees.

    Key Design Principles:
    1. **Pull-based execution**: Operators pull tuples from their children
    2. **Iterator pattern**: hasNext() + next() for tuple streaming
    3. **Resource management**: open() + close() for setup/cleanup
    4. **Composability**: Operators can be chained together
    5. **Lazy evaluation**: Tuples are produced on-demand
    """

    @abstractmethod
    def open(self) -> None:
        """
        Opens the iterator.
        This must be called before any other methods.

        This method should:
        1. Initialize any internal state
        2. Open child iterators (if any)
        3. Acquire the necessary resources
        4. Position iterator at the first tuple

        Raises:
            DbException: When there are problems opening/accessing the database
            TransactionAbortedException: If transaction is aborted
        """
        pass

    @abstractmethod
    def has_next(self) -> bool:
        """
        Returns true if the iterator has more tuples.

        This method should NOT advance the iterator position.
        It may need to look ahead to determine if more tuples exist.

        Returns:
            True if more tuples are available

        Raises:
            IllegalStateException: If the iterator has not been opened
            DbException: Database errors
            TransactionAbortedException: If transaction is aborted
        """
        pass

    @abstractmethod
    def next(self) -> 'Tuple':
        """
        Returns the next tuple from the operator.

        This method advances the iterator position and returns the
        current tuple.
        Should only be called if it has_next() returns True.

        Returns:
            The next tuple in the iteration

        Raises:
            StopIteration: If there are no more tuples
            IllegalStateException: If the iterator has not been opened
            DbException: Database errors
            TransactionAbortedException: If transaction is aborted
        """
        pass

    @abstractmethod
    def rewind(self) -> None:
        """
        Resets the iterator to the start.

        After calling rewind(), the iterator should behave as if
        it was just opened - next call to next() should return
        the first tuple again.

        Raises:
            DbException: When rewind is unsupported or fails
            IllegalStateException: If the iterator has not been opened
        """
        pass

    @abstractmethod
    def get_tuple_desc(self) -> 'TupleDesc':
        """
        Returns the TupleDesc associated with this DbIterator.

        This describes the schema of tuples produced by this operator.
        Must remain consistent throughout the iterator's lifetime.

        Returns:
            The tuple descriptor for this iterator's output
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the iterator and releases resources.

        After calling close(), calling next(), has_next(), or rewind()
        should raise IllegalStateException.

        This method should:
        1. Close child iterators (if any)
        2. Release any held resources
        3. Mark iterator as closed
        """
        pass