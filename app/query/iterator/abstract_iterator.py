from typing import Optional
from .db_iterator import DbIterator
from ...core.tuple import Tuple
from abc import  abstractmethod


class AbstractDbIterator(DbIterator):
    """
    Helper base class for implementing DbIterators.

    This class handles the common logic for hasNext()/next() by implementing
    a "read-ahead" pattern. Subclasses only need to implement readNext().

    How it works:
    1. hasNext() calls readNext() if no tuple is buffered
    2. next() returns the buffered tuple and clears the buffer
    3. readNext() is where subclasses implement their specific logic

    This pattern simplifies operator implementation and ensures consistent
    behavior across all operators.
    """

    def __init__(self):
        self._next_tuple: Optional[Tuple] = None
        self._is_open = False

    def has_next(self) -> bool:
        """
        Check if there are more tuples available.

        Uses read-ahead pattern: if no tuple is buffered, try to read one.
        """
        if not self._is_open:
            raise RuntimeError("Iterator not open")

        if self._next_tuple is None:
            self._next_tuple = self.read_next()
        return self._next_tuple is not None

    def next(self) -> Tuple:
        """
        Return the next tuple and advance the iterator.

        Uses the buffered tuple from has_next() or read a new one.
        """
        if not self._is_open:
            raise RuntimeError("Iterator not open")

        if self._next_tuple is None:
            self._next_tuple = self.read_next()

        if self._next_tuple is None:
            raise StopIteration("No more tuples")

        result = self._next_tuple
        self._next_tuple = None  # Clear buffer
        return result

    def open(self) -> None:
        """Mark iterator as open. Subclasses should override and call super()."""
        self._is_open = True

    def close(self) -> None:
        """Mark iterator as closed and clear buffer. Subclasses should override and call super()."""
        self._is_open = False
        self._next_tuple = None

    @abstractmethod
    def read_next(self) -> Optional[Tuple]:
        """
        Read the next tuple from the data source.

        This is the main method subclasses need to implement.
        Should return None when no more tuples are available.

        Returns:
            The next tuple, or None if iteration is finished

        Raises:
            DbException: Database errors
            TransactionAbortedException: If transaction is aborted
        """
        pass
