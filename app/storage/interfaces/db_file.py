from abc import ABC, abstractmethod

from app.core.tuple import Tuple, TupleDesc
from .page import Page, PageId
from app.query.iterator import DbIterator
from app.concurrency.transactions import TransactionId


class DbFile(ABC):
    """
    Abstract interface for database files on disk. 💾

    Each table is represented by a single DbFile. DbFiles can fetch pages
    and iterate through tuples. Each file has a unique id used to store 
    metadata about the table in the Catalog. 📊

    Key Concepts:
    - A DbFile represents one table stored as pages on disk 📝
    - Pages are the unit of I/O (fixed size, typically 4KB) 📄
    - DbFiles are accessed through the buffer pool for caching 🔄
    """

    @abstractmethod
    def read_page(self, page_id: PageId) -> Page:
        """
        Read the specified page from the disk. 📖

        Args:
            page_id: The ID of the page to read

        Returns:
            The page data loaded from disk

        Raises:
            ValueError: If the page does not exist in this file
        """
        pass

    @abstractmethod
    def write_page(self, page: 'Page') -> None:
        """
        Write the specified page to disk. ✍️

        Args:
            page: The page to write. page.get_id().page_number() 
                  specifies the offset in the file where the page should be written.

        Raises:
            IOError: If the writing fails
        """
        pass

    @abstractmethod
    def add_tuple(self, tid: TransactionId, tuple_data: Tuple) -> list[Page]:
        """
        Add the specified tuple to the file on behalf of the transaction. ➕

        This method will acquire a lock on the affected pages and may block
        until the lock can be acquired. 🔒

        Args:
            tid: The transaction performing the update
            tuple_data: The tuple to add. This tuple should be updated to reflect
                       that it is now stored in this file.

        Returns:
            List of pages that were modified

        Raises:
            DbException: If the tuple cannot be added
            IOError: If the necessary file can't be read/written
        """
        pass

    @abstractmethod
    def delete_tuple(self, tid: 'TransactionId', tuple_data: 'Tuple') -> Page:
        """
        Remove the specified tuple from the file on behalf of the transaction. ➖

        This method will acquire a lock on the affected pages and may block
        until the lock can be acquired. 🔒

        Args:
            tid: The transaction performing to delete
            tuple_data: The tuple to delete

        Returns:
            The page that was modified

        Raises:
            DbException: If the tuple cannot be deleted or is not a member of the file
        """
        pass

    @abstractmethod
    def iterator(self, tid: 'TransactionId') -> 'DbIterator':
        """
        Return an iterator over all tuples stored in this DbFile. 🔄

        The iterator must use BufferPool.get_page() rather than read_page()
        to iterate through pages (for proper caching and locking).

        Args:
            tid: The transaction id for this iteration

        Returns:
            An iterator over all tuples in this file
        """
        pass

    @abstractmethod
    def get_id(self) -> int:
        """
        Return a unique ID used to identify this DbFile in the Catalog. 🔑

        This id can be used to look up the table via Catalog.get_db_file()
        and Catalog.get_tuple_desc().

        Implementation note: Use the hash code of the absolute path of the
        underlying file, i.e., file.absolute().hash()

        Returns:
            An ID uniquely identifying this DbFile
        """
        pass

    @abstractmethod
    def get_tuple_desc(self) -> 'TupleDesc':
        """
        Return the TupleDesc of the table stored in this DbFile. 📋

        Returns:
            TupleDesc of this DbFile
        """
        pass
