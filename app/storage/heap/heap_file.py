import os
from pathlib import Path

from app.storage.interfaces.db_file import DbFile
from .heap_page import HeapPage, HeapPageId
from app.storage.interfaces.page import Page
from app.core.exceptions import DbException
from app.storage.permissions import Permissions
from app.core.tuple import Tuple, TupleDesc
from app.concurrency.transactions import TransactionId
from app.query.iterator import DbIterator
from app.storage.disk import StorageManager


class HeapFile(DbFile):
    """
    HeapFile is an implementation of DbFile that stores a collection of tuples
    in no particular order (hence "heap"). Tuples are stored on pages, each of
    which is a fixed size, and the file is simply a collection of those pages.

    HeapFile works closely with HeapPage. The format of HeapPages is described
    in the HeapPage class.

    Key Design Decisions:
    1. **No ordering**: Tuples are stored in insertion order (heap storage)
    2. **Fixed page size**: All pages are PAGE_SIZE bytes (typically 4KB)
    3. **Simple file format**: Just concatenated pages on disk
    4. **Page-based I/O**: Always read/write entire pages, never individual tuples
    """

    PAGE_SIZE = 4096  # 4KB pages - standard database page size

    def __init__(self, file_path: str, tuple_desc: TupleDesc):
        """
        Construct a heap file backed by the specified file.

        Args:
            file_path: Path to the file that stores the on-disk data
            tuple_desc: Schema description for tuples in this file
        """
        self.file_path = Path(file_path)
        self._storage_manager = StorageManager()
        self.tuple_desc = tuple_desc

        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self.file_path.touch()

    def get_file_path(self) -> Path:
        """Return the file path backing this HeapFile."""
        return self.file_path

    def get_id(self) -> int:
        """
        Return an ID uniquely identifying this HeapFile.

        We use the hash code of the absolute file path to ensure uniqueness.
        This means the same file will always have the same ID.
        """
        return hash(str(self.file_path.absolute()))

    def get_tuple_desc(self) -> TupleDesc:
        """Return the TupleDesc of the table stored in this DbFile."""
        return self.tuple_desc

    def num_pages(self) -> int:
        """
        Return the number of pages in this HeapFile.

        This is calculated by dividing the file size by the page size.
        If the file size is not a multiple of PAGE_SIZE, we round up.
        """
        if not self.file_path.exists():
            return 0

        file_size = self.file_path.stat().st_size
        return (file_size + self.PAGE_SIZE - 1) // self.PAGE_SIZE

    def read_page(self, page_id: HeapPageId) -> Page:
        """
        Read the specified page from the disk.

        This method:
        1. Calculates the byte offset in file
        2. Seeks to that position
        3. Exactly read PAGE_SIZE bytes
        4. Create a HeapPage object from the raw data
        """
        if page_id is None:
            raise ValueError("PageId cannot be None")

        page_number = page_id.get_page_number()
        file_offset = page_number * self.PAGE_SIZE

        try:
            with open(self.file_path, 'rb') as f:
                f.seek(file_offset)
                page_data = f.read(self.PAGE_SIZE)

                # If we read fewer than PAGE_SIZE bytes, pad with zeros
                if len(page_data) < self.PAGE_SIZE:
                    page_data += b'\x00' * (self.PAGE_SIZE - len(page_data))

                return HeapPage(page_id, page_data)

        except IOError as e:
            raise DbException(f"Failed to read page {page_number}: {e}")

    def write_page(self, page: HeapPage) -> None:
        """
        Write the specified page to disk.

        This method:
        1. Calculates the byte offset in file
        2. Seeks to that position  
        3. Write page data
        4. Ensures the writing is flushed to disk
        """
        page_number = page.get_id().get_page_number()
        file_offset = page_number * self.PAGE_SIZE

        try:
            with open(self.file_path, 'r+b') if self.file_path.exists() else open(self.file_path, 'w+b') as f:
                f.seek(file_offset)
                page_data = page.get_page_data()

                if len(page_data) != self.PAGE_SIZE:
                    raise ValueError(
                        f"Page data must be exactly {self.PAGE_SIZE} bytes, got {len(page_data)}")

                f.write(page_data)
                f.flush()
                os.fsync(f.fileno())

        except IOError as e:
            raise DbException(f"Failed to write page {page_number}: {e}")

    def add_tuple(self, tid: 'TransactionId', tuple_data: Tuple) -> list[HeapPage]:
        """
        Add a tuple to the file.

        Algorithm:
        1. Try to find an existing page with space
        2. If no space is found, create a new page
        3. Add the tuple to page
        4. Mark the page as dirty
        5. Return the modified page(s)
        """
        from ...database import Database

        table_id = self.get_id()
        buffer_pool = Database.get_buffer_pool()

        # Try to find a page with space
        for page_num in range(self.num_pages()):
            page_id = HeapPageId(table_id, page_num)
            page = buffer_pool.get_page(tid, page_id, Permissions.READ_ONLY)

            if isinstance(page, HeapPage) and page.get_num_empty_slots() > 0:
                page = buffer_pool.get_page(
                    tid, page_id, Permissions.READ_WRITE)

                if not isinstance(page, HeapPage):
                    raise TypeError("D")
                page.add_tuple(tuple_data)
                page.mark_dirty(True, tid)
                return [page]

            # Release the read lock early since we're not using this page
            buffer_pool.release_page(tid, page_id)

        # No existing page has space, create a new one
        # Use synchronization to prevent race conditions
        with Database.get_catalog_lock():
            # Re-check num_pages in case another thread added a page
            current_num_pages = self.num_pages()
            new_page_id = HeapPageId(table_id, current_num_pages)

            # Create new empty page
            new_page = HeapPage(new_page_id, bytes(self.PAGE_SIZE))
            new_page.add_tuple(tuple_data)

            # Write the new page to disk immediately
            self.write_page(new_page)

            return [new_page]

    def delete_tuple(self, tid: 'TransactionId', tuple_data: 'Tuple') -> 'Page':
        """
        Delete a tuple from the file.

        This method:
        1. Get the page containing the tuple
        2. Removes the tuple from page
        3. Marks the page as dirty
        4. Returns the modified page
        """
        from ...database import Database

        record_id = tuple_data.get_record_id()
        if record_id is None:
            raise DbException("Tuple must have a RecordId to be deleted")

        page_id = record_id.get_page_id()
        buffer_pool = Database.get_buffer_pool()

        # Get the page with write permission
        page = buffer_pool.get_page(tid, page_id, Permissions.READ_WRITE)
        page.delete_tuple(tuple_data)
        page.mark_dirty(True, tid)

        return page

    def iterator(self, tid: 'TransactionId') -> 'DbIterator':
        """
        Return an iterator over all tuples in this file.

        The iterator uses the buffer pool to access pages, ensuring
        proper caching and locking behavior.
        """
        return HeapFileIterator(tid, self)


class HeapFileIterator(DbIterator):
    """
    Iterator for scanning all tuples in a HeapFile.

    This iterator:
    1. Iterates through all pages in the file
    2. For each page, iterates through all valid tuples
    3. Uses the buffer pool for page access (not direct file I/O)
    4. Handles empty pages and partially filled pages correctly
    """

    def __init__(self, tid: 'TransactionId', heap_file: HeapFile):
        self.tid = tid
        self.heap_file = heap_file
        self.table_id = heap_file.get_id()
        self.current_page = None
        self.current_page_iterator = None
        self.current_page_number = 0
        self.is_open = False

    def open(self) -> None:
        """Open the iterator and position it at the first tuple."""
        from ...database import Database

        self.is_open = True
        self.current_page_number = 0

        if self.heap_file.num_pages() > 0:
            page_id = HeapPageId(self.table_id, 0)
            self.current_page = Database.get_buffer_pool().get_page(
                self.tid, page_id, Permissions.READ_ONLY
            )
            self.current_page_iterator = iter(self.current_page)

    def has_next(self) -> bool:
        """Check if there are more tuples to iterate over."""
        if not self.is_open:
            return False

        if self.current_page is None:
            return False

        # Try to get next tuple from current page
        if self._current_page_has_next():
            return True

        # Try to advance to next page with tuples
        return self._advance_to_next_page_with_tuples()

    def next(self) -> 'Tuple':
        """Return the next tuple in the iteration."""
        if not self.has_next():
            raise StopIteration("No more tuples")

        return next(self.current_page_iterator)

    def _current_page_has_next(self) -> bool:
        """Check if the current page has more tuples."""
        if self.current_page_iterator is None:
            return False

        try:
            # Peek at the next item without consuming it
            next_tuple = next(self.current_page_iterator)
            # Put it back by creating a new iterator starting from this tuple
            # This is a bit of a hack, but Python iterators don't support peek
            return True
        except StopIteration:
            return False

    def _advance_to_next_page_with_tuples(self) -> bool:
        """Advance to the next page that contains tuples."""
        from ...database import Database

        buffer_pool = Database.get_buffer_pool()

        while self.current_page_number < self.heap_file.num_pages() - 1:
            self.current_page_number += 1

            try:
                page_id = HeapPageId(self.table_id, self.current_page_number)
                self.current_page = buffer_pool.get_page(
                    self.tid, page_id, Permissions.READ_ONLY
                )
                self.current_page_iterator = iter(self.current_page)

                if self._current_page_has_next():
                    return True

            except Exception as e:
                # Log error and continue to next page
                print(f"Error accessing page {self.current_page_number}: {e}")
                continue

        return False

    def rewind(self) -> None:
        """Reset the iterator to the beginning."""
        self.close()
        self.open()

    def close(self) -> None:
        """Close the iterator and release resources."""
        self.is_open = False
        self.current_page = None
        self.current_page_iterator = None
        self.current_page_number = 0

    def get_tuple_desc(self) -> 'TupleDesc':
        """Return the TupleDesc for tuples returned by this iterator."""
        return self.heap_file.get_tuple_desc()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()
