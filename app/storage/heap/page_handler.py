from app.storage.interfaces import PageHandler, Page
from app.core.exceptions import DbException
from cachetools import LRUCache
from dataclasses import dataclass

from .heap_page import HeapPage
from app.storage.exceptions import (
    InvalidPageTypeError,
    InvalidPageSizeError,
    InvalidPageHeaderError,
    InvalidSlotError,
    TupleValidationError,
    PageCorruptionError,
    PageValidationError
)
from .heap_page_id import HeapPageId
from app.storage.exceptions import IOException


@dataclass(frozen=True)
class PageInfo:
    page_type: str
    page_id: str
    total_slots: int
    used_slots: int
    free_slots: int
    utilization: float
    header_size: int
    tuple_desc: str
    is_dirty: bool
    dirty_transaction: str


class HeapPageHandler(PageHandler):
    """Handler for heap pages containing table data."""

    def __init__(self):
        self.page_cache: LRUCache[HeapPageId, Page] = LRUCache(maxsize=1000)

    def load_page(self, page_id: HeapPageId) -> Page:
        """Load a heap page from disk."""
        from app.database import Database

        table_id = page_id.get_table_id()
        catalog = Database.get_catalog()
        db_file = catalog.get_db_file(table_id)

        if not db_file:
            raise DbException(f"Table {table_id} not found")

        try:
            page = db_file.read_page(page_id)
            if not self.validate_page(page):
                raise DbException(f"Invalid heap page structure: {page_id}")

            return page

        except Exception as e:
            raise IOException(f"Failed to load heap page {page_id}: {e}")

    def write_page(self, page: Page) -> None:
        """Write a heap page to disk."""
        from app.database import Database

        try:
            # Validate before writing
            if not self.validate_page(page):
                raise DbException(
                    f"Invalid heap page structure: {page.get_id()}")

            page_id = page.get_id()
            table_id = page_id.get_table_id()
            catalog = Database.get_catalog()
            db_file = catalog.get_db_file(table_id)

            if not db_file:
                raise DbException(f"Table {table_id} not found")

            db_file.write_page(page)

        except Exception as e:
            raise IOException(
                f"Failed to write heap page {page.get_id()}: {e}")

    def validate_page(self, page: Page) -> None:
        """Validate heap page structure."""

        try:
            if not isinstance(page, HeapPage):
                raise InvalidPageTypeError(
                    f"Expected HeapPage, got {type(page).__name__}"
                )

            if len(page.serialize()) != page.PAGE_SIZE_IN_BYTES:
                raise InvalidPageSizeError(
                    f"Page size {len(page.serialize())} doesn't match expected {page.PAGE_SIZE_IN_BYTES}"
                )

            if page.header_size <= 0 or page.header_size > page.PAGE_SIZE_IN_BYTES // 2:
                raise InvalidPageHeaderError(
                    f"Invalid header size: {page.header_size}"
                )

            if page.num_slots <= 0 or page.num_slots > page.PAGE_SIZE_IN_BYTES:
                raise InvalidSlotError(
                    f"Invalid number of slots: {page.num_slots}"
                )

            self._validate_tuples(page)

        except PageValidationError:
            raise
        except Exception as e:
            raise PageCorruptionError(
                f"Unexpected error during page validation: {e}"
            )

    def get_page_info(self, page: Page) -> PageInfo:
        """Get diagnostic information about a heap page."""
        from app.storage.heap.heap_page import HeapPage

        if not isinstance(page, HeapPage):
            raise InvalidPageTypeError(
                f"Expected HeapPage, got {type(page).__name__}"
            )

        used_slots = sum(1 for i in range(page.num_slots)
                         if page.slot_manager.is_slot_used(i))

        return PageInfo(
            page_type='heap',
            page_id=str(page.get_id()),
            total_slots=page.num_slots,
            used_slots=used_slots,
            free_slots=page.num_slots - used_slots,
            utilization=used_slots / page.num_slots if page.num_slots > 0 else 0,
            header_size=page.header_size,
            tuple_desc=str(page.tuple_desc),
            is_dirty=page.is_dirty() is not None,
            dirty_transaction=str(
                page.dirty_transaction) if page.dirty_transaction else None
        )

    @classmethod
    def _validate_tuples(cls, h_page: HeapPage) -> None:
        for i, tuple_obj in enumerate(h_page.tuples):
            if tuple_obj is not None:
                if not h_page.slot_manager.is_slot_used(i):
                    raise InvalidSlotError(
                        f"Tuple exists at slot {i} but slot is marked as unused"
                    )

                if tuple_obj.get_tuple_desc() != h_page.tuple_desc:
                    raise TupleValidationError(
                        f"Tuple at slot {i} has mismatched tuple descriptor"
                    )
