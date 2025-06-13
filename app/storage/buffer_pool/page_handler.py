"""
Page Handlers for Different Page Types

This implementation provides specialized handlers for:
1. Heap pages (table data)
2. B-tree pages (index data)
3. Hash pages (hash index data)
4. Metadata pages (system information)

Each handler implements page-specific operations like loading, saving,
validation, and optimization.
"""
import struct
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List
from pathlib import Path

from app.primitives import PageId, TransactionId
from app.storage.interfaces import Page
from app.core.exceptions import DbException, IOException


class PageHandler(ABC):
    """Abstract base class for page handlers."""

    @abstractmethod
    def load_page(self, page_id: PageId) -> Page:
        """Load a page from disk."""
        pass

    @abstractmethod
    def write_page(self, page: Page) -> None:
        """Write a page to disk."""
        pass

    @abstractmethod
    def validate_page(self, page: Page) -> bool:
        """Validate page structure and contents."""
        pass

    @abstractmethod
    def get_page_info(self, page: Page) -> Dict[str, Any]:
        """Get diagnostic information about a page."""
        pass


class HeapPageHandler(PageHandler):
    """Handler for heap pages containing table data."""

    def __init__(self):
        self.page_cache: Dict[PageId, Page] = {}

    def load_page(self, page_id: PageId) -> Page:
        """Load a heap page from disk."""
        from app.database import Database

        # Get the table file
        table_id = page_id.get_table_id()
        catalog = Database.get_catalog()
        db_file = catalog.get_db_file(table_id)

        if not db_file:
            raise DbException(f"Table {table_id} not found")

        try:
            # Read page from file
            page = db_file.read_page(page_id)

            # Validate page structure
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

            # Get the table file
            page_id = page.get_id()
            table_id = page_id.get_table_id()
            catalog = Database.get_catalog()
            db_file = catalog.get_db_file(table_id)

            if not db_file:
                raise DbException(f"Table {table_id} not found")

            # Write page to file
            db_file.write_page(page)

        except Exception as e:
            raise IOException(
                f"Failed to write heap page {page.get_id()}: {e}")

    def validate_page(self, page: Page) -> bool:
        """Validate heap page structure."""
        try:
            from app.storage.heap.heap_page import HeapPage

            if not isinstance(page, HeapPage):
                return False

            # Check page size
            if len(page.get_page_data()) != page.PAGE_SIZE_IN_BYTES:
                return False

            # Check header integrity
            if page.header_size <= 0 or page.header_size > page.PAGE_SIZE_IN_BYTES // 2:
                return False

            # Check slot count
            if page.num_slots <= 0 or page.num_slots > page.PAGE_SIZE_IN_BYTES:
                return False

            # Validate tuples
            for i, tuple_obj in enumerate(page.tuples):
                if tuple_obj is not None:
                    # Check if slot is marked as used
                    if not page.slot_manager.is_slot_used(i):
                        return False

                    # Check tuple schema
                    if tuple_obj.get_tuple_desc() != page.tuple_desc:
                        return False

            return True

        except Exception:
            return False

    def get_page_info(self, page: Page) -> Dict[str, Any]:
        """Get diagnostic information about a heap page."""
        from app.storage.heap.heap_page import HeapPage

        if not isinstance(page, HeapPage):
            return {'error': 'Not a heap page'}

        used_slots = sum(1 for i in range(page.num_slots)
                         if page.slot_manager.is_slot_used(i))

        return {
            'page_type': 'heap',
            'page_id': str(page.get_id()),
            'total_slots': page.num_slots,
            'used_slots': used_slots,
            'free_slots': page.num_slots - used_slots,
            'utilization': used_slots / page.num_slots if page.num_slots > 0 else 0,
            'header_size': page.header_size,
            'tuple_desc': str(page.tuple_desc),
            'is_dirty': page.is_dirty(),
            'dirty_transaction': str(page.dirty_transaction) if page.dirty_transaction else None
        }


class BTreePageHandler(PageHandler):
    """Handler for B-tree index pages."""

    def __init__(self):
        self.page_cache: Dict[PageId, Page] = {}

    def load_page(self, page_id: PageId) -> Page:
        """Load a B-tree page from disk."""
        try:
            # Determine if this is a leaf or internal page
            if hasattr(page_id, 'is_leaf'):
                is_leaf = page_id.is_leaf
            else:
                # Try to determine from file structure
                is_leaf = self._determine_page_type(page_id)

            # Load page data from file
            page_data = self._read_page_data(page_id)

            # Create appropriate page type
            if is_leaf:
                from app.storage.index.btree.btree_file import BTreeLeafPage
                page = BTreeLeafPage(page_id, self._get_key_type(page_id))
            else:
                from app.storage.index.btree.btree_file import BTreeInternalPage
                page = BTreeInternalPage(page_id, self._get_key_type(page_id))

            # Deserialize page data
            page.deserialize(page_data)

            # Validate page structure
            if not self.validate_page(page):
                raise DbException(f"Invalid B-tree page structure: {page_id}")

            return page

        except Exception as e:
            raise IOException(f"Failed to load B-tree page {page_id}: {e}")

    def write_page(self, page: Page) -> None:
        """Write a B-tree page to disk."""
        try:
            # Validate before writing
            if not self.validate_page(page):
                raise DbException(
                    f"Invalid B-tree page structure: {page.get_id()}")

            # Serialize page data
            page_data = page.serialize()

            # Write to file
            self._write_page_data(page.get_id(), page_data)

        except Exception as e:
            raise IOException(
                f"Failed to write B-tree page {page.get_id()}: {e}")

    def validate_page(self, page: Page) -> bool:
        """Validate B-tree page structure."""
        try:
            from app.storage.index.btree.btree_file import BTreePage, BTreeLeafPage, BTreeInternalPage

            if not isinstance(page, BTreePage):
                return False

            # Check page size
            if len(page.serialize()) != page.PAGE_SIZE:
                return False

            # Check entry count
            if page.get_num_entries() > page.max_entries:
                return False

            # Validate entries are sorted
            for i in range(1, len(page.entries)):
                if page.entries[i-1].key.compare_to(page.entries[i].key) >= 0:
                    return False

            # Page-specific validation
            if isinstance(page, BTreeLeafPage):
                return self._validate_leaf_page(page)
            elif isinstance(page, BTreeInternalPage):
                return self._validate_internal_page(page)

            return True

        except Exception:
            return False

    def _validate_leaf_page(self, page) -> bool:
        """Validate B-tree leaf page specific properties."""
        from app.primitives import RecordId

        # Check that all values are RecordIds
        for entry in page.entries:
            if not isinstance(entry.value, RecordId):
                return False

        return True

    def _validate_internal_page(self, page) -> bool:
        """Validate B-tree internal page specific properties."""
        from app.storage.index.btree.btree_file import BTreePageId

        # Check that all values are page IDs
        for entry in page.entries:
            if not isinstance(entry.value, BTreePageId):
                return False

        return True

    def get_page_info(self, page: Page) -> Dict[str, Any]:
        """Get diagnostic information about a B-tree page."""
        from app.storage.index.btree.btree_file import BTreePage, BTreeLeafPage, BTreeInternalPage

        if not isinstance(page, BTreePage):
            return {'error': 'Not a B-tree page'}

        info = {
            'page_type': 'btree_leaf' if isinstance(page, BTreeLeafPage) else 'btree_internal',
            'page_id': str(page.get_id()),
            'num_entries': page.get_num_entries(),
            'max_entries': page.max_entries,
            'utilization': page.get_num_entries() / page.max_entries if page.max_entries > 0 else 0,
            'is_full': page.is_full(),
            'is_underflow': page.is_underflow(),
            'key_type': page.key_type.name,
            'is_dirty': page.is_dirty(),
            'dirty_transaction': str(page.dirty_transaction) if page.dirty_transaction else None
        }

        if isinstance(page, BTreeLeafPage):
            info.update({
                'next_leaf': str(page.next_leaf_id) if page.next_leaf_id else None,
                'prev_leaf': str(page.prev_leaf_id) if page.prev_leaf_id else None
            })

        return info

    def _determine_page_type(self, page_id: PageId) -> bool:
        """Determine if a page is a leaf page by examining its data."""
        try:
            page_data = self._read_page_data(page_id)
            # Read page type from header (first 4 bytes)
            page_type = struct.unpack('!I', page_data[:4])[0]
            return page_type == 1  # 1 = leaf, 0 = internal
        except Exception:
            return True  # Default to leaf

    def _get_key_type(self, page_id: PageId):
        """Get the key type for a B-tree page."""
        from app.core.types import FieldType
        # This would typically be stored in index metadata
        # For now, return a default
        return FieldType.INT

    def _read_page_data(self, page_id: PageId) -> bytes:
        """Read raw page data from file."""
        # This would read from the actual index file
        # Implementation depends on file format
        return b'\x00' * 4096  # Placeholder

    def _write_page_data(self, page_id: PageId, data: bytes) -> None:
        """Write raw page data to file."""
        # This would write to the actual index file
        # Implementation depends on file format
        pass


class HashPageHandler(PageHandler):
    """Handler for hash index pages."""

    def __init__(self):
        self.page_cache: Dict[PageId, Page] = {}

    def load_page(self, page_id: PageId) -> Page:
        """Load a hash page from disk."""
        try:
            # Determine page type (bucket, overflow, or directory)
            page_type = self._determine_hash_page_type(page_id)

            # Load page data from file
            page_data = self._read_page_data(page_id)

            # Create appropriate page type
            if page_type == "directory":
                from app.storage.index.hash.hash_file import HashDirectoryPage
                page = HashDirectoryPage(page_id)
            else:  # bucket or overflow
                from app.storage.index.hash.hash_file import HashBucketPage
                page = HashBucketPage(page_id, self._get_key_type(page_id))

            # Deserialize page data
            page.deserialize(page_data)

            # Validate page structure
            if not self.validate_page(page):
                raise DbException(f"Invalid hash page structure: {page_id}")

            return page

        except Exception as e:
            raise IOException(f"Failed to load hash page {page_id}: {e}")

    def write_page(self, page: Page) -> None:
        """Write a hash page to disk."""
        try:
            # Validate before writing
            if not self.validate_page(page):
                raise DbException(
                    f"Invalid hash page structure: {page.get_id()}")

            # Serialize page data
            page_data = page.serialize()

            # Write to file
            self._write_page_data(page.get_id(), page_data)

        except Exception as e:
            raise IOException(
                f"Failed to write hash page {page.get_id()}: {e}")

    def validate_page(self, page: Page) -> bool:
        """Validate hash page structure."""
        try:
            from app.storage.index.hash.hash_file import HashBucketPage, HashDirectoryPage

            if isinstance(page, HashBucketPage):
                return self._validate_bucket_page(page)
            elif isinstance(page, HashDirectoryPage):
                return self._validate_directory_page(page)

            return False

        except Exception:
            return False

    def _validate_bucket_page(self, page) -> bool:
        """Validate hash bucket page."""
        # Check page size
        if len(page.serialize()) != page.PAGE_SIZE:
            return False

        # Check entry count
        if len(page.entries) > page.max_entries:
            return False

        # Check that all entries have valid hash values
        for entry in page.entries:
            if entry.hash_value < 0:
                return False

        return True

    def _validate_directory_page(self, page) -> bool:
        """Validate hash directory page."""
        # Check page size
        if len(page.serialize()) != page.PAGE_SIZE:
            return False

        # Check global depth
        if page.global_depth < 0 or page.global_depth > 32:
            return False

        # Check bucket pointer count
        expected_buckets = 2 ** page.global_depth
        if len(page.bucket_pointers) != expected_buckets:
            return False

        return True

    def get_page_info(self, page: Page) -> Dict[str, Any]:
        """Get diagnostic information about a hash page."""
        from app.storage.index.hash.hash_file import HashBucketPage, HashDirectoryPage

        if isinstance(page, HashBucketPage):
            return {
                'page_type': 'hash_bucket',
                'page_id': str(page.get_id()),
                'num_entries': len(page.entries),
                'max_entries': page.max_entries,
                'utilization': len(page.entries) / page.max_entries if page.max_entries > 0 else 0,
                'is_full': page.is_full(),
                'overflow_page': str(page.overflow_page_id) if page.overflow_page_id else None,
                'key_type': page.key_type.name,
                'is_dirty': page.is_dirty(),
                'dirty_transaction': str(page.dirty_transaction) if page.dirty_transaction else None
            }

        elif isinstance(page, HashDirectoryPage):
            return {
                'page_type': 'hash_directory',
                'page_id': str(page.get_id()),
                'global_depth': page.global_depth,
                'num_buckets': len(page.bucket_pointers),
                'expected_buckets': 2 ** page.global_depth,
                'is_dirty': page.is_dirty(),
                'dirty_transaction': str(page.dirty_transaction) if page.dirty_transaction else None
            }

        else:
            return {'error': 'Not a hash page'}

    def _determine_hash_page_type(self, page_id: PageId) -> str:
        """Determine the type of hash page."""
        if hasattr(page_id, 'page_type'):
            return page_id.page_type
        return "bucket"  # Default

    def _get_key_type(self, page_id: PageId):
        """Get the key type for a hash page."""
        from app.core.types import FieldType
        # This would typically be stored in index metadata
        return FieldType.INT

    def _read_page_data(self, page_id: PageId) -> bytes:
        """Read raw page data from file."""
        # This would read from the actual index file
        return b'\x00' * 4096  # Placeholder

    def _write_page_data(self, page_id: PageId, data: bytes) -> None:
        """Write raw page data to file."""
        # This would write to the actual index file
        pass


class MetadataPageHandler(PageHandler):
    """Handler for system metadata pages."""

    def __init__(self):
        self.page_cache: Dict[PageId, Page] = {}

    def load_page(self, page_id: PageId) -> Page:
        """Load a metadata page from disk."""
        try:
            # Load raw data
            page_data = self._read_metadata_page(page_id)

            # Create metadata page wrapper
            page = MetadataPage(page_id, page_data)

            # Validate
            if not self.validate_page(page):
                raise DbException(f"Invalid metadata page: {page_id}")

            return page

        except Exception as e:
            raise IOException(f"Failed to load metadata page {page_id}: {e}")

    def write_page(self, page: Page) -> None:
        """Write a metadata page to disk."""
        try:
            if not self.validate_page(page):
                raise DbException(f"Invalid metadata page: {page.get_id()}")

            # Write metadata to file
            self._write_metadata_page(page.get_id(), page.get_data())

        except Exception as e:
            raise IOException(
                f"Failed to write metadata page {page.get_id()}: {e}")

    def validate_page(self, page: Page) -> bool:
        """Validate metadata page structure."""
        try:
            if not isinstance(page, MetadataPage):
                return False

            # Check data format
            data = page.get_data()
            if len(data) == 0:
                return False

            # Validate metadata structure
            return self._validate_metadata_structure(data)

        except Exception:
            return False

    def _validate_metadata_structure(self, data: Dict[str, Any]) -> bool:
        """Validate metadata structure."""
        required_fields = ['version', 'type', 'created_at']

        for field in required_fields:
            if field not in data:
                return False

        return True

    def get_page_info(self, page: Page) -> Dict[str, Any]:
        """Get diagnostic information about a metadata page."""
        if not isinstance(page, MetadataPage):
            return {'error': 'Not a metadata page'}

        data = page.get_data()

        return {
            'page_type': 'metadata',
            'page_id': str(page.get_id()),
            'metadata_type': data.get('type', 'unknown'),
            'version': data.get('version', 'unknown'),
            'created_at': data.get('created_at', 0),
            'size': len(str(data)),
            'is_dirty': page.is_dirty(),
            'dirty_transaction': str(page.dirty_transaction) if page.dirty_transaction else None
        }

    def _read_metadata_page(self, page_id: PageId) -> Dict[str, Any]:
        """Read metadata from file."""
        # This would read from system metadata files
        return {'version': '1.0', 'type': 'system', 'created_at': 0}

    def _write_metadata_page(self, page_id: PageId, data: Dict[str, Any]) -> None:
        """Write metadata to file."""
        # This would write to system metadata files
        pass


class MetadataPage(Page):
    """Simple metadata page implementation."""

    def __init__(self, page_id: PageId, data: Dict[str, Any]):
        self.page_id = page_id
        self.data = data
        self._is_dirty = False
        self.dirty_transaction = None

    def get_id(self) -> PageId:
        return self.page_id

    def get_data(self) -> Dict[str, Any]:
        return self.data

    def is_dirty(self) -> bool:
        return self._is_dirty

    def mark_dirty(self, dirty: bool, tid: Optional[TransactionId] = None) -> None:
        self._is_dirty = dirty
        if dirty:
            self.dirty_transaction = tid
        else:
            self.dirty_transaction = None

    def get_page_data(self) -> bytes:
        """Get serialized page data."""
        import json
        return json.dumps(self.data).encode('utf-8')


class PageHandlerFactory:
    """Factory for creating appropriate page handlers."""

    _handlers = {
        'heap': HeapPageHandler,
        'btree': BTreePageHandler,
        'hash': HashPageHandler,
        'metadata': MetadataPageHandler
    }

    @classmethod
    def get_handler(cls, page_type: str) -> PageHandler:
        """Get handler for a specific page type."""
        handler_class = cls._handlers.get(page_type)
        if not handler_class:
            raise ValueError(f"Unknown page type: {page_type}")

        return handler_class()

    @classmethod
    def register_handler(cls, page_type: str, handler_class: type) -> None:
        """Register a custom page handler."""
        cls._handlers[page_type] = handler_class

    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported page types."""
        return list(cls._handlers.keys())
