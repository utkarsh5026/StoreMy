import math
from typing import Optional, Iterator, TYPE_CHECKING
from app.primitives import TransactionId
from app.storage.heap.heap_page_id import HeapPageId
from .bitmap_slot import BitmapSlotManager
from app.primitives import RecordId

if TYPE_CHECKING:
    from app.core.tuple import Tuple, TupleDesc


class HeapPage:
    """
    Implementation of a database page for heap files with full header support.

    Page Layout (following SimpleDB format):
    1. Header: Bitmap tracking which slots are used (1 bit per slot)
    2. Tuples: Fixed-size slots containing tuple data
    3. Padding: Unused space to reach PAGE_SIZE

    The header size is calculated as: ceil(num_slots / 8) bytes
    Each tuple occupies: tuple_desc.get_size() bytes
    """

    PAGE_SIZE_IN_BYTES = 4096  # Standard 4KB page size

    def __init__(self,
                 page_id: HeapPageId,
                 data: Optional[bytes] = None,
                 tuple_desc: Optional['TupleDesc'] = None):
        """
        Initialize a HeapPage.

        Args:
            page_id: Unique identifier for this page
            data: Raw page data to deserialize (if reading from disk)
            tuple_desc: Schema information (if creating new page)
        """
        self.page_id = page_id
        self.dirty_transaction: Optional['TransactionId'] = None
        self.before_image_data: Optional[bytes] = None

        if data is not None:
            # Deserializing existing page from disk
            self._deserialize(data)
        else:
            # Creating new empty page
            if tuple_desc is None:
                raise ValueError("Must provide tuple_desc for new page")
            self.tuple_desc = tuple_desc
            self._initialize_empty_page()

        self.set_before_image()


    @property
    def header_size(self) -> int:
        return self._calculate_header_size()

    @property
    def header(self):
        return self.slot_manager.bitmap.copy()

    def _initialize_empty_page(self) -> None:
        """Initialize an empty page with no tuples."""
        self.num_slots = self._calculate_num_slots()
        self.slot_manager = BitmapSlotManager(self.num_slots)
        self.tuples: list[Optional['Tuple']] = [None] * self.num_slots

    def _calculate_num_slots(self) -> int:
        """
        Calculate the maximum number of tuple slots on this page.

        Uses SimpleDB formula: floor((PAGE_SIZE * 8) / (tuple_size * 8 + 1))

        Explanation:
        - PAGE_SIZE * 8: Total bits available in page
        - tuple_size * 8: Bits needed for tuple data
        - +1: One header bit per tuple slot

        Returns:
            Maximum number of slots that can fit on this page
        """
        tuple_size = self.tuple_desc.get_size()
        return (self.PAGE_SIZE_IN_BYTES * 8) // (tuple_size * 8 + 1)

    def _calculate_header_size(self) -> int:
        """
        Calculate header size in bytes.

        Formula: ceil(num_slots / 8)
        We need one bit per slot, rounded up to nearest byte.

        Returns:
            Header size in bytes
        """
        return math.ceil(self.num_slots / 8)

    def _deserialize(self, data: bytes) -> None:
        """
        Reconstruct page state from raw bytes using dependency injection.

        This method follows the SimpleDB page format:
        1. Read header bitmap
        2. Read tuple data from slots
        3. Reconstruct tuples using schema from catalog

        Args:
            data: Raw page data from disk

        Raises:
            ValueError: If data is invalid or catalog provider is missing
        """
        if len(data) != self.PAGE_SIZE_IN_BYTES:
            raise ValueError(f"Page data must be exactly {self.PAGE_SIZE_IN_BYTES} bytes")



        # Calculate layout parameters
        self.num_slots = self._calculate_num_slots()
        header_size = self._calculate_header_size()
        tuple_size = self.tuple_desc.get_size()

        # Extract header bitmap
        header_data = data[:header_size]
        self.slot_manager = BitmapSlotManager(self.num_slots, header_data)

        # Extract tuple data section
        tuple_data_start = header_size
        tuple_data_end = tuple_data_start + (self.num_slots * tuple_size)
        tuple_data_section = data[tuple_data_start:tuple_data_end]

        # Deserialize tuples from slots
        self.tuples = [None] * self.num_slots

        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot):
                # Extract tuple data for this slot
                slot_start = slot * tuple_size
                slot_end = slot_start + tuple_size
                slot_data = tuple_data_section[slot_start:slot_end]

                # Deserialize tuple using schema
                from app.core.tuple import Tuple  # Import here to avoid circular imports
                tuple_obj = Tuple.deserialize(slot_data, self.tuple_desc)

                record_id = RecordId(self.page_id, slot)
                tuple_obj.set_record_id(record_id)

                self.tuples[slot] = tuple_obj

    def get_page_data(self) -> bytes:
        """
        Serialize this page to bytes for disk storage.

        Format follows SimpleDB layout:
        1. Header bitmap (1 bit per slot, packed into bytes)
        2. Tuple data (fixed-size slots)
        3. Padding to reach PAGE_SIZE

        Returns:
            Serialized page data ready for disk storage
        """
        data = bytearray()

        # 1. Add header bitmap
        header_data = bytes(self.slot_manager.bitmap)
        data.extend(header_data)

        # 2. Add tuple data
        tuple_size = self.tuple_desc.get_size()
        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot) and self.tuples[slot] is not None:
                # Serialize the tuple
                tuple_data = self.tuples[slot].serialize()
                if len(tuple_data) != tuple_size:
                    raise RuntimeError(f"Tuple size mismatch: expected {tuple_size}, got {len(tuple_data)}")
                data.extend(tuple_data)
            else:
                # Empty slot - fill with zeros
                data.extend(b'\x00' * tuple_size)

        # 3. Add padding to reach PAGE_SIZE
        current_size = len(data)
        if current_size > self.PAGE_SIZE_IN_BYTES:
            raise RuntimeError(f"Page data too large: {current_size} > {self.PAGE_SIZE_IN_BYTES}")

        padding_size = self.PAGE_SIZE_IN_BYTES - current_size
        data.extend(b'\x00' * padding_size)

        return bytes(data)

    def insert_tuple(self, tuple_obj: 'Tuple') -> Optional[int]:
        """
        Insert a tuple into the page.

        Args:
            tuple_obj: Tuple to insert

        Returns:
            Slot number where tuple was inserted, or None if the page is full
        """
        if tuple_obj.get_tuple_desc() != self.tuple_desc:
            raise ValueError("Tuple schema doesn't match page schema")

        # Find free slot
        slot = self.slot_manager.find_free_slot()
        if slot is None:
            return None  # Page is full

        # Insert tuple
        self.tuples[slot] = tuple_obj
        self.slot_manager.set_slot_used(slot, True)

        # Update tuple's record ID
        from app.core.tuple import RecordId
        record_id = RecordId(self.page_id, slot)
        tuple_obj.set_record_id(record_id)

        return slot

    def delete_tuple(self, tuple_obj: 'Tuple') -> bool:
        """
        Delete a tuple from the page.

        Args:
            tuple_obj: Tuple to delete

        Returns:
            True if tuple was found and deleted, False otherwise
        """
        record_id = tuple_obj.get_record_id()
        if record_id is None or record_id.get_page_id() != self.page_id:
            return False

        slot = record_id.get_tuple_number()
        if slot >= self.num_slots or not self.slot_manager.is_slot_used(slot):
            return False

        # Remove tuple
        self.tuples[slot] = None
        self.slot_manager.set_slot_used(slot, False)

        # Clear tuple's record ID
        tuple_obj.set_record_id(None)

        return True

    def get_tuple(self, slot: int) -> Optional['Tuple']:
        """Get tuple at specific slot."""
        if slot >= self.num_slots or not self.slot_manager.is_slot_used(slot):
            return None
        return self.tuples[slot]

    def iterator(self) -> Iterator['Tuple']:
        """Iterate over all tuples in used slots."""
        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot) and self.tuples[slot] is not None:
                yield self.tuples[slot]

    def get_num_empty_slots(self) -> int:
        """Return number of empty slots."""
        return self.slot_manager.get_free_slot_count()

    def get_id(self) -> HeapPageId:
        """Get page identifier."""
        return self.page_id

    def is_dirty(self) -> Optional['TransactionId']:
        """Check if page has been modified."""
        return self.dirty_transaction

    def mark_dirty(self, dirty: bool, transaction_id: Optional['TransactionId']) -> None:
        """Mark page as dirty or clean."""
        if dirty:
            self.dirty_transaction = transaction_id
        else:
            self.dirty_transaction = None

    def set_before_image(self) -> None:
        """Save current page state as before image for recovery."""
        self.before_image_data = self.get_page_data()

    def get_before_image(self) -> 'HeapPage':
        """Get page state before modifications."""
        if self.before_image_data is None:
            raise RuntimeError("No before image available")

        # Create new page from before image data
        before_page = HeapPage(
            self.page_id,
            self.before_image_data,
        )
        return before_page

    @staticmethod
    def create_empty_page_data() -> bytes:
        """Create empty page data filled with zeros."""
        return b'\x00' * HeapPage.PAGE_SIZE_IN_BYTES

    def __str__(self) -> str:
        used_slots = self.num_slots - self.slot_manager.get_free_slot_count()
        return f"HeapPage(id={self.page_id}, slots={used_slots}/{self.num_slots})"