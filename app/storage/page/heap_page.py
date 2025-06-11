import math
from typing import List, Optional, Iterator, TYPE_CHECKING
from ...primitives import TransactionId
from .page import Page
from .heap_page_id import HeapPageId

if TYPE_CHECKING:
    from ...core.tuple import Tuple, TupleDesc


class HeapPage(Page):
    """
    Implementation of a database page for heap files.

    Page Layout:
    1. Header: bit vector indicating which slots are used
    2. Tuples: fixed-size slots for tuple data
    3. Padding: unused space at end of page

    The number of slots is calculated based on:
    - Page size (4096 bytes)
    - Tuple size (from TupleDesc)
    - Header overhead (1 bit per slot)

    Formula: slots = floor((PAGE_SIZE * 8) / (tuple_size * 8 + 1))
    The "+1" accounts for the header bit per slot.
    """

    def __init__(self, page_id: HeapPageId, data: Optional[bytes] = None, tuple_desc: Optional['TupleDesc'] = None):
        self.page_id = page_id
        self.dirty_transaction: Optional[TransactionId] = None
        self.before_image_data: Optional[bytes] = None

        if data is not None:
            self._deserialize(data)
        else:
            if tuple_desc is None:
                raise ValueError("Must provide tuple_desc for new page")
            self.tuple_desc = tuple_desc
            self._initialize_empty_page()

        # Save initial state as before image
        self.set_before_image()

    def _initialize_empty_page(self) -> None:
        """Initialize an empty page with no tuples."""
        self.num_slots = self._calculate_num_slots()
        self.header_size = self._calculate_header_size()

        # Initialize header (all slots empty)
        self.header = bytearray(self.header_size)

        self.tuples: List[Optional['Tuple']] = [None] * self.num_slots

    def _calculate_num_slots(self) -> int:
        """
        Calculate the maximum number of tuple slots on this page.

        Formula: floor((PAGE_SIZE * 8) / (tuple_size * 8 + 1))

        Explanation:
        - PAGE_SIZE * 8: total bits available
        - tuple_size * 8: bits needed for one tuple
        - +1: one header bit per tuple
        """
        tuple_size = self.tuple_desc.get_size()
        return (self.PAGE_SIZE_IN_BYTES * 8) // (tuple_size * 8 + 1)

    def _calculate_header_size(self) -> int:
        """
        Calculate header size in bytes.

        We need one bit per slot, rounded up to the nearest byte.
        """
        return math.ceil(self.num_slots / 8.0)

    def _deserialize(self, data: bytes) -> None:
        """
        Reconstruct page state from raw bytes.

        This is complex because we need to:
        1. Extract the tuple descriptor somehow (not stored in page)
        2. Parse the header bit vector
        3. Deserialize each tuple from its slot

        Note: In a real implementation, we'd need the catalog to
        provide the TupleDesc. For now, we'll require it to be
        passed in during construction.
        """
        # This is a simplified version - in reality we'd need
        # the catalog to provide the schema
        raise NotImplementedError(
            "Page deserialization requires catalog integration")

    def get_id(self) -> HeapPageId:
        return self.page_id

    def is_dirty(self) -> Optional[TransactionId]:
        return self.dirty_transaction

    def mark_dirty(self, dirty: bool, transaction_id: Optional[TransactionId]) -> None:
        if dirty:
            self.dirty_transaction = transaction_id
        else:
            self.dirty_transaction = None

    def get_before_image(self) -> 'HeapPage':
        """
        Return a copy of this page from before modifications.

        Used for recovery - if we need to undo changes, we can
        restore the page to its before image state.
        """
        if self.before_image_data is None:
            return self._create_copy()

        # Recreate page from saved before image
        before_page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        before_page._deserialize(self.before_image_data)
        return before_page

    def set_before_image(self) -> None:
        """Save current page state as the before image."""
        self.before_image_data = self.get_page_data()

    def _create_copy(self) -> 'HeapPage':
        """Create a deep copy of this page."""
        copy_page = HeapPage(self.page_id, tuple_desc=self.tuple_desc)
        copy_page.header = self.header.copy()
        copy_page.tuples = self.tuples.copy()
        copy_page.dirty_transaction = self.dirty_transaction
        return copy_page

    def get_num_empty_slots(self) -> int:
        """Return the number of empty slots on this page."""
        empty_count = 0
        for slot in range(self.num_slots):
            if not self._is_slot_used(slot):
                empty_count += 1
        return empty_count

    def _is_slot_used(self, slot_number: int) -> bool:
        """Check if a specific slot contains a tuple."""
        if not (0 <= slot_number < self.num_slots):
            return False

        byte_index = slot_number // 8
        bit_offset = slot_number % 8
        bit_mask = 1 << bit_offset

        return (self.header[byte_index] & bit_mask) != 0

    def _set_slot_used(self, slot_number: int, used: bool) -> None:
        """Mark a slot as used or unused in the header."""
        if not (0 <= slot_number < self.num_slots):
            raise IndexError(
                f"Slot {slot_number} out of range [0, {self.num_slots})")

        byte_index = slot_number // 8
        bit_offset = slot_number % 8
        bit_mask = 1 << bit_offset

        if used:
            self.header[byte_index] |= bit_mask
        else:
            self.header[byte_index] &= ~bit_mask

    def add_tuple(self, tuple_obj: 'Tuple') -> None:
        """
        Add a tuple to the first available slot on this page.

        Raises:
            ValueError: if the tuple schema doesn't match
            RuntimeError: if no empty slots available
        """
        # Validate tuple schema
        if not self.tuple_desc.equals(tuple_obj.get_tuple_desc()):
            raise ValueError("Tuple schema doesn't match page schema")

        if not tuple_obj.is_complete():
            raise ValueError("Cannot add incomplete tuple to page")

        # Find first empty slot
        empty_slot = None
        for slot in range(self.num_slots):
            if not self._is_slot_used(slot):
                empty_slot = slot
                break

        if empty_slot is None:
            raise RuntimeError("No empty slots available on page")

        # Add tuple to slot
        self.tuples[empty_slot] = tuple_obj
        self._set_slot_used(empty_slot, True)

        # Set the tuple's record ID
        from ...primitives import RecordId
        record_id = RecordId(self.page_id, empty_slot)
        tuple_obj.set_record_id(record_id)

    def delete_tuple(self, tuple_obj: 'Tuple') -> None:
        """
        Remove a tuple from this page.

        The tuple's RecordId must point to a valid slot on this page.
        """
        record_id = tuple_obj.get_record_id()
        if record_id is None:
            raise ValueError("Tuple has no RecordId - cannot delete")

        if record_id.get_page_id() != self.page_id:
            raise ValueError("Tuple RecordId points to different page")

        slot_number = record_id.get_tuple_number()
        if not self._is_slot_used(slot_number):
            raise ValueError(f"Slot {slot_number} is already empty")

        # Remove tuple and mark slot as empty
        self.tuples[slot_number] = None
        self._set_slot_used(slot_number, False)

        # Clear the tuple's record ID
        tuple_obj.set_record_id(None)

    def iterator(self) -> Iterator['Tuple']:
        """
        Return an iterator over all tuples on this page.

        Only returns tuples in used slots.
        """
        for slot in range(self.num_slots):
            if self._is_slot_used(slot) and self.tuples[slot] is not None:
                yield self.tuples[slot]

    def get_page_data(self) -> bytes:
        """
        Serialize this page to bytes for disk storage.

        Format:
        1. Header bytes (bit vector)
        2. Tuple data (serialized tuples in slots)
        3. Padding to reach PAGE_SIZE
        """
        data = bytearray()

        # Add header
        data.extend(self.header)

        # Add tuple data
        tuple_size = self.tuple_desc.get_size()
        for slot in range(self.num_slots):
            if self._is_slot_used(slot) and self.tuples[slot] is not None:
                # Serialize the tuple
                tuple_data = self.tuples[slot].serialize()
            else:
                # Empty slot - fill with zeros
                tuple_data = b'\x00' * tuple_size

            data.extend(tuple_data)

        # Pad to PAGE_SIZE
        current_size = len(data)
        if current_size > self.PAGE_SIZE_IN_BYTES:
            raise RuntimeError(
                f"Page data too large: {current_size} > {self.PAGE_SIZE_IN_BYTES}")

        padding_size = self.PAGE_SIZE_IN_BYTES - current_size
        data.extend(b'\x00' * padding_size)

        return bytes(data)

    @staticmethod
    def create_empty_page_data() -> bytes:
        """Create empty page data filled with zeros."""
        return b'\x00' * HeapPage.PAGE_SIZE_IN_BYTES

    def __str__(self) -> str:
        used_slots = sum(1 for slot in range(self.num_slots)
                         if self._is_slot_used(slot))
        return f"HeapPage(id={self.page_id}, slots={used_slots}/{self.num_slots})"
