from .slot_manager import SlotManager
from typing import Optional


class BitmapSlotManager(SlotManager):
    """Bitmap-based slot management."""

    def __init__(self, num_slots: int):
        self.num_slots = num_slots
        self.bitmap = bytearray((num_slots + 7) // 8)  # Ceiling division

    def allocate_slot(self) -> Optional[int]:
        for slot in range(self.num_slots):
            if not self.is_slot_used(slot):
                self._set_slot_used(slot, True)
                return slot
        return None

    def deallocate_slot(self, slot_number: int) -> None:
        if 0 <= slot_number < self.num_slots:
            self._set_slot_used(slot_number, False)

    def is_slot_used(self, slot_number: int) -> bool:
        if not (0 <= slot_number < self.num_slots):
            return False

        byte_index = slot_number // 8
        bit_offset = slot_number % 8
        bit_mask = 1 << bit_offset
        return (self.bitmap[byte_index] & bit_mask) != 0

    def _set_slot_used(self, slot_number: int, used: bool) -> None:
        byte_index = slot_number // 8
        bit_offset = slot_number % 8
        bit_mask = 1 << bit_offset

        if used:
            self.bitmap[byte_index] |= bit_mask
        else:
            self.bitmap[byte_index] &= ~bit_mask

    def get_free_slot_count(self) -> int:
        return sum(1 for slot in range(self.num_slots)
                   if not self.is_slot_used(slot))
