from typing import Optional
import math


class BitmapSlotManager:
    """Manages slot allocation using a bitmap."""

    def __init__(self, num_slots: int, bitmap_data: Optional[bytes] = None):
        self.num_slots = num_slots
        self.bitmap_size_bytes = math.ceil(num_slots / 8)

        if bitmap_data is not None:
            if len(bitmap_data) != self.bitmap_size_bytes:
                raise ValueError(
                    f"Bitmap data size mismatch: expected {self.bitmap_size_bytes}, got {len(bitmap_data)}")
            self.bitmap = bytearray(bitmap_data)
        else:
            self.bitmap = bytearray(self.bitmap_size_bytes)

    def is_slot_used(self, slot_number: int) -> bool:
        """Check if a slot is used."""
        if slot_number >= self.num_slots:
            raise IndexError(
                f"Slot {slot_number} out of range (max: {self.num_slots-1})")

        byte_index = slot_number // 8
        bit_index = slot_number % 8
        return bool(self.bitmap[byte_index] & (1 << bit_index))

    def set_slot_used(self, slot_number: int, used: bool) -> None:
        """Set slot usage status."""
        if slot_number >= self.num_slots:
            raise IndexError(
                f"Slot {slot_number} out of range (max: {self.num_slots-1})")

        byte_index = slot_number // 8
        bit_index = slot_number % 8

        if used:
            self.bitmap[byte_index] |= (1 << bit_index)
        else:
            self.bitmap[byte_index] &= ~(1 << bit_index)

    def find_free_slot(self) -> Optional[int]:
        """Find the first free slot, or return None if all are used."""
        for slot in range(self.num_slots):
            if not self.is_slot_used(slot):
                return slot
        return None

    def get_free_slot_count(self) -> int:
        """Count number of free slots."""
        count = 0
        for slot in range(self.num_slots):
            if not self.is_slot_used(slot):
                count += 1
        return count
