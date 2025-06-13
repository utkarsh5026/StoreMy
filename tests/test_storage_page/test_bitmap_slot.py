import pytest
import math
from app.storage.heap.bitmap_slot import BitmapSlotManager


class TestBitmapSlotManager:
    """Comprehensive tests for BitmapSlotManager implementation."""

    def test_init_without_bitmap_data(self):
        """Test initialization without existing bitmap data."""
        manager = BitmapSlotManager(8)

        assert manager.num_slots == 8
        assert manager.bitmap_size_bytes == 1  # 8 slots need 1 byte
        assert len(manager.bitmap) == 1
        assert manager.bitmap[0] == 0  # All slots initially unused

    def test_init_with_bitmap_data(self):
        """Test initialization with existing bitmap data."""
        # Create bitmap data with some slots used
        bitmap_data = b'\x0F'  # 00001111 - first 4 slots used
        manager = BitmapSlotManager(8, bitmap_data)

        assert manager.num_slots == 8
        assert manager.bitmap_size_bytes == 1
        assert len(manager.bitmap) == 1
        assert manager.bitmap[0] == 0x0F

    def test_init_bitmap_size_calculation(self):
        """Test bitmap size calculation for different slot counts."""
        # 1-8 slots: 1 byte
        manager = BitmapSlotManager(1)
        assert manager.bitmap_size_bytes == 1

        manager = BitmapSlotManager(8)
        assert manager.bitmap_size_bytes == 1

        # 9-16 slots: 2 bytes
        manager = BitmapSlotManager(9)
        assert manager.bitmap_size_bytes == 2

        manager = BitmapSlotManager(16)
        assert manager.bitmap_size_bytes == 2

        # 17-24 slots: 3 bytes
        manager = BitmapSlotManager(17)
        assert manager.bitmap_size_bytes == 3

        manager = BitmapSlotManager(24)
        assert manager.bitmap_size_bytes == 3

    def test_init_bitmap_data_size_mismatch_raises_error(self):
        """Test that bitmap data size mismatch raises ValueError."""
        # Need 2 bytes for 9 slots, but provide 1 byte
        with pytest.raises(ValueError, match="Bitmap data size mismatch: expected 2, got 1"):
            BitmapSlotManager(9, b'\x00')

        # Need 1 byte for 8 slots, but provide 2 bytes
        with pytest.raises(ValueError, match="Bitmap data size mismatch: expected 1, got 2"):
            BitmapSlotManager(8, b'\x00\x00')

    def test_is_slot_used_initially_false(self):
        """Test that all slots are initially unused."""
        manager = BitmapSlotManager(16)

        for slot in range(manager.num_slots):
            assert not manager.is_slot_used(slot)

    def test_is_slot_used_with_bitmap_data(self):
        """Test is_slot_used with existing bitmap data."""
        # Create bitmap with pattern: 10101010
        bitmap_data = b'\xAA'  # 10101010
        manager = BitmapSlotManager(8, bitmap_data)

        # Even slots should be unused, odd slots should be used
        for slot in range(8):
            if slot % 2 == 0:
                assert not manager.is_slot_used(slot)
            else:
                assert manager.is_slot_used(slot)

    def test_is_slot_used_out_of_range_raises_error(self):
        """Test that out of range slot raises IndexError."""
        manager = BitmapSlotManager(8)

        with pytest.raises(IndexError, match="Slot 8 out of range \\(max: 7\\)"):
            manager.is_slot_used(8)

        with pytest.raises(IndexError, match="Slot 100 out of range \\(max: 7\\)"):
            manager.is_slot_used(100)

    def test_set_slot_used_true(self):
        """Test setting slots as used."""
        manager = BitmapSlotManager(8)

        # Set slot 0 as used
        manager.set_slot_used(0, True)
        assert manager.is_slot_used(0)
        assert manager.bitmap[0] == 0x01  # 00000001

        # Set slot 7 as used
        manager.set_slot_used(7, True)
        assert manager.is_slot_used(7)
        assert manager.bitmap[0] == 0x81  # 10000001

    def test_set_slot_used_false(self):
        """Test setting slots as unused."""
        # Start with all slots used
        bitmap_data = b'\xFF'  # 11111111
        manager = BitmapSlotManager(8, bitmap_data)

        # Set slot 0 as unused
        manager.set_slot_used(0, False)
        assert not manager.is_slot_used(0)
        assert manager.bitmap[0] == 0xFE  # 11111110

        # Set slot 7 as unused
        manager.set_slot_used(7, False)
        assert not manager.is_slot_used(7)
        assert manager.bitmap[0] == 0x7E  # 01111110

    def test_set_slot_used_multiple_bytes(self):
        """Test setting slots across multiple bytes."""
        manager = BitmapSlotManager(16)

        # Set slot 0 (first byte, first bit)
        manager.set_slot_used(0, True)
        assert manager.is_slot_used(0)
        assert manager.bitmap[0] == 0x01
        assert manager.bitmap[1] == 0x00

        # Set slot 8 (second byte, first bit)
        manager.set_slot_used(8, True)
        assert manager.is_slot_used(8)
        assert manager.bitmap[0] == 0x01
        assert manager.bitmap[1] == 0x01

        # Set slot 15 (second byte, last bit)
        manager.set_slot_used(15, True)
        assert manager.is_slot_used(15)
        assert manager.bitmap[0] == 0x01
        assert manager.bitmap[1] == 0x81  # 10000001

    def test_set_slot_used_out_of_range_raises_error(self):
        """Test that out of range slot raises IndexError."""
        manager = BitmapSlotManager(8)
        print(manager.num_slots)

        with pytest.raises(IndexError, match="Slot 8 out of range \\(max: 7\\)"):
            manager.set_slot_used(8, True)

        with pytest.raises(IndexError, match="Slot -1 out of range \\(max: 7\\)"):
            manager.set_slot_used(-1, True)

    def test_find_free_slot_empty_manager(self):
        """Test finding free slot in empty manager."""
        manager = BitmapSlotManager(8)

        # Should return first slot (0)
        assert manager.find_free_slot() == 0

    def test_find_free_slot_with_used_slots(self):
        """Test finding free slot with some used slots."""
        manager = BitmapSlotManager(8)

        # Use first 3 slots
        manager.set_slot_used(0, True)
        manager.set_slot_used(1, True)
        manager.set_slot_used(2, True)

        # Should return slot 3
        assert manager.find_free_slot() == 3

    def test_find_free_slot_with_gaps(self):
        """Test finding free slot with gaps in usage."""
        manager = BitmapSlotManager(8)

        # Use slots 0, 2, 4
        manager.set_slot_used(0, True)
        manager.set_slot_used(2, True)
        manager.set_slot_used(4, True)

        # Should return first free slot (1)
        assert manager.find_free_slot() == 1

    def test_find_free_slot_all_used(self):
        """Test finding free slot when all slots are used."""
        manager = BitmapSlotManager(8)

        # Use all slots
        for slot in range(8):
            manager.set_slot_used(slot, True)

        # Should return None
        assert manager.find_free_slot() is None

    def test_find_free_slot_single_slot(self):
        """Test finding free slot with single slot manager."""
        manager = BitmapSlotManager(1)

        # Initially free
        assert manager.find_free_slot() == 0

        # After using it
        manager.set_slot_used(0, True)
        assert manager.find_free_slot() is None

    def test_get_free_slot_count_empty_manager(self):
        """Test counting free slots in empty manager."""
        manager = BitmapSlotManager(8)
        assert manager.get_free_slot_count() == 8

    def test_get_free_slot_count_with_used_slots(self):
        """Test counting free slots with some used slots."""
        manager = BitmapSlotManager(8)

        # Use 3 slots
        manager.set_slot_used(0, True)
        manager.set_slot_used(2, True)
        manager.set_slot_used(7, True)

        assert manager.get_free_slot_count() == 5

    def test_get_free_slot_count_all_used(self):
        """Test counting free slots when all slots are used."""
        manager = BitmapSlotManager(8)

        # Use all slots
        for slot in range(8):
            manager.set_slot_used(slot, True)

        assert manager.get_free_slot_count() == 0

    def test_get_free_slot_count_single_slot(self):
        """Test counting free slots with single slot manager."""
        manager = BitmapSlotManager(1)

        # Initially free
        assert manager.get_free_slot_count() == 1

        # After using it
        manager.set_slot_used(0, True)
        assert manager.get_free_slot_count() == 0

    def test_bit_manipulation_correctness(self):
        """Test that bit manipulation works correctly for all positions."""
        manager = BitmapSlotManager(8)

        # Test each bit position
        for bit_pos in range(8):
            # Set bit
            manager.set_slot_used(bit_pos, True)

            # Check only this bit is set
            expected_byte = 1 << bit_pos
            assert manager.bitmap[0] == expected_byte
            assert manager.is_slot_used(bit_pos)

            # Check other bits are not set
            for other_pos in range(8):
                if other_pos != bit_pos:
                    assert not manager.is_slot_used(other_pos)

            # Clear bit
            manager.set_slot_used(bit_pos, False)
            assert manager.bitmap[0] == 0
            assert not manager.is_slot_used(bit_pos)

    def test_large_slot_count(self):
        """Test with large number of slots."""
        manager = BitmapSlotManager(1000)

        expected_bytes = math.ceil(1000 / 8)
        assert manager.bitmap_size_bytes == expected_bytes
        assert len(manager.bitmap) == expected_bytes

        # Test some random slots
        test_slots = [0, 7, 8, 15, 16, 100, 500, 999]

        for slot in test_slots:
            assert not manager.is_slot_used(slot)
            manager.set_slot_used(slot, True)
            assert manager.is_slot_used(slot)

        assert manager.get_free_slot_count() == 1000 - len(test_slots)

    def test_edge_case_zero_slots(self):
        """Test edge case with zero slots."""
        manager = BitmapSlotManager(0)

        assert manager.num_slots == 0
        assert manager.bitmap_size_bytes == 0
        assert len(manager.bitmap) == 0
        assert manager.find_free_slot() is None
        assert manager.get_free_slot_count() == 0

    def test_comprehensive_slot_lifecycle(self):
        """Test comprehensive slot lifecycle operations."""
        manager = BitmapSlotManager(16)

        # Initially all free
        assert manager.get_free_slot_count() == 16

        # Allocate some slots
        allocated_slots = []
        for _ in range(5):
            slot = manager.find_free_slot()
            assert slot is not None
            allocated_slots.append(slot)
            manager.set_slot_used(slot, True)

        assert manager.get_free_slot_count() == 11
        assert len(allocated_slots) == 5

        # Free some slots
        for slot in allocated_slots[:3]:
            manager.set_slot_used(slot, False)

        assert manager.get_free_slot_count() == 14

        # Verify we can find and allocate freed slots
        new_slot = manager.find_free_slot()
        assert new_slot in allocated_slots[:3]

        # Fill up remaining slots
        while manager.get_free_slot_count() > 0:
            slot = manager.find_free_slot()
            manager.set_slot_used(slot, True)

        assert manager.get_free_slot_count() == 0
        assert manager.find_free_slot() is None

    def test_bitmap_data_preservation(self):
        """Test that bitmap data is correctly preserved and modified."""
        # Start with specific pattern
        bitmap_data = b'\x55\xAA'  # 01010101 10101010
        manager = BitmapSlotManager(16, bitmap_data)

        # Verify initial pattern
        assert manager.bitmap[0] == 0x55
        assert manager.bitmap[1] == 0xAA

        # Modify some bits
        manager.set_slot_used(0, True)   # Should set bit 0 in byte 0
        manager.set_slot_used(8, False)  # Should clear bit 0 in byte 1

        # 01010101 | 00000001 = 01010101
        assert manager.bitmap[0] == 0x55 | 0x01
        # 10101010 & 11111110 = 10101010
        assert manager.bitmap[1] == 0xAA & 0xFE
