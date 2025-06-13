from typing import Optional
import math


class BitmapSlotManager:
    """
    🗂️ Manages slot allocation using a bitmap 🗂️

    💡 A bitmap is a space-efficient way to track which slots are free/used.
    Each bit represents one slot: 0 = free, 1 = used

    Bitmap Structure:
    ---------------------------------------------------
     Byte 0         │ Byte 1          │ Byte 2        │ ...         
     7 6 5 4 3 2 1 0│ 7 6 5 4 3 2 1 0│ 7 6 5 4 3 2 1 0│         
     │ │ │ │ │ │ │ ││ │ │ │ │ │ │ │ ││ │ │ │ │ │ │ │ ││         
     │ │ │ │ │ │ │ ││ │ │ │ │ │ │ │ ││ │ │ │ │ │ │ │ ││         
     Slot positions │                │                │             
     0 1 2 3 4 5 6 7│ 8 9 0 1 2 3 4 5│6 7 8 9 0 1 2 3 │         
     --------------------------------------------------

    Example with 10 slots (slots 0, 2, 7 are used):
    -----------------------------------------
     Byte 0: 10000101 (0x85)     │  Slots 0,2,7 used
     Byte 1: 00000001 (0x01)     │  Slot 8 used  
     Remaining bits ignored       │
     -----------------------------------------

    🎯 Key Benefits:
    ✅ Space efficient: 1 bit per slot
    🚀 Fast operations: bitwise operations
    🔍 Easy to scan for free slots
    💾 Compact storage format
    """

    BITS_PER_BYTE = 8

    def __init__(self, num_slots: int, bitmap_data: Optional[bytes] = None):
        self.num_slots = num_slots
        self.bitmap_size_bytes = math.ceil(
            num_slots / BitmapSlotManager.BITS_PER_BYTE)

        if bitmap_data is not None:
            if len(bitmap_data) != self.bitmap_size_bytes:
                raise ValueError(
                    f"Bitmap data size mismatch: expected {self.bitmap_size_bytes}, got {len(bitmap_data)}")
            self.bitmap = bytearray(bitmap_data)
        else:
            self.bitmap = bytearray(self.bitmap_size_bytes)

    def is_slot_used(self, slot_number: int) -> bool:
        """
        🔍 Check if a slot is used 🔍

        Bit Lookup Process:

        1. 📍 Calculate byte position: slot_number ÷ BITS_PER_BYTE (8)       
            Slot 10 → Byte 1 (10 ÷ 8 = 1)                   

        2. 🎯 Calculate bit position: slot_number % BITS_PER_BYTE (8)        
            Slot 10 → Bit 2 (10 % 8 = 2)                    

        3. 🔬 Extract bit using mask: (1 << bit_pos)        
            Mask for bit 2: 00000100                         

        4. ✅ Test bit: byte & mask != 0                    
            Example: 11010110 & 00000100 = 00000100 = True   

        Visual Example (checking slot 10):
        ┌─────────────────────────────────────────────────────┐
        │ Slot numbers: 0 1 2 3 4 5 6 7│8 9 0 1 2 3 4 5      │
        │ Byte 0:       1 0 1 0 1 1 0 1│                     │
        │ Byte 1:       0 1 1 0 0 1 0 1│                     │
        │                         ↑                           │
        │                    Slot 10 = 1 (USED) ✅           │
        └─────────────────────────────────────────────────────┘
        """
        if slot_number >= self.num_slots:
            raise IndexError(
                f"Slot {slot_number} out of range (max: {self.num_slots-1})")

        byte_index = slot_number // BitmapSlotManager.BITS_PER_BYTE
        bit_index = slot_number % BitmapSlotManager.BITS_PER_BYTE
        return bool(self.bitmap[byte_index] & (1 << bit_index))

    def set_slot_used(self, slot_number: int, used: bool) -> None:
        """
        ✏️ Set slot usage status ✏️

        Bit Manipulation Operations:
        🟢 Setting bit to 1 (mark as USED):                 
            byte |= (1 << bit_pos)                           
            Before: 10100110                                 
            Mask:   00001000  (1 << 3)                      
            After:  10101110  (OR operation)                

        🔴 Setting bit to 0 (mark as FREE):                 
            byte &= ~(1 << bit_pos)                          
            Before: 10101110                                 
            Mask:   11110111  (~(1 << 3))                   
            After:  10100110  (AND operation)               

        Visual Example (setting slot 3):
        ┌─────────────────────────────────────────────────────┐
        │ Slot positions: 0 1 2 3 4 5 6 7                     │
        │                     ↑                               │
        │ Before: 1 0 1 0 0 1 1 0  (slot 3 = 0, FREE)         │
        │ After:  1 0 1 1 0 1 1 0  (slot 3 = 1, USED) ✅      │
        │                                                     │
        │ OR with: 0 0 0 1 0 0 0 0  (mask = 1 << 3)           │
        └─────────────────────────────────────────────────────┘

        Args:
            🎯 slot_number: Which slot to modify
            ✅ used: True to mark as used, False to mark as free
        """
        if slot_number >= self.num_slots or slot_number < 0:
            raise IndexError(
                f"Slot {slot_number} out of range (max: {self.num_slots-1})")

        byte_index = slot_number // 8
        bit_index = slot_number % 8

        if used:
            self.bitmap[byte_index] |= (1 << bit_index)
        else:
            self.bitmap[byte_index] &= ~(1 << bit_index)

    def find_free_slot(self) -> Optional[int]:
        """
        🔍 Find the first free slot, or return None if all are used 🔍

        Search Algorithm:
        1. 🚀 Scan from slot 0 to num_slots-1                
        2. ✅ For each slot, check if bit = 0 (free)         
        3. 🎯 Return first slot where bit = 0                
        4. 🚫 Return None if no free slots found             

        Visual Search Example:
        -----------------------------------------
         Slots: 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5              
         Bits:  1 1 0 1 1 1 1 0 1 1 0 1 1 1 1 1              
                    ↑         ↑     ↑                        
                First      Skip   Found!                    
                free      (used)   slot 10                  
                (slot 2)                                      

        🎉 Returns: 2 (first free slot found)   
        -----------------------------------------

        Returns:
            🎯 First free slot number, or None if all slots used
        """
        for slot in range(self.num_slots):
            if not self.is_slot_used(slot):
                return slot
        return None

    def get_free_slot_count(self) -> int:
        """
        📊 Count number of free slots 📊

        Counting Algorithm:
        1. 🔄 Iterate through all slots                      
        2. ➕ Increment counter for each free slot (bit = 0) 
        3. 📋 Return total count                             

        Visual Count Example:
        -----------------------------------------
         Slots: 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5             
         Bits:  1 1 0 1 1 0 1 0 1 1 0 1 1 1 1 1             
         Free:    X   X   X   X     X                        
         Count:   1   2   3   4     5                        
         Result: 5 free slots out of 16 total             
         Usage: 11/16 = 68.75% utilized                   
         Free:  5/16 = 31.25% available                   
        -----------------------------------------

        Returns:
            📊 Number of free slots available
        """
        count = 0
        for slot in range(self.num_slots):
            if not self.is_slot_used(slot):
                count += 1
        return count
