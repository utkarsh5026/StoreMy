import math
from typing import Optional, Iterator, TYPE_CHECKING
from app.primitives import TransactionId
from app.storage.heap.heap_page_id import HeapPageId
from .bitmap_slot import BitmapSlotManager
from app.primitives import RecordId
from app.storage.interfaces import Page

if TYPE_CHECKING:
    from app.core.tuple import Tuple, TupleDesc


class HeapPage(Page):
    """
    🗃️ Implementation of a database page for heap files with full header support 🗃️

    💡 A heap page stores tuples in a simple unordered format with a bitmap header
    to track which slots are occupied. This follows the SimpleDB page format.

    📄 Page Layout (4KB total):
    ---------------------------------------------------------------
     HEADER Section    │ TUPLE DATA Section      │ PADDING      
     (Bitmap)          │ (Fixed-size slots)      │ (Zeros)      
                       │                         │              
     Bit per slot      │ Slot 0 │ Slot 1 │ ... │ Unused       
     0=free, 1=used    │ Tuple  │ Tuple  │     │ space        
    ---------------------------------------------------------------
     ← header_size →   ← num_slots * tuple_size → ← padding →   
                       ←────────── PAGE_SIZE (4096) ──────────→   

    🔢 Size Calculations:

     📐 Header size = ceil(num_slots / 8) bytes                  
     📦 Tuple section = num_slots × tuple_size bytes            
     🗂️ Total used = header_size + tuple_section                 
     🔳 Padding = PAGE_SIZE - total_used                         

    ------------------------------------------------------------
    🧮 Slot Calculation Formula:
     num_slots = floor((PAGE_SIZE * 8) / (tuple_size * 8 + 1))  

     Explanation:                                                
     • PAGE_SIZE * 8: Total bits available                      
     • tuple_size * 8: Bits needed for tuple data               
     • +1: One header bit per tuple slot                        

     Example with 100-byte tuples:                               
     num_slots = (4096 * 8) / (100 * 8 + 1) = 32768 / 801 = 40 
     ------------------------------------------------------------

    🎯 Key Features:
    ✅ Fixed-size tuple slots for predictable layout
    🗂️ Bitmap header for efficient slot tracking  
    💾 Disk-optimized serialization format
    🔄 Transaction support with before-images
    🏃‍♂️ Fast iteration over occupied slots
    """

    PAGE_SIZE_IN_BYTES = 4096  # Standard 4KB page size

    def __init__(self,
                 page_id: HeapPageId,
                 data: Optional[bytes] = None,
                 tuple_desc: Optional['TupleDesc'] = None):
        """
        🏗️ Initialize a HeapPage 🏗️

        Two modes of initialization:
        ------------------------------------------------------------
        📖 LOAD MODE: data provided                             
        ├─ Deserialize existing page from disk               
        ├─ Extract header bitmap                              
        ├─ Reconstruct tuples from slots                     
        └─ Set up internal structures                        

        🆕 CREATE MODE: tuple_desc provided                     
        ├─ Calculate optimal slot layout                     
        ├─ Initialize empty bitmap                           
        ├─ Create empty tuple array                          
        └─ Prepare for tuple insertion                       
        ------------------------------------------------------------

        Args:
            🆔 page_id: Unique identifier for this page
            💾 data: Raw page data to deserialize (if reading from disk)
            📋 tuple_desc: Schema information (if creating new page)
        """
        self.page_id = page_id
        self.dirty_transaction: Optional['TransactionId'] = None
        self.before_image_data: Optional[bytes] = None

        if data is not None:
            self.deserialize(data)
        else:
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
        """
        🚀 Initialize an empty page with no tuples 🚀

        Initialization Process:
        ------------------------------------------------------------
        1. 🧮 Calculate maximum slots for tuple size        
         Formula: (PAGE_SIZE * 8) / (tuple_size * 8 + 1) 

        2. 🗂️ Create bitmap slot manager                    
         All bits start as 0 (free)                      

        3. 📋 Initialize tuple array                        
         [None, None, None, ..., None]                   
         ← num_slots elements →                          

        Empty Page Visualization:
        ------------------------------------------------------------
        HEADER: 00000000 00000000 ... (all slots free)     
        SLOTS:  [None]   [None]   ... (all slots empty)    
        STATUS: Ready for tuple insertion! 🎯              
        ------------------------------------------------------------
        """
        self.num_slots = self._calculate_num_slots()
        self.slot_manager = BitmapSlotManager(self.num_slots)
        self.tuples: list[Optional['Tuple']] = [None] * self.num_slots

    def _calculate_num_slots(self) -> int:
        """
        🧮 Calculate the maximum number of tuple slots on this page 🧮

        📐 Uses SimpleDB formula for optimal space utilization:

        Formula Breakdown:
        ------------------------------------------------------------
        num_slots = floor((PAGE_SIZE * 8) / (tuple_size * 8 + 1)) 

        Bit Budget Analysis:                                
        • Total page bits: PAGE_SIZE * 8                   
        • Per slot cost: tuple_size * 8 + 1                
        ├─ tuple_size * 8: tuple data bits               
        └─ +1: header bit for slot tracking              
        ------------------------------------------------------------

        Real Example (100-byte tuples in 4KB page):
        ------------------------------------------------------------
        PAGE_SIZE = 4096 bytes = 32,768 bits               
        tuple_size = 100 bytes = 800 bits                  
        Cost per slot = 800 + 1 = 801 bits                 
        Max slots = 32,768 ÷ 801 = 40.9 → 40 slots

        Space usage:                                        
        • Header: ceil(40/8) = 5 bytes                     
        • Tuples: 40 × 100 = 4000 bytes                   
        • Used: 4005 bytes                                 
        • Free: 91 bytes (2.2% waste) ✅                   
        ------------------------------------------------------------

        Returns:
            🔢 Maximum number of slots that can fit on this page
        """
        tuple_size = self.tuple_desc.get_size()
        return (self.PAGE_SIZE_IN_BYTES * 8) // (tuple_size * 8 + 1)

    def _calculate_header_size(self) -> int:
        """
        📏 Calculate header size in bytes 📏

        🔢 Formula: ceil(num_slots / 8)
        We need one bit per slot, rounded up to nearest byte.

        Header Size Examples:
        ------------------------------------------------------------
        Slots → Header Size (bytes)                         
        1-8  → 1 byte   (bits: ●●●●●●●● )                
        9-16 → 2 bytes  (bits: ●●●●●●●● ●●●●●●●● )        
        17-24 → 3 bytes  (bits: ●●●●●●●● ●●●●●●●● ●●●●●●●● ) 
        25-32 → 4 bytes  ...and so on              
        ------------------------------------------------------------

        Memory Layout:
        ------------------------------------------------------------
        40 slots example:                                   
        Header = ceil(40/8) = ceil(5.0) = 5 bytes          

        Byte 0: [●●●●●●●●] slots 0-7                       
        Byte 1: [●●●●●●●●] slots 8-15                      
        Byte 2: [●●●●●●●●] slots 16-23                     
        Byte 3: [●●●●●●●●] slots 24-31                     
        Byte 4: [●●●●●●●●] slots 32-39                     
        └─ 8 bits  └─ only 8 bits used             
        ------------------------------------------------------------

        Returns:
            📏 Header size in bytes
        """
        return math.ceil(self.num_slots / 8)

    def deserialize(self, data: bytes) -> None:
        """
        🔄 Reconstruct page state from raw bytes 🔄

        📖 This method follows the SimpleDB page format to rebuild the page
        from disk data, including the header bitmap and all tuple objects.

        Deserialization Process:
        ------------------------------------------------------------
        1. 🔍 Validate data size (must be exactly 4KB)      

        2. 📐 Calculate layout parameters                    
        ├─ num_slots (from tuple size)                  
        ├─ header_size (bits → bytes)                   
        └─ tuple_size (from schema)                     

        3. 🗂️ Extract header bitmap                         
        data[0:header_size] → BitmapSlotManager          

        4. 📦 Extract tuple data section                    
        data[header_size:header_size + slots×tuple_size] 

        5. 🔄 Reconstruct individual tuples                 
        For each used slot: deserialize tuple object    

        Memory Layout During Deserialization:
        ------------------------------------------------------------
        Raw Data (4096 bytes):                              
        [HEADER|TUPLE0|TUPLE1|TUPLE2|...|TUPLEN|PADDING]    
          ↑      ↑                           ↑    ↑         
          │      │                           │    │         
          │      tuple_data_start            │    │         
          │                                  │    │         
          header_data                        │    │         
                                   tuple_data_end │         
                                                  │         
                                            padding         
        ------------------------------------------------------------

        Args:
            💾 data: Raw page data from disk

        Raises:
            ❌ ValueError: If data is invalid or catalog provider is missing
        """
        if len(data) != self.PAGE_SIZE_IN_BYTES:
            raise ValueError(
                f"Page data must be exactly {self.PAGE_SIZE_IN_BYTES} bytes")

        self.num_slots = self._calculate_num_slots()
        header_size = self._calculate_header_size()
        tuple_size = self.tuple_desc.get_size()

        header_data = data[:header_size]
        self.slot_manager = BitmapSlotManager(self.num_slots, header_data)

        tuple_data_start = header_size
        tuple_data_end = tuple_data_start + (self.num_slots * tuple_size)
        tuple_data_section = data[tuple_data_start:tuple_data_end]

        self.tuples = [None] * self.num_slots

        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot):
                slot_start = slot * tuple_size
                slot_end = slot_start + tuple_size
                slot_data = tuple_data_section[slot_start:slot_end]

                from app.core.tuple import Tuple
                tuple_obj = Tuple.deserialize(slot_data, self.tuple_desc)

                record_id = RecordId(self.page_id, slot)
                tuple_obj.set_record_id(record_id)

                self.tuples[slot] = tuple_obj

    def serialize(self) -> bytes:
        """
        💾 Serialize this page to bytes for disk storage 💾

        🔄 Format follows SimpleDB layout for compatibility and efficiency.
        All data is packed tightly with no gaps between sections.

        Serialization Process:
        ------------------------------------------------------------
        1. 🗂️ Serialize header bitmap                       
        Convert BitManager to raw bytes                  

        2. 📦 Serialize tuple data                          
        For each slot:                                   
        ├─ If occupied: serialize tuple → bytes          
        └─ If empty: write zeros (tuple_size bytes)     

        3. 🔳 Add padding to reach PAGE_SIZE                
        Fill remaining space with zeros                  

        Output Format:
        ------------------------------------------------------------
        Byte Layout:                                        
        ┌─────────┬─────────────────────┬─────────────────┐ 
        │ HEADER  │    TUPLE DATA       │    PADDING      │ 
        │ (bitmap)│   (fixed slots)     │    (zeros)      │ 
        └─────────┴─────────────────────┴─────────────────┘ 
            ↑        ↑                     ↑                   
            0        header_size           data_end            

        Size validation:                                    
        ✅ Total size MUST equal PAGE_SIZE_IN_BYTES (4096)  
        ------------------------------------------------------------

        Tuple Slot Handling:
        ------------------------------------------------------------
        Slot 0: [OCCUPIED] → serialize tuple data           
        Slot 1: [EMPTY   ] → write zeros (tuple_size)      
        Slot 2: [OCCUPIED] → serialize tuple data           
        Slot 3: [EMPTY   ] → write zeros (tuple_size)      
        ...                                                 
        ------------------------------------------------------------

        Returns:
            💾 Serialized page data ready for disk storage
        """
        data = bytearray()

        header_data = bytes(self.slot_manager.bitmap)
        data.extend(header_data)

        tuple_size = self.tuple_desc.get_size()
        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot) and self.tuples[slot] is not None:
                tuple_data = self.tuples[slot].serialize()
                if len(tuple_data) != tuple_size:
                    raise RuntimeError(
                        f"Tuple size mismatch: expected {tuple_size}, got {len(tuple_data)}")
                data.extend(tuple_data)
            else:
                data.extend(b'\x00' * tuple_size)

        current_size = len(data)
        if current_size > self.PAGE_SIZE_IN_BYTES:
            raise RuntimeError(
                f"Page data too large: {current_size} > {self.PAGE_SIZE_IN_BYTES}")

        padding_size = self.PAGE_SIZE_IN_BYTES - current_size
        data.extend(b'\x00' * padding_size)

        return bytes(data)

    def insert_tuple(self, tuple_obj: 'Tuple') -> Optional[int]:
        """
        ➕ Insert a tuple into the page ➕

        📝 This method finds the first available slot and places the tuple there,
        updating both the tuple array and the bitmap header.

        Insertion Process:
        ------------------------------------------------------------
        1. ✅ Validate tuple schema matches page schema      
        tuple.schema == page.schema                      

        2. 🔍 Find first free slot                          
        Scan bitmap: 0 = free, 1 = used                 

        3. 📍 Insert tuple at found slot                    
        tuples[slot] = tuple_obj                         

        4. 🗂️ Update bitmap header                          
        set_slot_used(slot, True)                       

        5. 🔗 Set tuple's record ID                         
        tuple.record_id = RecordId(page_id, slot)       
        ------------------------------------------------------------

        Before/After Example:
        ------------------------------------------------------------
        BEFORE insertion:                                   
        Bitmap: [1,1,0,1,0,0,1,1] (slots 2,4,5 free)      
        Tuples: [T1,T2,∅,T3,∅,∅,T4,T5]                     
                       ↑                                    
                   First free                              

        AFTER inserting new tuple T6:                       
        Bitmap: [1,1,1,1,0,0,1,1] (slot 2 now used)       
        Tuples: [T1,T2,T6,T3,∅,∅,T4,T5]                    
                       ↑                                    
                    T6 inserted                              
                 RecordId=(page_id, 2)                       
        ------------------------------------------------------------

        Args:
            📦 tuple_obj: Tuple to insert

        Returns:
            🎯 Slot number where tuple was inserted, or None if the page is full

        Raises:
            ❌ ValueError: If tuple schema doesn't match page schema
            ❌ RuntimeError: If no empty slots available
        """
        if tuple_obj.get_tuple_desc() != self.tuple_desc:
            raise ValueError("Tuple schema doesn't match page schema")

        slot = self.slot_manager.find_free_slot()
        if slot is None:
            raise RuntimeError("No empty slots available")

        self.tuples[slot] = tuple_obj
        self.slot_manager.set_slot_used(slot, True)

        record_id = RecordId(self.page_id, slot)
        tuple_obj.set_record_id(record_id)

        return slot

    def delete_tuple(self, tuple_obj: 'Tuple') -> bool:
        """
        🗑️ Delete a tuple from the page 🗑️

        🔍 This method removes a tuple by clearing its slot and updating
        the bitmap, but only if the tuple actually belongs to this page.

        Deletion Process:
        ------------------------------------------------------------
        1. 🔍 Validate tuple belongs to this page           
        Check: record_id.page_id == this.page_id        

        2. 📍 Extract slot number from record ID            
        slot = record_id.tuple_number                    

        3. ✅ Verify slot is valid and occupied             
        0 ≤ slot < num_slots AND bitmap[slot] = 1       

        4. 🗑️ Remove tuple from slot                        
        tuples[slot] = None                             

        5. 🗂️ Update bitmap header                          
        set_slot_used(slot, False)                      

        6. 🔗 Clear tuple's record ID                       
        tuple.record_id = None                          

        Before/After Example:
        ------------------------------------------------------------
        BEFORE deletion (deleting T6 from slot 2):          
        Bitmap: [1,1,1,1,0,0,1,1] (slot 2 occupied)        
        Tuples: [T1,T2,T6,T3,∅,∅,T4,T5]                    
                ↑                                    
           Target tuple                              

        AFTER deletion:                                     
        Bitmap: [1,1,0,1,0,0,1,1] (slot 2 now free)       
        Tuples: [T1,T2,∅,T3,∅,∅,T4,T5]                     
                       ↑                                    
                    Deleted                                 
         T6.record_id = None                         
        ------------------------------------------------------------

        Args:
            🗑️ tuple_obj: Tuple to delete

        Returns:
            ✅ True if tuple was found and deleted, False otherwise
        """
        record_id = tuple_obj.get_record_id()
        if record_id is None or record_id.get_page_id() != self.page_id:
            return False

        slot = record_id.get_tuple_number()
        if slot >= self.num_slots or not self.slot_manager.is_slot_used(slot):
            return False

        self.tuples[slot] = None
        self.slot_manager.set_slot_used(slot, False)

        tuple_obj.set_record_id(None)

        return True

    def get_tuple(self, slot: int) -> Optional['Tuple']:
        """
        🔍 Get tuple at specific slot 🔍

        Slot Access Logic:
        ------------------------------------------------------------
        Input: slot number                                  
        ↓                                                 
        ✅ Validate: 0 ≤ slot < num_slots                   
        ↓                                                 
        🗂️ Check bitmap: is_slot_used(slot)?               
        ↓                                                 
        📦 Return: tuples[slot] or None
        ------------------------------------------------------------

        Args:
            🎯 slot: Slot number to retrieve

        Returns:
            📦 Tuple object if slot is valid and occupied, None otherwise
        """
        if slot >= self.num_slots or not self.slot_manager.is_slot_used(slot):
            return None
        return self.tuples[slot]

    def iterator(self) -> Iterator['Tuple']:
        """
        🔄 Iterate over all tuples in used slots 🔄

        Iteration Example:
        ------------------------------------------------------------
        Slots: [T1, T2, ∅, T3, ∅, ∅, T4, T5]              
        Yield:  T1  T2      T3          T4  T5    

        Skips: Empty slots (∅) are automatically skipped   
        ------------------------------------------------------------

        Yields:
            📦 Each tuple object in occupied slots
        """
        for slot in range(self.num_slots):
            if self.slot_manager.is_slot_used(slot) and self.tuples[slot] is not None:
                yield self.tuples[slot]

    def get_num_empty_slots(self) -> int:
        """
        📊 Return number of empty slots 📊

        Free Space Calculation:
        ------------------------------------------------------------
        🗂️ Delegates to BitmapSlotManager                   
        🔢 Counts bits set to 0 in the bitmap               
        📈 Useful for capacity planning                     
        ------------------------------------------------------------

        Returns:
            📊 Number of available slots for new tuples
        """
        return self.slot_manager.get_free_slot_count()

    def get_id(self) -> HeapPageId:
        """
        🆔 Get page identifier 🆔

        Returns:
            🆔 Unique page identifier
        """
        return self.page_id

    def is_dirty(self) -> Optional['TransactionId']:
        """
        🔍 Check if page has been modified 🔍

        Dirty State Logic:
        ------------------------------------------------------------
        🔍 If dirty_transaction is not None:              
        ├─ Page has been modified                       
        └─ Return transaction ID that modified it       

        🧹 If dirty_transaction is None:                   
        ├─ Page is clean (matches disk)                 
        └─ Return None   
        ------------------------------------------------------------

        Returns:
            🆔 Transaction ID if dirty, None if clean
        """
        return self.dirty_transaction

    def mark_dirty(self, dirty: bool, transaction_id: Optional['TransactionId']) -> None:
        """
        🏷️ Mark page as dirty or clean 🏷️

        State Transition:
        ┌─────────────────────────────────────────────────────┐
        │ 🔴 mark_dirty(True, tid):                           │
        │    ├─ dirty_transaction = tid                       │
        │    ├─ Page needs to be written to disk             │
        │    └─ Used by transaction when modifying page      │
        │                                                     │
        │ 🧹 mark_dirty(False, None):                         │
        │    ├─ dirty_transaction = None                      │
        │    ├─ Page is clean (matches disk state)           │
        │    └─ Used after successful write to disk          │
        └─────────────────────────────────────────────────────┘

        Args:
            🚦 dirty: True if page is modified, False if clean
            🆔 transaction_id: ID of transaction making changes (if dirty)
        """
        if dirty:
            self.dirty_transaction = transaction_id
        else:
            self.dirty_transaction = None

    def set_before_image(self) -> None:
        """
        📸 Save current page state as before image for recovery 📸

        Recovery Support:
        ------------------------------------------------------------
        💾 Captures complete page state in bytes            
        🔄 Used for transaction rollback                    
        ⚡ Enables undo operations                           
        🛡️ Critical for ACID compliance                     
        ------------------------------------------------------------

        Process:
        ------------------------------------------------------------
        Current Page State → serialize() → Raw Bytes    
                                    ↓          
                        Store as before_image  
        ------------------------------------------------------------
        """
        self.before_image_data = self.serialize()

    def get_before_image(self) -> 'HeapPage':
        """
        ⏪ Get page state before modifications ⏪

        Recovery Operation:
        ------------------------------------------------------------
        📸 Uses stored before_image_data                    
        🏗️ Reconstructs HeapPage from raw bytes             
        🔄 Returns page as it was before transaction        
        💡 Used for transaction rollback/undo              
        ------------------------------------------------------------

        Rollback Process:
        ------------------------------------------------------------
        Transaction T1 modifies page:                       
        Before: [T1, T2, ∅, T3] → set_before_image()       
        After:  [T1, T2, T4, T3] → (current state)         

        On rollback: get_before_image() returns:           
        Restore: [T1, T2, ∅, T3] → (original state)        
        ------------------------------------------------------------

        Returns:
            📸 HeapPage representing state before modifications

        Raises:
            ❌ RuntimeError: If no before image is available
        """
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
        """
        🔳 Create empty page data filled with zeros 🔳

        Empty Page Structure:
        ------------------------------------------------------------
        💾 Returns 4096 bytes of zeros                      
        🛠️ Used for initializing new page files             
        🔄 Can be passed to HeapPage constructor             
        ------------------------------------------------------------

        Returns:
            💾 4KB of zero bytes ready for page initialization
        """
        return b'\x00' * HeapPage.PAGE_SIZE_IN_BYTES

    def __str__(self) -> str:
        """
        🖨️ String representation of the page 🖨️

        Output Format: "HeapPage(id=<page_id>, slots=<used>/<total>)"

        Example: "HeapPage(id=HeapPageId(1,5), slots=23/40)"
        """
        used_slots = self.num_slots - self.slot_manager.get_free_slot_count()
        return f"HeapPage(id={self.page_id}, slots={used_slots}/{self.num_slots})"
