package heap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

const (
	// SlotPointerSize is the size of each slot pointer (4 bytes: 2 for offset, 2 for length)
	SlotPointerSize = 4
	// MaxTupleSize is the maximum size a tuple can be (limited by uint16)
	MaxTupleSize = 65535
)

// SlotPointer represents a pointer to a tuple within a page (PostgreSQL-style)
// Each slot has an offset (where tuple starts) and length (how long it is)
// If offset is 0, the slot is considered empty/deleted
type SlotPointer struct {
	Offset primitives.SlotID // Offset from start of page (0 = empty slot)
	Length uint16            // Length of tuple data in bytes
}

// HeapPage represents a single page in a heap file and implements the page.Page interface.
// It uses a PostgreSQL-style slotted page structure with pointer array to track tuples.
//
// Page Layout (inspired by PostgreSQL):
//   - Slot Pointer Array: Array of (offset, length) pairs, one per slot (grows from start)
//   - Free Space: Available space in the middle
//   - Tuple Data: Actual tuple data (grows from end, backward)
//
// Benefits:
//   - Stable RecordIDs even after page compaction
//   - Variable-length tuple support
//   - Efficient space reclamation
//   - Better update performance (can reuse slots)
type HeapPage struct {
	pageID       *page.PageDescriptor
	tupleDesc    *tuple.TupleDescription
	tuples       []*tuple.Tuple    // In-memory tuple cache (indexed by slot number)
	slotPointers []SlotPointer     // Pointer array (offset, length) for each slot
	numSlots     primitives.SlotID // Maximum number of slots
	freeSpacePtr uint16            // Points to start of free space
	dirtier      *primitives.TransactionID
	oldData      []byte // Before-image for rollback
	mutex        sync.RWMutex
}

// NewEmptyHeapPage creates a brand new, empty HeapPage with the given PageDescriptor and TupleDescription.
// The page data is initialized to all zeroes (empty), and the page is formatted using the provided tuple layout.
// This is commonly used to allocate new pages before inserting any tuples.
//
// Parameters:
//   - pid: The PageDescriptor that uniquely identifies this page
//   - td:  The TupleDescription specifying the schema/format used for the tuples in this page
//
// Returns:
//   - *HeapPage: A pointer to the newly created, empty HeapPage
//   - error:     An error if creation failed (shouldn't happen with correct page size)
func NewEmptyHeapPage(pid *page.PageDescriptor, td *tuple.TupleDescription) (*HeapPage, error) {
	return NewHeapPage(pid, make([]byte, page.PageSize), td)
}

// NewHeapPage creates a new HeapPage by deserializing raw page data.
// It calculates the optimal number of slots based on tuple size and initializes
// the slot pointer array and tuple array from the provided data.
func NewHeapPage(pid *page.PageDescriptor, data []byte, td *tuple.TupleDescription) (*HeapPage, error) {
	if len(data) != page.PageSize {
		return nil, fmt.Errorf("invalid page data size: expected %d, got %d", page.PageSize, len(data))
	}

	hp := &HeapPage{
		pageID:    pid,
		tupleDesc: td,
		oldData:   make([]byte, page.PageSize),
	}

	hp.numSlots = hp.getNumTuples()
	hp.slotPointers = make([]SlotPointer, hp.numSlots)
	hp.tuples = make([]*tuple.Tuple, hp.numSlots)
	hp.freeSpacePtr = uint16(hp.getHeaderSize())

	if err := hp.parsePageData(data); err != nil {
		return nil, err
	}

	copy(hp.oldData, data)
	return hp, nil
}

// GetNumEmptySlots returns the count of unoccupied tuple slots on this page.
// This is useful for determining if the page has capacity for insertions.
//
// Returns:
//   - int: Number of empty slots available for new tuples
func (hp *HeapPage) GetNumEmptySlots() primitives.SlotID {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.getNumEmptySlots()
}

// GetID returns the unique page identifier for this heap page.
func (hp *HeapPage) GetID() *page.PageDescriptor {
	return hp.pageID
}

// IsDirty returns the transaction that last modified this page.
// A nil return indicates the page is clean (not modified in current transaction).
func (hp *HeapPage) IsDirty() *primitives.TransactionID {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.dirtier
}

// MarkDirty marks this page as dirty or clean for a specific transaction.
// This is typically called by the buffer pool when a page is modified or flushed.
func (hp *HeapPage) MarkDirty(dirty bool, tid *primitives.TransactionID) {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	if dirty {
		hp.dirtier = tid
	} else {
		hp.dirtier = nil
	}
}

// GetPageData serializes the entire page into a byte array suitable for disk storage.
// The returned data includes the slot pointer array, tuple data, and padding to reach page.PageSize.
//
// Layout:
//
//	[SlotPointer0][SlotPointer1]...[SlotPointerN][FreeSpace][...TupleData...]
func (hp *HeapPage) GetPageData() []byte {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	pageData := make([]byte, page.PageSize)

	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		offset := int(i) * SlotPointerSize
		binary.LittleEndian.PutUint16(pageData[offset:], uint16(hp.slotPointers[i].Offset))
		binary.LittleEndian.PutUint16(pageData[offset+2:], hp.slotPointers[i].Length)
	}

	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		if hp.slotPointers[i].Offset == 0 || hp.tuples[i] == nil {
			continue // Empty slot
		}

		tupleOffset := hp.slotPointers[i].Offset
		buffer := bytes.NewBuffer(pageData[tupleOffset:tupleOffset])

		for j := primitives.ColumnID(0); j < hp.tupleDesc.NumFields(); j++ {
			field, err := hp.tuples[i].GetField(j)
			if err != nil {
				continue
			}
			_ = field.Serialize(buffer)
		}
	}

	return pageData
}

// GetBeforeImage returns a page containing the state before the current transaction's modifications.
// This is used for rollback operations in transaction management.
func (hp *HeapPage) GetBeforeImage() page.Page {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	beforePage, _ := NewHeapPage(hp.pageID, hp.oldData, hp.tupleDesc)
	return beforePage
}

// SetBeforeImage captures the current page state as the before-image.
// This should be called before the first modification in a transaction to enable rollback.
//
// Usage:
//   - Call once at the beginning of a transaction before any modifications
//   - Typically invoked by the buffer pool or transaction manager
func (hp *HeapPage) SetBeforeImage() {
	hp.oldData = hp.GetPageData()
}

// AddTuple inserts a tuple into the first available empty slot on this page.
// The tuple's RecordID is set to identify its location on this page.
// Tuples are allocated from the end of the page, growing backward toward the pointer array.
//
// Thread-safe: Uses write lock for concurrent access.
//
// Parameters:
//   - t: Tuple to insert (must have matching schema)
//
// Returns:
//   - error: If schema mismatch, page full, or slot allocation fails
//
// Errors:
//   - Schema mismatch between tuple and page
//   - No empty slots available
//   - Not enough contiguous free space
func (hp *HeapPage) AddTuple(t *tuple.Tuple) error {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	if !t.TupleDesc.Equals(hp.tupleDesc) {
		return fmt.Errorf("tuple schema does not match page schema")
	}

	slotIndex, err := hp.findFirstEmptySlot()
	if err != nil {
		return fmt.Errorf("no empty slot available: %w", err)
	}

	tupleSize := hp.tupleDesc.GetSize()
	if tupleSize > MaxTupleSize {
		return fmt.Errorf("tuple size %d exceeds maximum %d", tupleSize, MaxTupleSize)
	}

	if !hp.hasSpaceForTuple(uint16(tupleSize)) {
		return fmt.Errorf("no space left on this page")
	}

	newTupleOffset := hp.freeSpacePtr
	hp.freeSpacePtr += uint16(tupleSize)

	hp.slotPointers[slotIndex] = SlotPointer{
		Offset: primitives.SlotID(newTupleOffset),
		Length: uint16(tupleSize),
	}

	hp.tuples[slotIndex] = t
	t.RecordID = tuple.NewTupleRecordID(hp.pageID, slotIndex)
	return nil
}

// DeleteTuple removes a tuple from this page by invalidating its slot pointer.
// The tuple's RecordID is set to nil after successful deletion.
// Note: This does not reclaim space immediately. Call Compact() to defragment the page.
func (hp *HeapPage) DeleteTuple(t *tuple.Tuple) error {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	recordID := t.RecordID
	if recordID == nil {
		return fmt.Errorf("tuple has no record ID")
	}

	if !recordID.PageID.Equals(hp.pageID) {
		return fmt.Errorf("tuple is not on this page")
	}

	slotIndex := recordID.TupleNum

	if !hp.isSlotUsed(slotIndex) {
		return fmt.Errorf("tuple slot %d is already empty", slotIndex)
	}

	hp.slotPointers[slotIndex] = SlotPointer{Offset: 0, Length: 0}
	hp.tuples[slotIndex] = nil
	t.RecordID = nil
	return nil
}

// GetTuples returns all non-empty tuples stored on this page.
// Empty slots (marked as unused in header) are excluded from the result.
func (hp *HeapPage) GetTuples() []*tuple.Tuple {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	tuples := make([]*tuple.Tuple, 0, hp.numSlots-hp.getNumEmptySlots())
	for _, t := range hp.tuples {
		if t != nil {
			tuples = append(tuples, t)
		}
	}

	return tuples
}

// GetTupleAt returns the tuple at the specified slot index, or nil if the slot is empty.
func (hp *HeapPage) GetTupleAt(idx primitives.SlotID) (*tuple.Tuple, error) {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	if idx >= hp.numSlots {
		return nil, fmt.Errorf("slot index %d out of bounds", idx)
	}

	return hp.tuples[idx], nil
}

// getNumTuples calculates the maximum number of tuple slots that fit on a page.
// This accounts for both tuple data size and the slot pointer array overhead.
//
// Formula: floor((PageSize) / (tupleSize + SlotPointerSize))
//   - Each slot needs SlotPointerSize bytes for the pointer + tupleSize bytes for data
//
// Returns:
//   - int: Maximum number of tuple slots for this page's schema
func (hp *HeapPage) getNumTuples() primitives.SlotID {
	tupleSize := hp.tupleDesc.GetSize()
	return primitives.SlotID(page.PageSize) / primitives.SlotID(tupleSize+SlotPointerSize)
}

// getHeaderSize calculates the number of bytes needed for the slot pointer array.
// Each slot requires SlotPointerSize (4) bytes: 2 for offset, 2 for length.
//
// Returns:
//   - int: Size in bytes of the slot pointer array
func (hp *HeapPage) getHeaderSize() primitives.SlotID {
	return hp.getNumTuples() * SlotPointerSize
}

// parsePageData deserializes raw page bytes into the slot pointer array and tuple array.
// Called during page initialization to reconstruct page state from disk.
//
// Layout:
//
//	[SlotPointer0][SlotPointer1]...[SlotPointerN][FreeSpace][...TupleData...]
func (hp *HeapPage) parsePageData(data []byte) error {
	maxOffset := uint16(0)
	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		offset := int(i) * SlotPointerSize
		if offset+SlotPointerSize > len(data) {
			return fmt.Errorf("invalid page data: insufficient data for slot pointers")
		}

		hp.slotPointers[i].Offset = primitives.SlotID(binary.LittleEndian.Uint16(data[offset:]))
		hp.slotPointers[i].Length = binary.LittleEndian.Uint16(data[offset+2:])

		if hp.slotPointers[i].Offset != 0 {
			endOffset := uint16(hp.slotPointers[i].Offset) + hp.slotPointers[i].Length
			if endOffset > maxOffset {
				maxOffset = endOffset
			}
		}
	}

	hp.freeSpacePtr = max(maxOffset, uint16(hp.getHeaderSize()))

	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		if hp.slotPointers[i].Offset == 0 {
			continue // Empty slot
		}

		tupleOffset := hp.slotPointers[i].Offset
		tupleLength := hp.slotPointers[i].Length

		if int(uint16(tupleOffset)+tupleLength) > len(data) {
			return fmt.Errorf("invalid tuple at slot %d: offset %d + length %d exceeds page size",
				i, tupleOffset, tupleLength)
		}

		tupleData := data[tupleOffset : uint16(tupleOffset)+tupleLength]
		reader := bytes.NewReader(tupleData)

		t, err := readTuple(reader, hp.tupleDesc)
		if err != nil {
			return fmt.Errorf("failed to read tuple at slot %d: %v", i, err)
		}

		t.RecordID = tuple.NewTupleRecordID(hp.pageID, i)
		hp.tuples[i] = t
	}

	return nil
}

// getNumEmptySlots internal implementation that counts unoccupied slots.
// Must be called with lock already held (does not acquire lock).
func (hp *HeapPage) getNumEmptySlots() primitives.SlotID {
	emptySlots := primitives.SlotID(0)
	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		if hp.slotPointers[i].Offset == 0 {
			emptySlots++
		}
	}
	return emptySlots
}

// isSlotUsed checks if a slot is marked as occupied in the slot pointer array.
// A slot is considered used if its offset is non-zero.
func (hp *HeapPage) isSlotUsed(idx primitives.SlotID) bool {
	if idx >= hp.numSlots {
		return false
	}
	return hp.slotPointers[idx].Offset != 0
}

// findFirstEmptySlot scans the slot pointer array for the first unoccupied slot.
func (hp *HeapPage) findFirstEmptySlot() (primitives.SlotID, error) {
	var i primitives.SlotID
	for i = 0; i < hp.numSlots; i++ {
		if !hp.isSlotUsed(i) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no slot found")
}

// hasSpaceForTuple checks if there is enough contiguous free space for a tuple of the given size.
// Free space is between freeSpacePtr and the end of the page.
func (hp *HeapPage) hasSpaceForTuple(tupleSize uint16) bool {
	return uint32(hp.freeSpacePtr)+uint32(tupleSize) <= uint32(page.PageSize)
}

// readTuple deserializes a single tuple from a byte stream.
// Used during page parsing to reconstruct tuples from disk.
//
// Parameters:
//   - reader: Byte stream positioned at start of tuple data
//   - td: Schema descriptor for the tuple
//
// Returns:
//   - *tuple.Tuple: Parsed tuple with all fields populated
//   - error: If field parsing fails or reader encounters error
func readTuple(reader io.Reader, td *tuple.TupleDescription) (*tuple.Tuple, error) {
	t := tuple.NewTuple(td)

	for j := primitives.ColumnID(0); j < td.NumFields(); j++ {
		fieldType, err := td.TypeAtIndex(j)
		if err != nil {
			return nil, err
		}

		field, err := types.ParseField(reader, fieldType)
		if err != nil {
			return nil, err
		}

		if err := t.SetField(j, field); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// Compact defragments the page by moving all tuples together to eliminate gaps.
// This reclaims space left by deleted tuples, making it available for new insertions.
//
// Key benefits:
//   - Reclaims fragmented free space after deletions
//   - RecordIDs remain stable (slot numbers don't change, only internal offsets)
//   - Indexes continue to work correctly
//
// Thread-safe: Uses write lock for concurrent access.
//
// Returns:
//   - int: Number of bytes reclaimed by compaction
func (hp *HeapPage) Compact() int {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	// Serialize all tuples to a temporary buffer
	type tupleData struct {
		slotIndex primitives.SlotID
		data      []byte
	}

	var activeTuples []tupleData
	for i := primitives.SlotID(0); i < hp.numSlots; i++ {
		if hp.slotPointers[i].Offset == 0 || hp.tuples[i] == nil {
			continue // Skip empty slots
		}

		// Serialize tuple
		buffer := &bytes.Buffer{}
		for j := primitives.ColumnID(0); j < hp.tupleDesc.NumFields(); j++ {
			field, err := hp.tuples[i].GetField(j)
			if err != nil {
				continue
			}
			_ = field.Serialize(buffer)
		}

		activeTuples = append(activeTuples, tupleData{
			slotIndex: i,
			data:      buffer.Bytes(),
		})
	}

	// Calculate space before compaction
	spaceBefore := int(page.PageSize) - int(hp.freeSpacePtr)

	// Reset free space pointer to start right after header
	hp.freeSpacePtr = uint16(hp.getHeaderSize())

	// Repack tuples contiguously from the free space pointer
	for _, td := range activeTuples {
		tupleSize := uint16(len(td.data))

		// Update slot pointer to new location
		hp.slotPointers[td.slotIndex] = SlotPointer{
			Offset: primitives.SlotID(hp.freeSpacePtr),
			Length: tupleSize,
		}

		// Advance free space pointer
		hp.freeSpacePtr += tupleSize
	}

	// Calculate space after compaction
	spaceAfter := int(page.PageSize) - int(hp.freeSpacePtr)
	spaceReclaimed := spaceAfter - spaceBefore

	return spaceReclaimed
}

// GetTupleDesc returns the tuple description (schema) for this page
// This is useful for recovery operations that need to deserialize tuples
func (hp *HeapPage) GetTupleDesc() *tuple.TupleDescription {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.tupleDesc
}
