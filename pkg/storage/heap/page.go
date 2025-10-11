package heap

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

const (
	// BitsPerByte is the number of bits in a byte (8)
	BitsPerByte = 8
)

// HeapPage represents a single page in a heap file and implements the page.Page interface.
// It uses a slotted page structure with a bitmap header to track occupied tuple slots.
//
// Page Layout:
//   - Header (variable size): Bitmap tracking which slots contain tuples (1 bit per slot)
//   - Tuple data (remainder): Fixed-size slots for tuples, some may be empty
//   - Padding: Zero-filled bytes to reach PageSize
type HeapPage struct {
	pageID          *HeapPageID
	tupleDesc       *tuple.TupleDescription
	tuples          []*tuple.Tuple
	numSlots        int
	dirtier         *primitives.TransactionID
	header, oldData []byte
	mutex           sync.RWMutex
}

// NewHeapPage creates a new HeapPage by deserializing raw page data.
// It calculates the optimal number of slots based on tuple size and initializes
// the header bitmap and tuple array from the provided data.
func NewHeapPage(pid *HeapPageID, data []byte, td *tuple.TupleDescription) (*HeapPage, error) {
	if len(data) != page.PageSize {
		return nil, fmt.Errorf("invalid page data size: expected %d, got %d", page.PageSize, len(data))
	}

	hp := &HeapPage{
		pageID:    pid,
		tupleDesc: td,
		oldData:   make([]byte, page.PageSize),
	}

	hp.numSlots = hp.getNumTuples()
	headerSize := hp.getHeaderSize()
	hp.header = make([]byte, headerSize)
	hp.tuples = make([]*tuple.Tuple, hp.numSlots)

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
func (hp *HeapPage) GetNumEmptySlots() int {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.getNumEmptySlots()
}

// GetID returns the unique page identifier for this heap page.
// Implements the page.Page interface.
//
// Returns:
//   - primitives.PageID: The HeapPageID cast to PageID interface
func (hp *HeapPage) GetID() primitives.PageID {
	return hp.pageID
}

// IsDirty returns the transaction that last modified this page.
// A nil return indicates the page is clean (not modified in current transaction).
// Implements the page.Page interface.
//
// Returns:
//   - *primitives.TransactionID: Transaction that dirtied the page, or nil if clean
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
// The returned data includes the header bitmap, all tuple data (including empty slots),
// and padding to reach page.PageSize.
func (hp *HeapPage) GetPageData() []byte {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	buffer := bytes.NewBuffer(make([]byte, 0, page.PageSize))

	buffer.Write(hp.header)
	tupleSize := hp.tupleDesc.GetSize()
	emptySlot := make([]byte, tupleSize)

	for i := 0; i < hp.numSlots; i++ {
		if hp.tuples[i] == nil {
			buffer.Write(emptySlot)
			continue
		}

		for j := 0; j < hp.tupleDesc.NumFields(); j++ {
			field, err := hp.tuples[i].GetField(j)
			if err != nil {
				buffer.Write(emptySlot)
				break
			}
			field.Serialize(buffer)
		}
	}

	if buffer.Len() < page.PageSize {
		buffer.Write(make([]byte, page.PageSize-buffer.Len()))
	}

	return buffer.Bytes()
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
//   - Internal error finding empty slot
func (hp *HeapPage) AddTuple(t *tuple.Tuple) error {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	if !t.TupleDesc.Equals(hp.tupleDesc) {
		return fmt.Errorf("tuple schema does not match page schema")
	}

	if hp.getNumEmptySlots() == 0 {
		return fmt.Errorf("no space left on this page")
	}

	slotIndex := hp.findFirstEmptySlot()
	if slotIndex == -1 {
		return fmt.Errorf("no empty slot found")
	}

	hp.setSlot(slotIndex, true)
	hp.tuples[slotIndex] = t
	t.RecordID = tuple.NewTupleRecordID(hp.pageID, slotIndex)
	return nil
}

// DeleteTuple removes a tuple from this page by clearing its slot in the header bitmap.
// The tuple's RecordID is set to nil after successful deletion.
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
	if slotIndex < 0 || slotIndex >= hp.numSlots {
		return fmt.Errorf("invalid tuple slot: %d", slotIndex)
	}

	if !hp.isSlotUsed(slotIndex) {
		return fmt.Errorf("tuple slot %d is already empty", slotIndex)
	}

	hp.setSlot(slotIndex, false)
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
func (hp *HeapPage) GetTupleAt(idx int) (*tuple.Tuple, error) {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	if idx < 0 || idx >= hp.numSlots {
		return nil, fmt.Errorf("slot index %d out of bounds", idx)
	}

	return hp.tuples[idx], nil
}

// getNumTuples calculates the maximum number of tuple slots that fit on a page.
// This accounts for both tuple data size and the header bitmap overhead.
//
// Formula: floor((PageSize * BitsPerByte) / (tupleSize * BitsPerByte + 1))
//   - The +1 accounts for the header bit required per tuple slot
//
// Returns:
//   - int: Maximum number of tuple slots for this page's schema
func (hp *HeapPage) getNumTuples() int {
	tupleSize := hp.tupleDesc.GetSize()
	return int((page.PageSize * BitsPerByte)) / int((tupleSize*BitsPerByte + 1))
}

// getHeaderSize calculates the number of bytes needed for the header bitmap.
// One bit is required per tuple slot to track occupancy.
//
// Formula: ceiling(numTuples / 8)
//
// Returns:
//   - int: Size in bytes of the header bitmap
func (hp *HeapPage) getHeaderSize() int {
	numTuples := hp.getNumTuples()
	headerBytes := numTuples / BitsPerByte
	if numTuples%BitsPerByte != 0 {
		headerBytes++
	}
	return headerBytes
}

// parsePageData deserializes raw page bytes into the header bitmap and tuple array.
// Called during page initialization to reconstruct page state from disk.
func (hp *HeapPage) parsePageData(data []byte) error {
	reader := bytes.NewReader(data)

	if _, err := reader.Read(hp.header); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	tupleSize := hp.tupleDesc.GetSize()
	for i := 0; i < hp.numSlots; i++ {
		if hp.isSlotUsed(i) {
			t, err := readTuple(reader, hp.tupleDesc)
			if err != nil {
				return fmt.Errorf("failed to read tuple at slot %d: %v", i, err)
			}

			t.RecordID = tuple.NewTupleRecordID(hp.pageID, i)
			hp.tuples[i] = t
			continue
		}

		tup := emptyTuple(tupleSize)
		if _, err := reader.Read(tup); err != nil {
			return err
		}
	}

	return nil
}

// getNumEmptySlots internal implementation that counts unoccupied slots.
// Must be called with lock already held (does not acquire lock).
func (hp *HeapPage) getNumEmptySlots() int {
	usedSlots := 0
	for _, b := range hp.header {
		usedSlots += bits.OnesCount8(b)
	}
	return hp.numSlots - usedSlots
}

// isSlotUsed checks if a slot is marked as occupied in the header bitmap.
func (hp *HeapPage) isSlotUsed(idx int) bool {
	if idx < 0 || idx >= hp.numSlots {
		return false
	}

	byteIndex := idx / BitsPerByte
	bitOffset := idx % BitsPerByte

	if byteIndex >= len(hp.header) {
		return false
	}

	bitMask := byte(1 << bitOffset)
	return (hp.header[byteIndex] & bitMask) != 0
}

// setSlot sets or clears a bit in the header bitmap to mark slot occupancy.
func (hp *HeapPage) setSlot(idx int, used bool) {
	if idx < 0 || idx >= hp.numSlots {
		return
	}

	byteIndex := idx / 8
	bitOffset := idx % 8

	if byteIndex >= len(hp.header) {
		return
	}

	mask := byte(1 << bitOffset)
	if used {
		hp.header[byteIndex] |= mask
	} else {
		hp.header[byteIndex] &^= mask
	}
}

// findFirstEmptySlot scans the header bitmap for the first unoccupied slot.
func (hp *HeapPage) findFirstEmptySlot() int {
	for i := 0; i < hp.numSlots; i++ {
		if !hp.isSlotUsed(i) {
			return i
		}
	}
	return -1
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

	for j := 0; j < td.NumFields(); j++ {
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

// emptyTuple creates a zero-filled byte slice representing an empty tuple slot.
// Used when serializing pages to ensure consistent size for empty slots.
func emptyTuple(size uint32) []byte {
	return make([]byte, size)
}
