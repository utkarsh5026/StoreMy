package heap

import (
	"bytes"
	"fmt"
	"io"
	"storemy/pkg/storage"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

const (
	BitsPerByte = 8
)

// HeapPage stores pages of HeapFiles and implements the Page interface
// It manages tuples within a single page using a header bitmap to track occupied slots
type HeapPage struct {
	pageID    *HeapPageID             // ID of this page
	tupleDesc *tuple.TupleDescription // Schema of tuples on this page
	header    []byte                  // Bitmap indicating which slots are occupied
	tuples    []*tuple.Tuple          // Array of tuples (nil means empty slot)
	numSlots  int                     // Total number of tuple slots on this page
	dirtier   *storage.TransactionID  // Transaction that dirtied this page
	oldData   []byte                  // Before image for recovery
	mutex     sync.RWMutex            // Protects concurrent access
}

func NewHeapPage(pid *HeapPageID, data []byte, td *tuple.TupleDescription) (*HeapPage, error) {
	if len(data) != storage.PageSize {
		return nil, fmt.Errorf("invalid page data size: expected %d, got %d", storage.PageSize, len(data))
	}

	hp := &HeapPage{
		pageID:    pid,
		tupleDesc: td,
		oldData:   make([]byte, storage.PageSize),
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

func (hp *HeapPage) GetNumEmptySlots() int {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.getNumEmptySlots()
}

// GetID returns the PageID associated with this page
func (hp *HeapPage) GetID() tuple.PageID {
	return hp.pageID
}

// IsDirty returns the transaction that last dirtied this page, or nil if clean
func (hp *HeapPage) IsDirty() *storage.TransactionID {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()
	return hp.dirtier
}

// MarkDirty marks this page as dirty/clean and records the transaction
func (hp *HeapPage) MarkDirty(dirty bool, tid *storage.TransactionID) {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	if dirty {
		hp.dirtier = tid
	} else {
		hp.dirtier = nil
	}
}

// GetPageData generates a byte array representing the contents of this page
func (hp *HeapPage) GetPageData() []byte {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	var buffer bytes.Buffer

	buffer.Write(hp.header)
	tupleSize := hp.tupleDesc.GetSize()
	for i := 0; i < hp.numSlots; i++ {
		if hp.tuples[i] == nil {
			buffer.Write(emptyTuple(tupleSize))
			continue
		}

		for j := 0; j < hp.tupleDesc.NumFields(); j++ {
			field, err := hp.tuples[i].GetField(j)
			if err != nil {
				buffer.Write(emptyTuple(tupleSize))
				break
			}
			field.Serialize(&buffer)
		}
	}

	currentSize := buffer.Len()
	padding := storage.PageSize - currentSize
	if padding > 0 {
		buffer.Write(make([]byte, padding))
	}

	return buffer.Bytes()
}

// GetBeforeImage returns a page with the before image data
func (hp *HeapPage) GetBeforeImage() storage.Page {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	beforePage, _ := NewHeapPage(hp.pageID, hp.oldData, hp.tupleDesc)
	return beforePage
}

// SetBeforeImage copies current content to the before image
func (hp *HeapPage) SetBeforeImage() {
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	hp.oldData = hp.GetPageData()
}

// AddTuple adds a tuple to this page
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

// DeleteTuple removes a tuple from this page
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

// GetTuples returns all tuples on this page (excluding nil entries)
func (hp *HeapPage) GetTuples() []*tuple.Tuple {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	var tuples []*tuple.Tuple
	for _, t := range hp.tuples {
		if t != nil {
			tuples = append(tuples, t)
		}
	}

	return tuples
}

// GetTupleAt returns the tuple at the specified slot index
func (hp *HeapPage) GetTupleAt(idx int) (*tuple.Tuple, error) {
	hp.mutex.RLock()
	defer hp.mutex.RUnlock()

	if idx < 0 || idx >= hp.numSlots {
		return nil, fmt.Errorf("slot index %d out of bounds", idx)
	}

	return hp.tuples[idx], nil
}

// getNumTuples calculates the number of tuples that fit on this page
// Formula: floor((PageSize * BitsPerByte) / (tupleSize * BitsPerByte + 1))
// The +1 accounts for the header bit per tuple
func (hp *HeapPage) getNumTuples() int {
	tupleSize := hp.tupleDesc.GetSize()
	return int((storage.PageSize * BitsPerByte)) / int((tupleSize*BitsPerByte + 1))
}

// getHeaderSize calculates the number of bytes needed for the header bitmap
// Formula: ceiling(numTuples / 8)
func (hp *HeapPage) getHeaderSize() int {
	numTuples := hp.getNumTuples()
	headerBytes := numTuples / BitsPerByte
	if numTuples%BitsPerByte != 0 {
		headerBytes++
	}
	return headerBytes
}

// parsePageData parses the raw page data into header and tuples
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

		tup := make([]byte, tupleSize)
		if _, err := reader.Read(tup); err != nil {
			return err
		}
	}

	return nil
}

// getNumEmptySlots internal implementation without locking
func (hp *HeapPage) getNumEmptySlots() int {
	usedSlots := 0

	for i := 0; i < hp.numSlots; i++ {
		if hp.isSlotUsed(i) {
			usedSlots++
		}
	}

	return hp.numSlots - usedSlots
}

// isSlotUsed checks if a slot is occupied using the header bitmap
func (hp *HeapPage) isSlotUsed(slotIndex int) bool {
	if slotIndex < 0 || slotIndex >= hp.numSlots {
		return false
	}

	byteIndex := slotIndex / BitsPerByte
	bitOffset := slotIndex % BitsPerByte

	if byteIndex >= len(hp.header) {
		return false
	}

	bitMask := byte(1 << bitOffset)
	return (hp.header[byteIndex] & bitMask) != 0
}

// setSlot sets or clears a slot in the header bitmap
func (hp *HeapPage) setSlot(slotIndex int, used bool) {
	if slotIndex < 0 || slotIndex >= hp.numSlots {
		return
	}

	byteIndex := slotIndex / 8
	bitOffset := slotIndex % 8

	if byteIndex >= len(hp.header) {
		return
	}

	if used {
		hp.header[byteIndex] |= byte(1 << bitOffset)
	} else {
		// Clear the bit
		hp.header[byteIndex] &^= byte(1 << bitOffset)
	}
}

// findFirstEmptySlot finds the first available slot index
func (hp *HeapPage) findFirstEmptySlot() int {
	for i := 0; i < hp.numSlots; i++ {
		if !hp.isSlotUsed(i) {
			return i
		}
	}
	return -1
}

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

func emptyTuple(size uint32) []byte {
	return make([]byte, size)
}
