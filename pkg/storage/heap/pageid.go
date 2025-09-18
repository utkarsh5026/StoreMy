package heap

import (
	"fmt"
	"storemy/pkg/tuple"
)

// HeapPageID represents a unique identifier for a heap page
type HeapPageID struct {
	tableID int // The table this page belongs to
	pageNum int // The page number within the table
}

// NewHeapPageID creates a new heap page ID
func NewHeapPageID(tableID, pageNum int) *HeapPageID {
	return &HeapPageID{
		tableID: tableID,
		pageNum: pageNum,
	}
}

// GetTableID returns the table ID
func (hpid *HeapPageID) GetTableID() int {
	return hpid.tableID
}

// PageNo returns the page number
func (hpid *HeapPageID) PageNo() int {
	return hpid.pageNum
}

// Serialize returns this page ID as an array of integers
func (hpid *HeapPageID) Serialize() []int {
	return []int{hpid.tableID, hpid.pageNum}
}

// Equals checks if two heap page IDs are equal
func (hpid *HeapPageID) Equals(other tuple.PageID) bool {
	otherHeap, ok := other.(*HeapPageID)
	if !ok {
		return false
	}
	return hpid.tableID == otherHeap.tableID && hpid.pageNum == otherHeap.pageNum
}

// String returns a string representation of this heap page ID
func (hpid *HeapPageID) String() string {
	return fmt.Sprintf("HeapPageID(table=%d, page=%d)", hpid.tableID, hpid.pageNum)
}

// HashCode returns a hash code for this heap page ID
func (hpid *HeapPageID) HashCode() int {
	return hpid.tableID*1000 + hpid.pageNum
}
