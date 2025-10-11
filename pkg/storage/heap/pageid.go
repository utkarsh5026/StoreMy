package heap

import (
	"fmt"
	"storemy/pkg/primitives"
)

// HeapPageID represents a unique identifier for a heap page
type HeapPageID struct {
	tableID, pageNum int
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
func (hpid *HeapPageID) Equals(other primitives.PageID) bool {
	if other == nil {
		return false
	}
	return hpid.tableID == other.GetTableID() && hpid.pageNum == other.PageNo()
}

// String returns a string representation of this heap page ID
func (hpid *HeapPageID) String() string {
	return fmt.Sprintf("HeapPageID(table=%d, page=%d)", hpid.tableID, hpid.pageNum)
}

// HashCode returns a hash code for this heap page ID
func (hpid *HeapPageID) HashCode() int {
	return hpid.tableID*1000 + hpid.pageNum
}
