package btree

import (
	"fmt"
	"storemy/pkg/primitives"
)

// BTreePageID represents a unique identifier for a B+Tree page
type BTreePageID struct {
	tableID int // The index (table) this page belongs to
	pageNum int // The page number within the index
}

// NewBTreePageID creates a new B+Tree page ID
func NewBTreePageID(tableID, pageNum int) *BTreePageID {
	return &BTreePageID{
		tableID: tableID,
		pageNum: pageNum,
	}
}

// GetTableID returns the table/index ID
func (bpid *BTreePageID) GetTableID() int {
	return bpid.tableID
}

// PageNo returns the page number
func (bpid *BTreePageID) PageNo() int {
	return bpid.pageNum
}

// Serialize returns this page ID as an array of integers
func (bpid *BTreePageID) Serialize() []int {
	return []int{bpid.tableID, bpid.pageNum}
}

// Equals checks if two B+Tree page IDs are equal
func (bpid *BTreePageID) Equals(other primitives.PageID) bool {
	if other == nil {
		return false
	}
	return bpid.tableID == other.GetTableID() && bpid.pageNum == other.PageNo()
}

// String returns a string representation of this B+Tree page ID
func (bpid *BTreePageID) String() string {
	return fmt.Sprintf("BTreePageID(index=%d, page=%d)", bpid.tableID, bpid.pageNum)
}

// HashCode returns a hash code for this B+Tree page ID
func (bpid *BTreePageID) HashCode() int {
	return bpid.tableID*1000 + bpid.pageNum
}
