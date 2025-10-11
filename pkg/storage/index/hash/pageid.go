package hash

import (
	"fmt"
	"storemy/pkg/primitives"
)

// HashPageID represents a unique identifier for a hash index page
type HashPageID struct {
	tableID int
	pageNum int
}

// NewHashPageID creates a new HashPageID
func NewHashPageID(tableID int, pageNum int) *HashPageID {
	return &HashPageID{
		tableID: tableID,
		pageNum: pageNum,
	}
}

// GetTableID returns the table ID (index ID in this case)
func (hpid *HashPageID) GetTableID() int {
	return hpid.tableID
}

// PageNo returns the page number
func (hpid *HashPageID) PageNo() int {
	return hpid.pageNum
}

// Equals checks if two page IDs are equal
func (hpid *HashPageID) Equals(other primitives.PageID) bool {
	if other == nil {
		return false
	}
	otherHash, ok := other.(*HashPageID)
	if !ok {
		return false
	}
	return hpid.tableID == otherHash.tableID && hpid.pageNum == otherHash.pageNum
}

// Serialize serializes the page ID to integers
func (hpid *HashPageID) Serialize() []int {
	return []int{hpid.tableID, hpid.pageNum}
}

// String returns a string representation of the page ID
func (hpid *HashPageID) String() string {
	return fmt.Sprintf("HashPageID(table=%d, page=%d)", hpid.tableID, hpid.pageNum)
}

// HashCode returns the hash code for this page ID
func (hpid *HashPageID) HashCode() int {
	return hpid.tableID*31 + hpid.pageNum
}
