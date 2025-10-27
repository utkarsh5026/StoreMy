package page

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"storemy/pkg/primitives"
)

type PageType uint8

// PageDescriptor represents a unique identifier for a heap page
type PageDescriptor struct {
	tableID primitives.TableID
	pageNum primitives.PageNumber
}

// NewPageDescriptor creates a new page descriptor
func NewPageDescriptor(tableID primitives.TableID, pageNum primitives.PageNumber) *PageDescriptor {
	return &PageDescriptor{
		tableID: tableID,
		pageNum: pageNum,
	}
}

// GetTableID returns the table ID
func (hpid *PageDescriptor) GetTableID() primitives.TableID {
	return hpid.tableID
}

// PageNo returns the page number
func (hpid *PageDescriptor) PageNo() primitives.PageNumber {
	return hpid.pageNum
}

// Serialize returns this page ID as an array of integers
func (hpid *PageDescriptor) Serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(hpid.tableID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(hpid.pageNum))
	return buf
}

// Equals checks if two heap page IDs are equal
func (hpid *PageDescriptor) Equals(other primitives.PageID) bool {
	if other == nil {
		return false
	}
	return hpid.tableID == other.GetTableID() && hpid.pageNum == other.PageNo()
}

// String returns a string representation of this heap page ID
func (hpid *PageDescriptor) String() string {
	return fmt.Sprintf("PageDescriptor(table=%d, page=%d)", hpid.tableID, hpid.pageNum)
}

// HashCode returns a hash code for this heap page ID
func (hpid *PageDescriptor) HashCode() primitives.HashCode {
	h := fnv.New64a()
	h.Write(hpid.Serialize())
	return primitives.HashCode(h.Sum64())
}
