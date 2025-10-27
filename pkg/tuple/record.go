package tuple

import (
	"fmt"
	"storemy/pkg/primitives"
)

// TupleRecordID represents a reference to a specific tuple on a specific page
type TupleRecordID struct {
	PageID   primitives.PageID // The page containing this tuple
	TupleNum primitives.SlotID // The tuple number within the page
}

// NewTupleRecordID creates a new TupleRecordID
func NewTupleRecordID(pageID primitives.PageID, tupleNum primitives.SlotID) *TupleRecordID {
	return &TupleRecordID{
		PageID:   pageID,
		TupleNum: tupleNum,
	}
}

func (rid *TupleRecordID) Equals(other *TupleRecordID) bool {
	if other == nil {
		return false
	}
	return rid.PageID.Equals(other.PageID) && rid.TupleNum == other.TupleNum
}

func (rid *TupleRecordID) String() string {
	return fmt.Sprintf("TupleRecordID(page=%s, tuple=%d)", rid.PageID.String(), rid.TupleNum)
}
