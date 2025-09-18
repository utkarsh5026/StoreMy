package tuple

import "fmt"

// TupleRecordID represents a reference to a specific tuple on a specific page
type TupleRecordID struct {
	PageID   PageID // The page containing this tuple
	TupleNum int    // The tuple number within the page
}

// NewTupleRecordID creates a new TupleRecordID
func NewTupleRecordID(pageID PageID, tupleNum int) *TupleRecordID {
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
