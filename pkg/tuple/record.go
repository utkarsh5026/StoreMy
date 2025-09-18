package tuple

import "fmt"

// RecordID represents a reference to a specific tuple on a specific page
type RecordID struct {
	PageID   PageID // The page containing this tuple
	TupleNum int    // The tuple number within the page
}

// NewRecordID creates a new RecordID
func NewRecordID(pageID PageID, tupleNum int) *RecordID {
	return &RecordID{
		PageID:   pageID,
		TupleNum: tupleNum,
	}
}

func (rid *RecordID) Equals(other *RecordID) bool {
	if other == nil {
		return false
	}
	return rid.PageID.Equals(other.PageID) && rid.TupleNum == other.TupleNum
}

func (rid *RecordID) String() string {
	return fmt.Sprintf("RecordID(page=%s, tuple=%d)", rid.PageID.String(), rid.TupleNum)
}
