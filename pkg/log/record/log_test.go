package record

import (
	"storemy/pkg/primitives"
	"testing"
	"time"
)

// mockPageIDForLog implements primitives.PageID for testing
type mockPageIDForLog struct {
	tableID primitives.FileID
	pageNo  primitives.PageNumber
}

func (m *mockPageIDForLog) FileID() primitives.FileID {
	return m.tableID
}

func (m *mockPageIDForLog) PageNo() primitives.PageNumber {
	return m.pageNo
}

func (m *mockPageIDForLog) Equals(other primitives.PageID) bool {
	return m.FileID() == other.FileID() && m.PageNo() == other.PageNo()
}

func (m *mockPageIDForLog) HashCode() primitives.HashCode {
	return primitives.HashCode(uint64(m.tableID)*31 + uint64(m.pageNo))
}

func (m *mockPageIDForLog) Serialize() []byte {
	result := make([]byte, 16)
	// Serialize TableID (8 bytes)
	for i := 0; i < 8; i++ {
		result[i] = byte(m.tableID >> (i * 8))
	}
	// Serialize PageNumber (8 bytes)
	for i := 0; i < 8; i++ {
		result[8+i] = byte(m.pageNo >> (i * 8))
	}
	return result
}

func (m *mockPageIDForLog) String() string {
	return ""
}

func TestNewLogRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 1, pageNo: 42}
	beforeImage := []byte("before")
	afterImage := []byte("after")
	prevLSN := primitives.LSN(100)

	record := NewLogRecord(UpdateRecord, tid, pageID, beforeImage, afterImage, prevLSN)

	if record == nil {
		t.Fatal("expected non-nil LogRecord")
	}

	if record.Type != UpdateRecord {
		t.Errorf("expected Type to be UpdateRecord, got %v", record.Type)
	}

	if record.TID != tid {
		t.Errorf("expected TID to match")
	}

	if !record.PageID.Equals(pageID) {
		t.Errorf("expected PageID to match")
	}

	if string(record.BeforeImage) != "before" {
		t.Errorf("expected BeforeImage to be 'before', got %s", string(record.BeforeImage))
	}

	if string(record.AfterImage) != "after" {
		t.Errorf("expected AfterImage to be 'after', got %s", string(record.AfterImage))
	}

	if record.PrevLSN != prevLSN {
		t.Errorf("expected PrevLSN to be %d, got %d", prevLSN, record.PrevLSN)
	}

	if record.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set")
	}

	// Timestamp should be recent (within last second)
	if time.Since(record.Timestamp) > time.Second {
		t.Error("expected Timestamp to be recent")
	}
}

func TestNewLogRecord_BeginRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	record := NewLogRecord(BeginRecord, tid, nil, nil, nil, primitives.LSN(0))

	if record.Type != BeginRecord {
		t.Errorf("expected Type to be BeginRecord, got %v", record.Type)
	}

	if record.PageID != nil {
		t.Error("expected PageID to be nil for BeginRecord")
	}

	if record.BeforeImage != nil {
		t.Error("expected BeforeImage to be nil for BeginRecord")
	}

	if record.AfterImage != nil {
		t.Error("expected AfterImage to be nil for BeginRecord")
	}

	if record.PrevLSN != 0 {
		t.Errorf("expected PrevLSN to be 0, got %d", record.PrevLSN)
	}
}

func TestNewLogRecord_CommitRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	prevLSN := primitives.LSN(500)
	record := NewLogRecord(CommitRecord, tid, nil, nil, nil, prevLSN)

	if record.Type != CommitRecord {
		t.Errorf("expected Type to be CommitRecord, got %v", record.Type)
	}

	if record.PrevLSN != prevLSN {
		t.Errorf("expected PrevLSN to be %d, got %d", prevLSN, record.PrevLSN)
	}
}

func TestNewLogRecord_AbortRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	prevLSN := primitives.LSN(300)
	record := NewLogRecord(AbortRecord, tid, nil, nil, nil, prevLSN)

	if record.Type != AbortRecord {
		t.Errorf("expected Type to be AbortRecord, got %v", record.Type)
	}

	if record.PrevLSN != prevLSN {
		t.Errorf("expected PrevLSN to be %d, got %d", prevLSN, record.PrevLSN)
	}
}

func TestNewLogRecord_InsertRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 5, pageNo: 10}
	afterImage := []byte("new_data")
	prevLSN := primitives.LSN(200)

	record := NewLogRecord(InsertRecord, tid, pageID, nil, afterImage, prevLSN)

	if record.Type != InsertRecord {
		t.Errorf("expected Type to be InsertRecord, got %v", record.Type)
	}

	if record.BeforeImage != nil {
		t.Error("expected BeforeImage to be nil for InsertRecord")
	}

	if string(record.AfterImage) != "new_data" {
		t.Errorf("expected AfterImage to be 'new_data', got %s", string(record.AfterImage))
	}

	if record.PageID.FileID() != 5 {
		t.Errorf("expected TableID to be 5, got %d", record.PageID.FileID())
	}

	if record.PageID.PageNo() != 10 {
		t.Errorf("expected PageNumber to be 10, got %d", record.PageID.PageNo())
	}
}

func TestNewLogRecord_DeleteRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 3, pageNo: 7}
	beforeImage := []byte("deleted_data")
	prevLSN := primitives.LSN(400)

	record := NewLogRecord(DeleteRecord, tid, pageID, beforeImage, nil, prevLSN)

	if record.Type != DeleteRecord {
		t.Errorf("expected Type to be DeleteRecord, got %v", record.Type)
	}

	if string(record.BeforeImage) != "deleted_data" {
		t.Errorf("expected BeforeImage to be 'deleted_data', got %s", string(record.BeforeImage))
	}

	if record.AfterImage != nil {
		t.Error("expected AfterImage to be nil for DeleteRecord")
	}
}

func TestNewLogRecord_UpdateRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 2, pageNo: 15}
	beforeImage := []byte("old_value")
	afterImage := []byte("new_value")
	prevLSN := primitives.LSN(600)

	record := NewLogRecord(UpdateRecord, tid, pageID, beforeImage, afterImage, prevLSN)

	if record.Type != UpdateRecord {
		t.Errorf("expected Type to be UpdateRecord, got %v", record.Type)
	}

	if string(record.BeforeImage) != "old_value" {
		t.Errorf("expected BeforeImage to be 'old_value', got %s", string(record.BeforeImage))
	}

	if string(record.AfterImage) != "new_value" {
		t.Errorf("expected AfterImage to be 'new_value', got %s", string(record.AfterImage))
	}

	if record.PageID.FileID() != 2 {
		t.Errorf("expected TableID to be 2, got %d", record.PageID.FileID())
	}

	if record.PageID.PageNo() != 15 {
		t.Errorf("expected PageNumber to be 15, got %d", record.PageID.PageNo())
	}
}

func TestNewLogRecord_NilTransaction(t *testing.T) {
	// Some log records might not have a transaction (e.g., checkpoint)
	record := NewLogRecord(CheckpointBegin, nil, nil, nil, nil, primitives.LSN(0))

	if record.Type != CheckpointBegin {
		t.Errorf("expected Type to be CheckpointBegin, got %v", record.Type)
	}

	if record.TID != nil {
		t.Error("expected TID to be nil for checkpoint records")
	}
}

func TestNewLogRecord_CLRRecord(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 1, pageNo: 5}
	beforeImage := []byte("undo_data")
	prevLSN := primitives.LSN(700)

	record := NewLogRecord(CLRRecord, tid, pageID, beforeImage, nil, prevLSN)

	if record.Type != CLRRecord {
		t.Errorf("expected Type to be CLRRecord, got %v", record.Type)
	}

	// CLR records are used during undo, so they typically have before image
	if string(record.BeforeImage) != "undo_data" {
		t.Errorf("expected BeforeImage to be 'undo_data', got %s", string(record.BeforeImage))
	}
}

func TestTransactionLogInfo_Initialization(t *testing.T) {
	info := &TransactionLogInfo{
		FirstLSN:    primitives.LSN(100),
		LastLSN:     primitives.LSN(500),
		UndoNextLSN: primitives.LSN(450),
	}

	if info.FirstLSN != 100 {
		t.Errorf("expected FirstLSN to be 100, got %d", info.FirstLSN)
	}

	if info.LastLSN != 500 {
		t.Errorf("expected LastLSN to be 500, got %d", info.LastLSN)
	}

	if info.UndoNextLSN != 450 {
		t.Errorf("expected UndoNextLSN to be 450, got %d", info.UndoNextLSN)
	}
}

func TestTransactionLogInfo_EmptyTransaction(t *testing.T) {
	info := &TransactionLogInfo{
		FirstLSN:    primitives.LSN(0),
		LastLSN:     primitives.LSN(0),
		UndoNextLSN: primitives.LSN(0),
	}

	if info.FirstLSN != 0 {
		t.Errorf("expected FirstLSN to be 0, got %d", info.FirstLSN)
	}

	if info.LastLSN != 0 {
		t.Errorf("expected LastLSN to be 0, got %d", info.LastLSN)
	}
}

func TestLSN_Ordering(t *testing.T) {
	lsn1 := primitives.LSN(100)
	lsn2 := primitives.LSN(200)
	lsn3 := primitives.LSN(300)

	if lsn1 >= lsn2 {
		t.Error("expected primitives.LSN 100 to be less than primitives.LSN 200")
	}

	if lsn2 >= lsn3 {
		t.Error("expected primitives.LSN 200 to be less than primitives.LSN 300")
	}

	if lsn1 >= lsn3 {
		t.Error("expected primitives.LSN 100 to be less than primitives.LSN 300")
	}
}

func TestLSN_FirstLSN(t *testing.T) {
	firstLSN := primitives.LSN(0)
	if firstLSN != 0 {
		t.Errorf("expected firstLSN to be 0, got %d", firstLSN)
	}

	// FirstLSN should be less than any valid primitives.LSN
	validLSN := primitives.LSN(1)
	if firstLSN >= validLSN {
		t.Error("expected firstLSN to be less than any valid primitives.LSN")
	}
}

func TestLogRecordType_Values(t *testing.T) {
	// Verify the log record types have expected values
	types := []struct {
		name     string
		recType  LogRecordType
		expected uint8
	}{
		{"BeginRecord", BeginRecord, 0},
		{"CommitRecord", CommitRecord, 1},
		{"AbortRecord", AbortRecord, 2},
		{"UpdateRecord", UpdateRecord, 3},
		{"InsertRecord", InsertRecord, 4},
		{"DeleteRecord", DeleteRecord, 5},
		{"CheckpointBegin", CheckpointBegin, 6},
		{"CheckpointEnd", CheckpointEnd, 7},
		{"CLRRecord", CLRRecord, 8},
	}

	for _, tt := range types {
		if uint8(tt.recType) != tt.expected {
			t.Errorf("%s: expected value %d, got %d", tt.name, tt.expected, uint8(tt.recType))
		}
	}
}

func TestNewLogRecord_LargeImages(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 1, pageNo: 1}

	// Create large images (simulating a full page)
	beforeImage := make([]byte, 8192)
	afterImage := make([]byte, 8192)

	for i := 0; i < 8192; i++ {
		beforeImage[i] = byte(i % 256)
		afterImage[i] = byte((i + 1) % 256)
	}

	record := NewLogRecord(UpdateRecord, tid, pageID, beforeImage, afterImage, primitives.LSN(100))

	if len(record.BeforeImage) != 8192 {
		t.Errorf("expected BeforeImage length to be 8192, got %d", len(record.BeforeImage))
	}

	if len(record.AfterImage) != 8192 {
		t.Errorf("expected AfterImage length to be 8192, got %d", len(record.AfterImage))
	}

	// Verify data integrity
	for i := 0; i < 8192; i++ {
		if record.BeforeImage[i] != byte(i%256) {
			t.Errorf("BeforeImage data mismatch at index %d", i)
			break
		}
		if record.AfterImage[i] != byte((i+1)%256) {
			t.Errorf("AfterImage data mismatch at index %d", i)
			break
		}
	}
}

func TestNewLogRecord_EmptyImages(t *testing.T) {
	tid := primitives.NewTransactionID()
	pageID := &mockPageIDForLog{tableID: 1, pageNo: 1}

	// Create empty but non-nil slices
	beforeImage := []byte{}
	afterImage := []byte{}

	record := NewLogRecord(UpdateRecord, tid, pageID, beforeImage, afterImage, primitives.LSN(50))

	if record.BeforeImage == nil {
		t.Error("expected BeforeImage to be non-nil")
	}

	if record.AfterImage == nil {
		t.Error("expected AfterImage to be non-nil")
	}

	if len(record.BeforeImage) != 0 {
		t.Errorf("expected BeforeImage length to be 0, got %d", len(record.BeforeImage))
	}

	if len(record.AfterImage) != 0 {
		t.Errorf("expected AfterImage length to be 0, got %d", len(record.AfterImage))
	}
}

func TestLogRecord_UndoNextLSN(t *testing.T) {
	tid := primitives.NewTransactionID()
	record := NewLogRecord(CLRRecord, tid, nil, nil, nil, primitives.LSN(100))

	// Initially UndoNextLSN should be zero
	if record.UndoNextLSN != 0 {
		t.Errorf("expected initial UndoNextLSN to be 0, got %d", record.UndoNextLSN)
	}

	// Set UndoNextLSN for CLR
	record.UndoNextLSN = primitives.LSN(50)

	if record.UndoNextLSN != 50 {
		t.Errorf("expected UndoNextLSN to be 50, got %d", record.UndoNextLSN)
	}
}

func TestLogRecord_LSNField(t *testing.T) {
	tid := primitives.NewTransactionID()
	record := NewLogRecord(BeginRecord, tid, nil, nil, nil, primitives.LSN(0))

	// Initially LSN should be zero (not set)
	if record.LSN != 0 {
		t.Errorf("expected initial LSN to be 0, got %d", record.LSN)
	}

	// primitives.LSN is typically set by the WAL when the record is written
	record.LSN = primitives.LSN(42)

	if record.LSN != 42 {
		t.Errorf("expected LSN to be 42, got %d", record.LSN)
	}
}

func TestNewLogRecord_TimestampAccuracy(t *testing.T) {
	before := time.Now()
	time.Sleep(1 * time.Millisecond) // Small delay to ensure timestamp is after 'before'

	tid := primitives.NewTransactionID()
	record := NewLogRecord(BeginRecord, tid, nil, nil, nil, primitives.LSN(0))

	time.Sleep(1 * time.Millisecond) // Small delay to ensure timestamp is before 'after'
	after := time.Now()

	if record.Timestamp.Before(before) {
		t.Error("expected Timestamp to be after 'before' time")
	}

	if record.Timestamp.After(after) {
		t.Error("expected Timestamp to be before 'after' time")
	}
}
