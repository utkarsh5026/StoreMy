package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"testing"
	"time"
)

// MockPageID implements PageID interface for testing
type MockPageID struct {
	tableID int
	pageNo  int
}

func (m *MockPageID) GetTableID() int {
	return m.tableID
}

func (m *MockPageID) PageNo() int {
	return m.pageNo
}

func (m *MockPageID) Serialize() []int {
	return []int{m.tableID, m.pageNo}
}

func (m *MockPageID) Equals(other tuple.PageID) bool {
	return m.tableID == other.GetTableID() && m.pageNo == other.PageNo()
}

func (m *MockPageID) HashCode() int {
	return m.tableID*31 + m.pageNo
}

func (m *MockPageID) String() string {
	return fmt.Sprintf("MockPageID{tableID: %d, pageNo: %d}", m.tableID, m.pageNo)
}

func TestSerializeLogRecord_BeginRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	record := &LogRecord{
		Type:      BeginRecord,
		TID:       tid,
		PrevLSN:   0,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size field
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Verify type
	if data[4] != byte(BeginRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], BeginRecord)
	}

	// Verify TID
	tid_val := binary.BigEndian.Uint64(data[5:13])
	if tid_val != uint64(tid.ID()) {
		t.Errorf("TID mismatch: got %d, want %d", tid_val, tid.ID())
	}
}

func TestSerializeLogRecord_UpdateRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 1, pageNo: 100}
	beforeImage := []byte("before")
	afterImage := []byte("after")

	record := &LogRecord{
		Type:        UpdateRecord,
		TID:         tid,
		PrevLSN:     10,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size field
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Verify type
	if data[4] != byte(UpdateRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], UpdateRecord)
	}

	// Verify TID
	tid_val := binary.BigEndian.Uint64(data[5:13])
	if tid_val != uint64(tid.ID()) {
		t.Errorf("TID mismatch: got %d, want %d", tid_val, tid.ID())
	}

	// Verify PrevLSN
	prevLSN := binary.BigEndian.Uint64(data[13:21])
	if prevLSN != 10 {
		t.Errorf("PrevLSN mismatch: got %d, want 10", prevLSN)
	}
}

func TestSerializeLogRecord_InsertRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 2, pageNo: 200}
	afterImage := []byte("new data")

	record := &LogRecord{
		Type:       InsertRecord,
		TID:        tid,
		PrevLSN:    20,
		Timestamp:  time.Unix(1234567890, 0),
		PageID:     pageID,
		AfterImage: afterImage,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size field
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Verify type
	if data[4] != byte(InsertRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], InsertRecord)
	}
}

func TestSerializeLogRecord_DeleteRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 3, pageNo: 300}
	beforeImage := []byte("old data")

	record := &LogRecord{
		Type:        DeleteRecord,
		TID:         tid,
		PrevLSN:     30,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: beforeImage,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size field
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Verify type
	if data[4] != byte(DeleteRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], DeleteRecord)
	}
}

func TestSerializeLogRecord_CLRRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 4, pageNo: 400}
	afterImage := []byte("compensated data")

	record := &LogRecord{
		Type:        CLRRecord,
		TID:         tid,
		PrevLSN:     40,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		UndoNextLSN: 35,
		AfterImage:  afterImage,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size field
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Verify type
	if data[4] != byte(CLRRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], CLRRecord)
	}
}

func TestSerializeLogRecord_NilTID(t *testing.T) {
	record := &LogRecord{
		Type:      BeginRecord,
		TID:       nil,
		PrevLSN:   0,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify TID is 0 when nil
	tid_val := binary.BigEndian.Uint64(data[5:13])
	if tid_val != 0 {
		t.Errorf("TID should be 0 when nil, got %d", tid_val)
	}
}

func TestSerializeLogRecord_NilImages(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 5, pageNo: 500}

	record := &LogRecord{
		Type:        UpdateRecord,
		TID:         tid,
		PrevLSN:     50,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: nil,
		AfterImage:  nil,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Should still serialize successfully with nil images
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}
}

func TestSerializeLogRecord_LargeImages(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 6, pageNo: 600}
	largeImage := bytes.Repeat([]byte("x"), 10000)

	record := &LogRecord{
		Type:       InsertRecord,
		TID:        tid,
		PrevLSN:    60,
		Timestamp:  time.Unix(1234567890, 0),
		PageID:     pageID,
		AfterImage: largeImage,
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify size includes the large image
	size := binary.BigEndian.Uint32(data[0:4])
	if size != uint32(len(data)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(data))
	}

	// Data should be larger than 10000 bytes
	if len(data) < 10000 {
		t.Errorf("Serialized data too small: got %d bytes", len(data))
	}
}

func TestSerializeLogRecord_CommitRecord(t *testing.T) {
	tid := transaction.NewTransactionID()

	record := &LogRecord{
		Type:      CommitRecord,
		TID:       tid,
		PrevLSN:   70,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify type
	if data[4] != byte(CommitRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], CommitRecord)
	}
}

func TestSerializeLogRecord_AbortRecord(t *testing.T) {
	tid := transaction.NewTransactionID()

	record := &LogRecord{
		Type:      AbortRecord,
		TID:       tid,
		PrevLSN:   80,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(record)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	// Verify type
	if data[4] != byte(AbortRecord) {
		t.Errorf("Type mismatch: got %d, want %d", data[4], AbortRecord)
	}
}

func TestSerializeImage_NonNilImage(t *testing.T) {
	var buf bytes.Buffer
	image := []byte("test image")

	serializeImage(&buf, image)

	data := buf.Bytes()

	// First 4 bytes should be the length
	length := binary.BigEndian.Uint32(data[0:4])
	if length != uint32(len(image)) {
		t.Errorf("Length mismatch: got %d, want %d", length, len(image))
	}

	// Next bytes should be the actual image
	if !bytes.Equal(data[4:], image) {
		t.Errorf("Image data mismatch")
	}
}

func TestSerializeImage_NilImage(t *testing.T) {
	var buf bytes.Buffer

	serializeImage(&buf, nil)

	data := buf.Bytes()

	// Should write 4 bytes with value 0
	if len(data) != 4 {
		t.Errorf("Expected 4 bytes, got %d", len(data))
	}

	length := binary.BigEndian.Uint32(data[0:4])
	if length != 0 {
		t.Errorf("Length should be 0 for nil image, got %d", length)
	}
}

func TestSerializeDataModification(t *testing.T) {
	var buf bytes.Buffer
	pageID := &MockPageID{tableID: 7, pageNo: 700}
	beforeImage := []byte("before")
	afterImage := []byte("after")

	record := &LogRecord{
		PageID:      pageID,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}

	serializeDataModification(&buf, record)

	data := buf.Bytes()

	// Should contain pageID, before image length + data, after image length + data
	if len(data) == 0 {
		t.Error("Expected non-empty serialization")
	}
}

func TestSerializeCLR(t *testing.T) {
	var buf bytes.Buffer
	pageID := &MockPageID{tableID: 8, pageNo: 800}
	afterImage := []byte("compensated")

	record := &LogRecord{
		PageID:      pageID,
		UndoNextLSN: 100,
		AfterImage:  afterImage,
	}

	serializeCLR(&buf, record)

	data := buf.Bytes()

	// Should contain pageID, UndoNextLSN, and after image
	if len(data) == 0 {
		t.Error("Expected non-empty serialization")
	}
}
