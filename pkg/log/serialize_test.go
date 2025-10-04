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

// ========== DESERIALIZATION TESTS ==========

func TestDeserializeLogRecord_BeginRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	original := &LogRecord{
		Type:      BeginRecord,
		TID:       tid,
		PrevLSN:   0,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
	if deserialized.TID.ID() != original.TID.ID() {
		t.Errorf("TID mismatch: got %d, want %d", deserialized.TID.ID(), original.TID.ID())
	}
	if deserialized.PrevLSN != original.PrevLSN {
		t.Errorf("PrevLSN mismatch: got %d, want %d", deserialized.PrevLSN, original.PrevLSN)
	}
	if deserialized.Timestamp.Unix() != original.Timestamp.Unix() {
		t.Errorf("Timestamp mismatch: got %d, want %d", deserialized.Timestamp.Unix(), original.Timestamp.Unix())
	}
}

func TestDeserializeLogRecord_UpdateRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 1, pageNo: 100}
	beforeImage := []byte("before data")
	afterImage := []byte("after data")

	original := &LogRecord{
		Type:        UpdateRecord,
		TID:         tid,
		PrevLSN:     10,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
	if deserialized.PageID.GetTableID() != pageID.GetTableID() {
		t.Errorf("TableID mismatch: got %d, want %d", deserialized.PageID.GetTableID(), pageID.GetTableID())
	}
	if deserialized.PageID.PageNo() != pageID.PageNo() {
		t.Errorf("PageNo mismatch: got %d, want %d", deserialized.PageID.PageNo(), pageID.PageNo())
	}
	if !bytes.Equal(deserialized.BeforeImage, beforeImage) {
		t.Errorf("BeforeImage mismatch: got %v, want %v", deserialized.BeforeImage, beforeImage)
	}
	if !bytes.Equal(deserialized.AfterImage, afterImage) {
		t.Errorf("AfterImage mismatch: got %v, want %v", deserialized.AfterImage, afterImage)
	}
}

func TestDeserializeLogRecord_InsertRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 2, pageNo: 200}
	afterImage := []byte("new inserted data")

	original := &LogRecord{
		Type:       InsertRecord,
		TID:        tid,
		PrevLSN:    20,
		Timestamp:  time.Unix(1234567890, 0),
		PageID:     pageID,
		AfterImage: afterImage,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
	if !bytes.Equal(deserialized.AfterImage, afterImage) {
		t.Errorf("AfterImage mismatch: got %v, want %v", deserialized.AfterImage, afterImage)
	}
	if deserialized.BeforeImage != nil {
		t.Errorf("BeforeImage should be nil for InsertRecord, got %v", deserialized.BeforeImage)
	}
}

func TestDeserializeLogRecord_DeleteRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 3, pageNo: 300}
	beforeImage := []byte("deleted data")

	original := &LogRecord{
		Type:        DeleteRecord,
		TID:         tid,
		PrevLSN:     30,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: beforeImage,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
	if !bytes.Equal(deserialized.BeforeImage, beforeImage) {
		t.Errorf("BeforeImage mismatch: got %v, want %v", deserialized.BeforeImage, beforeImage)
	}
}

func TestDeserializeLogRecord_CLRRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 4, pageNo: 400}
	afterImage := []byte("compensated data")

	original := &LogRecord{
		Type:        CLRRecord,
		TID:         tid,
		PrevLSN:     40,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		UndoNextLSN: 35,
		AfterImage:  afterImage,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
	if deserialized.UndoNextLSN != original.UndoNextLSN {
		t.Errorf("UndoNextLSN mismatch: got %d, want %d", deserialized.UndoNextLSN, original.UndoNextLSN)
	}
	if !bytes.Equal(deserialized.AfterImage, afterImage) {
		t.Errorf("AfterImage mismatch: got %v, want %v", deserialized.AfterImage, afterImage)
	}
}

func TestDeserializeLogRecord_CommitRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	original := &LogRecord{
		Type:      CommitRecord,
		TID:       tid,
		PrevLSN:   70,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
}

func TestDeserializeLogRecord_AbortRecord(t *testing.T) {
	tid := transaction.NewTransactionID()
	original := &LogRecord{
		Type:      AbortRecord,
		TID:       tid,
		PrevLSN:   80,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, original.Type)
	}
}

func TestDeserializeLogRecord_NilTID(t *testing.T) {
	original := &LogRecord{
		Type:      BeginRecord,
		TID:       nil,
		PrevLSN:   0,
		Timestamp: time.Unix(1234567890, 0),
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.TID != nil {
		t.Errorf("TID should be nil, got %v", deserialized.TID)
	}
}

func TestDeserializeLogRecord_NilImages(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 5, pageNo: 500}

	original := &LogRecord{
		Type:        UpdateRecord,
		TID:         tid,
		PrevLSN:     50,
		Timestamp:   time.Unix(1234567890, 0),
		PageID:      pageID,
		BeforeImage: nil,
		AfterImage:  nil,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if deserialized.BeforeImage != nil {
		t.Errorf("BeforeImage should be nil, got %v", deserialized.BeforeImage)
	}
	if deserialized.AfterImage != nil {
		t.Errorf("AfterImage should be nil, got %v", deserialized.AfterImage)
	}
}

func TestDeserializeLogRecord_LargeImages(t *testing.T) {
	tid := transaction.NewTransactionID()
	pageID := &MockPageID{tableID: 6, pageNo: 600}
	largeImage := bytes.Repeat([]byte("x"), 10000)

	original := &LogRecord{
		Type:       InsertRecord,
		TID:        tid,
		PrevLSN:    60,
		Timestamp:  time.Unix(1234567890, 0),
		PageID:     pageID,
		AfterImage: largeImage,
	}

	data, err := SerializeLogRecord(original)
	if err != nil {
		t.Fatalf("SerializeLogRecord failed: %v", err)
	}

	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("DeserializeLogRecord failed: %v", err)
	}

	if !bytes.Equal(deserialized.AfterImage, largeImage) {
		t.Errorf("AfterImage mismatch for large image")
	}
	if len(deserialized.AfterImage) != 10000 {
		t.Errorf("AfterImage size mismatch: got %d, want 10000", len(deserialized.AfterImage))
	}
}

func TestDeserializeLogRecord_InvalidData_TooShort(t *testing.T) {
	data := []byte{0x00, 0x00}

	_, err := DeserializeLogRecord(data)
	if err == nil {
		t.Error("Expected error for too short data, got nil")
	}
	if err != nil && err.Error() == "" {
		t.Error("Expected meaningful error message")
	}
}

func TestDeserializeLogRecord_InvalidData_SizeMismatch(t *testing.T) {
	data := make([]byte, 100)
	// Set size to 200 (but actual data is only 100 bytes)
	binary.BigEndian.PutUint32(data[0:4], 200)

	_, err := DeserializeLogRecord(data)
	if err == nil {
		t.Error("Expected error for size mismatch, got nil")
	}
}

func TestDeserializeLogRecord_InvalidData_UnknownType(t *testing.T) {
	data := make([]byte, RecordSize+TypeSize+TIDSize+PrevLSNSize+TimestampSize)
	binary.BigEndian.PutUint32(data[0:4], uint32(len(data)))
	data[4] = 255 // Invalid record type

	_, err := DeserializeLogRecord(data)
	if err == nil {
		t.Error("Expected error for unknown record type, got nil")
	}
}

func TestDeserializeLogRecord_RoundTrip(t *testing.T) {
	// Test multiple record types in sequence
	testCases := []struct {
		name   string
		record *LogRecord
	}{
		{
			name: "Begin",
			record: &LogRecord{
				Type:      BeginRecord,
				TID:       transaction.NewTransactionID(),
				PrevLSN:   0,
				Timestamp: time.Unix(1234567890, 0),
			},
		},
		{
			name: "Update",
			record: &LogRecord{
				Type:        UpdateRecord,
				TID:         transaction.NewTransactionID(),
				PrevLSN:     10,
				Timestamp:   time.Unix(1234567890, 0),
				PageID:      &MockPageID{tableID: 1, pageNo: 100},
				BeforeImage: []byte("before"),
				AfterImage:  []byte("after"),
			},
		},
		{
			name: "Insert",
			record: &LogRecord{
				Type:       InsertRecord,
				TID:        transaction.NewTransactionID(),
				PrevLSN:    20,
				Timestamp:  time.Unix(1234567890, 0),
				PageID:     &MockPageID{tableID: 2, pageNo: 200},
				AfterImage: []byte("new"),
			},
		},
		{
			name: "Delete",
			record: &LogRecord{
				Type:        DeleteRecord,
				TID:         transaction.NewTransactionID(),
				PrevLSN:     30,
				Timestamp:   time.Unix(1234567890, 0),
				PageID:      &MockPageID{tableID: 3, pageNo: 300},
				BeforeImage: []byte("old"),
			},
		},
		{
			name: "CLR",
			record: &LogRecord{
				Type:        CLRRecord,
				TID:         transaction.NewTransactionID(),
				PrevLSN:     40,
				Timestamp:   time.Unix(1234567890, 0),
				PageID:      &MockPageID{tableID: 4, pageNo: 400},
				UndoNextLSN: 35,
				AfterImage:  []byte("compensated"),
			},
		},
		{
			name: "Commit",
			record: &LogRecord{
				Type:      CommitRecord,
				TID:       transaction.NewTransactionID(),
				PrevLSN:   50,
				Timestamp: time.Unix(1234567890, 0),
			},
		},
		{
			name: "Abort",
			record: &LogRecord{
				Type:      AbortRecord,
				TID:       transaction.NewTransactionID(),
				PrevLSN:   60,
				Timestamp: time.Unix(1234567890, 0),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize
			data, err := SerializeLogRecord(tc.record)
			if err != nil {
				t.Fatalf("SerializeLogRecord failed: %v", err)
			}

			// Deserialize
			deserialized, err := DeserializeLogRecord(data)
			if err != nil {
				t.Fatalf("DeserializeLogRecord failed: %v", err)
			}

			// Verify type matches
			if deserialized.Type != tc.record.Type {
				t.Errorf("Type mismatch: got %v, want %v", deserialized.Type, tc.record.Type)
			}

			// Verify TID
			if tc.record.TID != nil {
				if deserialized.TID == nil || deserialized.TID.ID() != tc.record.TID.ID() {
					t.Errorf("TID mismatch")
				}
			}

			// Verify PrevLSN
			if deserialized.PrevLSN != tc.record.PrevLSN {
				t.Errorf("PrevLSN mismatch: got %d, want %d", deserialized.PrevLSN, tc.record.PrevLSN)
			}

			// Verify timestamp
			if deserialized.Timestamp.Unix() != tc.record.Timestamp.Unix() {
				t.Errorf("Timestamp mismatch: got %d, want %d", deserialized.Timestamp.Unix(), tc.record.Timestamp.Unix())
			}

			// Verify PageID if applicable
			if tc.record.PageID != nil {
				if deserialized.PageID == nil {
					t.Error("PageID should not be nil")
				} else {
					if !deserialized.PageID.Equals(tc.record.PageID) {
						t.Errorf("PageID mismatch")
					}
				}
			}

			// Verify images if applicable
			if tc.record.BeforeImage != nil {
				if !bytes.Equal(deserialized.BeforeImage, tc.record.BeforeImage) {
					t.Error("BeforeImage mismatch")
				}
			}
			if tc.record.AfterImage != nil {
				if !bytes.Equal(deserialized.AfterImage, tc.record.AfterImage) {
					t.Error("AfterImage mismatch")
				}
			}

			// Verify UndoNextLSN for CLR
			if tc.record.Type == CLRRecord {
				if deserialized.UndoNextLSN != tc.record.UndoNextLSN {
					t.Errorf("UndoNextLSN mismatch: got %d, want %d", deserialized.UndoNextLSN, tc.record.UndoNextLSN)
				}
			}
		})
	}
}

func TestDeserializeImage_ExcessiveSize(t *testing.T) {
	// Test that deserializeImage rejects excessively large images
	buf := bytes.NewBuffer(nil)

	// Write a length that exceeds the max (100MB + 1)
	excessiveSize := uint32(100*1024*1024 + 1)
	binary.Write(buf, binary.BigEndian, excessiveSize)

	reader := bytes.NewReader(buf.Bytes())
	_, err := deserializeImage(reader)

	if err == nil {
		t.Error("Expected error for excessive image size, got nil")
	}
}

func TestDeserializeImage_IncompleteData(t *testing.T) {
	// Test that deserializeImage detects incomplete data
	buf := bytes.NewBuffer(nil)

	// Write a length
	length := uint32(100)
	binary.Write(buf, binary.BigEndian, length)

	// Write only partial data (50 bytes instead of 100)
	partialData := make([]byte, 50)
	buf.Write(partialData)

	reader := bytes.NewReader(buf.Bytes())
	_, err := deserializeImage(reader)

	if err == nil {
		t.Error("Expected error for incomplete image data, got nil")
	}
}
