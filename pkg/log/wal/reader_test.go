package wal

import (
	"io"
	"os"
	"path/filepath"
	"storemy/pkg/log/record"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"testing"
)

// TestNewLogReader tests creating a new log reader
func TestNewLogReader(t *testing.T) {
	// Create a temporary log file
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Create an empty file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file.Close()

	// Create log reader
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	if reader == nil {
		t.Fatal("expected non-nil reader")
	}

	if reader.offset != 0 {
		t.Errorf("expected initial offset to be 0, got %d", reader.offset)
	}

	if reader.file == nil {
		t.Error("expected non-nil file")
	}
}

// TestNewLogReader_NonExistentFile tests opening a non-existent file
func TestNewLogReader_NonExistentFile(t *testing.T) {
	reader, err := NewLogReader("/nonexistent/path/log.file")
	if err == nil {
		t.Error("expected error when opening non-existent file")
		if reader != nil {
			reader.Close()
		}
	}
	if reader != nil {
		t.Error("expected nil reader for non-existent file")
	}
}

// TestLogReader_ReadNext_EmptyFile tests reading from an empty file
func TestLogReader_ReadNext_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "empty.log")

	// Create empty file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	record, err := reader.ReadNext()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
	if record != nil {
		t.Error("expected nil record for empty file")
	}
}

// TestLogReader_ReadNext_SingleRecord tests reading a single log record
func TestLogReader_ReadNext_SingleRecord(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "single.log")

	// Create a log record
	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 42)
	rec := record.NewLogRecord(record.UpdateRecord, tid, pageID, []byte("before"), []byte("after"), FirstLSN)

	// Write the record to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	serialized, err := record.SerializeLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to serialize record: %v", err)
	}

	_, err = file.Write(serialized)
	if err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	file.Close()

	// Read the record back
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	readRecord, err := reader.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext failed: %v", err)
	}

	if readRecord == nil {
		t.Fatal("expected non-nil record")
	}

	// Verify record contents
	if readRecord.Type != record.UpdateRecord {
		t.Errorf("expected Type UpdateRecord, got %v", readRecord.Type)
	}

	if readRecord.TID.ID() != tid.ID() {
		t.Errorf("expected TID %v, got %v", tid.ID(), readRecord.TID.ID())
	}

	if !readRecord.PageID.Equals(pageID) {
		t.Error("PageID mismatch")
	}

	if string(readRecord.BeforeImage) != "before" {
		t.Errorf("expected BeforeImage 'before', got %s", string(readRecord.BeforeImage))
	}

	if string(readRecord.AfterImage) != "after" {
		t.Errorf("expected AfterImage 'after', got %s", string(readRecord.AfterImage))
	}

	// LSN should be set to the offset where the record was read
	if readRecord.LSN != 0 {
		t.Errorf("expected LSN to be 0, got %d", readRecord.LSN)
	}

	// Try reading again - should get EOF
	record2, err := reader.ReadNext()
	if err != io.EOF {
		t.Errorf("expected io.EOF on second read, got %v", err)
	}
	if record2 != nil {
		t.Error("expected nil record on second read")
	}
}

// TestLogReader_ReadNext_MultipleRecords tests reading multiple log records
func TestLogReader_ReadNext_MultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "multiple.log")

	// Create multiple log records
	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()
	pageID1 := heap.NewHeapPageID(1, 10)
	pageID2 := heap.NewHeapPageID(2, 20)

	records := []*record.LogRecord{
		record.NewLogRecord(record.BeginRecord, tid1, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.UpdateRecord, tid1, pageID1, []byte("old1"), []byte("new1"), FirstLSN),
		record.NewLogRecord(record.BeginRecord, tid2, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.InsertRecord, tid2, pageID2, nil, []byte("inserted"), FirstLSN),
		record.NewLogRecord(record.CommitRecord, tid1, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.CommitRecord, tid2, nil, nil, nil, FirstLSN),
	}

	// Write all records to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	for _, rec := range records {
		serialized, err := record.SerializeLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}
		_, err = file.Write(serialized)
		if err != nil {
			t.Fatalf("failed to write record: %v", err)
		}
	}
	file.Close()

	// Read all records back
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	for i := 0; i < len(records); i++ {
		readRecord, err := reader.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext failed on record %d: %v", i, err)
		}

		if readRecord == nil {
			t.Fatalf("expected non-nil record at index %d", i)
		}

		if readRecord.Type != records[i].Type {
			t.Errorf("record %d: expected Type %v, got %v", i, records[i].Type, readRecord.Type)
		}
	}

	// Try reading one more time - should get EOF
	record, err := reader.ReadNext()
	if err != io.EOF {
		t.Errorf("expected io.EOF after all records, got %v", err)
	}
	if record != nil {
		t.Error("expected nil record after EOF")
	}
}

// TestLogReader_ReadAll tests reading all records at once
func TestLogReader_ReadAll(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "readall.log")

	// Create test records
	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 5)

	records := []*record.LogRecord{
		record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.UpdateRecord, tid, pageID, []byte("before"), []byte("after"), FirstLSN),
		record.NewLogRecord(record.CommitRecord, tid, nil, nil, nil, FirstLSN),
	}

	// Write records to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	for _, rec := range records {
		serialized, err := record.SerializeLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}
		_, err = file.Write(serialized)
		if err != nil {
			t.Fatalf("failed to write record: %v", err)
		}
	}
	file.Close()

	// Read all records
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	allRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(allRecords) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(allRecords))
	}

	for i, record := range allRecords {
		if record.Type != records[i].Type {
			t.Errorf("record %d: expected Type %v, got %v", i, records[i].Type, record.Type)
		}
	}
}

// TestLogReader_ReadAll_EmptyFile tests ReadAll on empty file
func TestLogReader_ReadAll_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "empty_readall.log")

	// Create empty file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	allRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(allRecords) != 0 {
		t.Errorf("expected 0 records from empty file, got %d", len(allRecords))
	}
}

// TestLogReader_Reset tests resetting the reader to the beginning
func TestLogReader_Reset(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "reset.log")

	// Create test records
	tid := primitives.NewTransactionID()
	records := []*record.LogRecord{
		record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.CommitRecord, tid, nil, nil, nil, FirstLSN),
	}

	// Write records to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	for _, rec := range records {
		serialized, err := record.SerializeLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}
		_, err = file.Write(serialized)
		if err != nil {
			t.Fatalf("failed to write record: %v", err)
		}
	}
	file.Close()

	// Read all records
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	// Read first record
	_, err = reader.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext failed: %v", err)
	}

	// Verify offset has changed
	if reader.offset == 0 {
		t.Error("expected offset to change after reading")
	}

	// Reset
	err = reader.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	if reader.offset != 0 {
		t.Errorf("expected offset to be 0 after reset, got %d", reader.offset)
	}

	// Read again - should get the first record again
	readRec, err := reader.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext after reset failed: %v", err)
	}

	if readRec.Type != record.BeginRecord {
		t.Errorf("expected first record to be BeginRecord after reset, got %v", readRec.Type)
	}
}

// TestLogReader_GetFileSize tests getting the file size
func TestLogReader_GetFileSize(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "filesize.log")

	// Create test record
	tid := primitives.NewTransactionID()
	rec := record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN)

	// Write record to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	serialized, err := record.SerializeLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to serialize record: %v", err)
	}

	_, err = file.Write(serialized)
	if err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	file.Close()

	// Open reader and check file size
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	size, err := reader.GetFileSize()
	if err != nil {
		t.Fatalf("GetFileSize failed: %v", err)
	}

	expectedSize := int64(len(serialized))
	if size != expectedSize {
		t.Errorf("expected file size %d, got %d", expectedSize, size)
	}
}

// TestLogReader_GetFileSize_EmptyFile tests getting size of empty file
func TestLogReader_GetFileSize_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "empty_size.log")

	// Create empty file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	size, err := reader.GetFileSize()
	if err != nil {
		t.Fatalf("GetFileSize failed: %v", err)
	}

	if size != 0 {
		t.Errorf("expected file size 0, got %d", size)
	}
}

// TestLogReader_Close tests closing the reader
func TestLogReader_Close(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "close.log")

	// Create file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}

	err = reader.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Note: Calling Close() again on an already closed file will return an error
	// This is expected OS-level behavior
}

// TestLogReader_CorruptedRecord tests handling corrupted records
func TestLogReader_CorruptedRecord(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "corrupted.log")

	// Write corrupted data
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Write some garbage data
	_, err = file.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00})
	if err != nil {
		t.Fatalf("failed to write corrupted data: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	_, err = reader.ReadNext()
	if err == nil {
		t.Error("expected error when reading corrupted record")
	}
}

// TestLogReader_InvalidRecordSize tests handling invalid record size
func TestLogReader_InvalidRecordSize(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "invalid_size.log")

	// Write invalid record size (exceeds MaxLogRecordSize)
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Write size that exceeds max (11 MB when max is 10 MB)
	invalidSize := uint32(11 * 1024 * 1024)
	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(invalidSize >> 24)
	sizeBuf[1] = byte(invalidSize >> 16)
	sizeBuf[2] = byte(invalidSize >> 8)
	sizeBuf[3] = byte(invalidSize)

	_, err = file.Write(sizeBuf)
	if err != nil {
		t.Fatalf("failed to write invalid size: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	_, err = reader.ReadNext()
	if err == nil {
		t.Error("expected error when reading record with invalid size")
	}
}

// TestLogReader_IncompleteRecord tests handling incomplete records
func TestLogReader_IncompleteRecord(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "incomplete.log")

	// Create a valid record
	tid := primitives.NewTransactionID()
	rec := record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN)

	serialized, err := record.SerializeLogRecord(rec)
	if err != nil {
		t.Fatalf("failed to serialize record: %v", err)
	}

	// Write only partial record
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Write only half of the record
	_, err = file.Write(serialized[:len(serialized)/2])
	if err != nil {
		t.Fatalf("failed to write incomplete record: %v", err)
	}
	file.Close()

	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	_, err = reader.ReadNext()
	if err == nil {
		t.Error("expected error when reading incomplete record")
	}
}

// TestLogReader_DifferentRecordTypes tests reading different types of records
func TestLogReader_DifferentRecordTypes(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "types.log")

	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 1)

	testCases := []struct {
		name   string
		record *record.LogRecord
	}{
		{"BeginRecord", record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN)},
		{"CommitRecord", record.NewLogRecord(record.CommitRecord, tid, nil, nil, nil, FirstLSN)},
		{"AbortRecord", record.NewLogRecord(record.AbortRecord, tid, nil, nil, nil, FirstLSN)},
		{"UpdateRecord", record.NewLogRecord(record.UpdateRecord, tid, pageID, []byte("old"), []byte("new"), FirstLSN)},
		{"InsertRecord", record.NewLogRecord(record.InsertRecord, tid, pageID, nil, []byte("data"), FirstLSN)},
		{"DeleteRecord", record.NewLogRecord(record.DeleteRecord, tid, pageID, []byte("deleted"), nil, FirstLSN)},
		{"CheckpointBegin", record.NewLogRecord(record.CheckpointBegin, nil, nil, nil, nil, FirstLSN)},
		{"CheckpointEnd", record.NewLogRecord(record.CheckpointEnd, nil, nil, nil, nil, FirstLSN)},
		{"CLRRecord", record.NewLogRecord(record.CLRRecord, tid, pageID, []byte("undo"), []byte("redo"), FirstLSN)},
	}

	// Write all record types to file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	for _, tc := range testCases {
		serialized, err := record.SerializeLogRecord(tc.record)
		if err != nil {
			t.Fatalf("failed to serialize %s: %v", tc.name, err)
		}
		_, err = file.Write(serialized)
		if err != nil {
			t.Fatalf("failed to write %s: %v", tc.name, err)
		}
	}
	file.Close()

	// Read all records back
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	for _, tc := range testCases {
		record, err := reader.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext failed for %s: %v", tc.name, err)
		}

		if record.Type != tc.record.Type {
			t.Errorf("%s: expected Type %v, got %v", tc.name, tc.record.Type, record.Type)
		}
	}
}

// TestLogReader_LSNAssignment tests that LSNs are correctly assigned
func TestLogReader_LSNAssignment(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "lsn.log")

	// Create records with different sizes
	tid := primitives.NewTransactionID()
	pageID := heap.NewHeapPageID(1, 1)

	records := []*record.LogRecord{
		record.NewLogRecord(record.BeginRecord, tid, nil, nil, nil, FirstLSN),
		record.NewLogRecord(record.UpdateRecord, tid, pageID, []byte("before"), []byte("after"), FirstLSN),
		record.NewLogRecord(record.CommitRecord, tid, nil, nil, nil, FirstLSN),
	}

	// Write records and track their sizes
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	var expectedLSNs []primitives.LSN
	currentOffset := int64(0)

	for _, rec := range records {
		serialized, err := record.SerializeLogRecord(rec)
		if err != nil {
			t.Fatalf("failed to serialize record: %v", err)
		}

		expectedLSNs = append(expectedLSNs, primitives.LSN(currentOffset))
		currentOffset += int64(len(serialized))

		_, err = file.Write(serialized)
		if err != nil {
			t.Fatalf("failed to write record: %v", err)
		}
	}
	file.Close()

	// Read records and verify LSNs
	reader, err := NewLogReader(logPath)
	if err != nil {
		t.Fatalf("NewLogReader failed: %v", err)
	}
	defer reader.Close()

	for i, expectedLSN := range expectedLSNs {
		record, err := reader.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext failed at record %d: %v", i, err)
		}

		if record.LSN != expectedLSN {
			t.Errorf("record %d: expected LSN %d, got %d", i, expectedLSN, record.LSN)
		}
	}
}
