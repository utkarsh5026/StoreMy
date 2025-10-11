package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"time"
)

const (
	RecordSize    = 4 // Size field: total record size in bytes (uint32)
	TypeSize      = 1 // Type field: log record type (byte)
	TIDSize       = 8 // Transaction ID field (uint64)
	PrevLSNSize   = 8 // Previous LSN field (uint64)
	TimestampSize = 8 // Timestamp field (uint64, Unix timestamp)
)

// SerializeLogRecord converts a LogRecord struct into a compact binary representation.
// The serialization format uses big-endian byte ordering for cross-platform compatibility.
func SerializeLogRecord(record *LogRecord) ([]byte, error) {
	return record.Serialize()
}

// DeserializeLogRecord converts a binary representation back into a LogRecord struct.
// It validates the input data and ensures proper error handling for corrupted or invalid records.
//
// Binary format structure (must match SerializeLogRecord):
//
//	[Size:4][Type:1][TID:8][PrevLSN:8][Timestamp:8][Type-specific data]
func DeserializeLogRecord(data []byte) (*LogRecord, error) {
	if len(data) < RecordSize {
		return nil, fmt.Errorf("invalid record: data too short (%d bytes, minimum %d required)", len(data), RecordSize)
	}

	recordSize := binary.BigEndian.Uint32(data[:RecordSize])
	if recordSize != uint32(len(data)) {
		return nil, fmt.Errorf("size mismatch: header indicates %d bytes, actual %d bytes", recordSize, len(data))
	}

	minSize := RecordSize + TypeSize + TIDSize + PrevLSNSize + TimestampSize
	if len(data) < minSize {
		return nil, fmt.Errorf("invalid record: data too short (%d bytes, minimum %d required)", len(data), minSize)
	}

	buf := bytes.NewReader(data[RecordSize:])
	record := &LogRecord{}

	var recordType byte
	if err := binary.Read(buf, binary.BigEndian, &recordType); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}
	record.Type = LogRecordType(recordType)

	var tidVal uint64
	if err := binary.Read(buf, binary.BigEndian, &tidVal); err != nil {
		return nil, fmt.Errorf("failed to read transaction ID: %w", err)
	}
	if tidVal != 0 {
		record.TID = primitives.NewTransactionIDFromValue(int64(tidVal))
	}

	var prevLSN uint64
	if err := binary.Read(buf, binary.BigEndian, &prevLSN); err != nil {
		return nil, fmt.Errorf("failed to read PrevLSN: %w", err)
	}
	record.PrevLSN = primitives.LSN(prevLSN)

	var timestamp uint64
	if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	record.Timestamp = time.Unix(int64(timestamp), 0)

	switch record.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		if err := deserializeDataModification(buf, record); err != nil {
			return nil, fmt.Errorf("failed to deserialize data modification record: %w", err)
		}
	case CLRRecord:
		if err := deserializeCLR(buf, record); err != nil {
			return nil, fmt.Errorf("failed to deserialize CLR record: %w", err)
		}
	case BeginRecord, CommitRecord, AbortRecord, CheckpointBegin, CheckpointEnd:
	default:
		return nil, fmt.Errorf("unknown record type: %d", record.Type)
	}

	return record, nil
}

// deserializeDataModification deserializes data modification records (Insert, Update, Delete).
// Reconstructs the PageID, BeforeImage, and AfterImage fields from binary format.
//
// This is critical for ARIES recovery phases:
//   - Analysis phase: Identifies which pages were modified
//   - Redo phase: Applies AfterImage to reconstruct database state
//   - Undo phase: Applies BeforeImage to rollback incomplete transactions
func deserializeDataModification(buf *bytes.Reader, record *LogRecord) error {
	pageID, err := deserializePageID(buf)
	if err != nil {
		return err
	}
	record.PageID = pageID

	beforeImage, err := deserializeImage(buf)
	if err != nil {
		return fmt.Errorf("failed to deserialize BeforeImage: %w", err)
	}
	record.BeforeImage = beforeImage

	afterImage, err := deserializeImage(buf)
	if err != nil {
		return fmt.Errorf("failed to deserialize AfterImage: %w", err)
	}
	record.AfterImage = afterImage

	return nil
}

// deserializeCLR deserializes Compensation Log Records (CLR).
func deserializeCLR(buf *bytes.Reader, record *LogRecord) error {
	pageID, err := deserializePageID(buf)
	if err != nil {
		return err
	}
	record.PageID = pageID

	var undoNextLSN uint64
	if err := binary.Read(buf, binary.BigEndian, &undoNextLSN); err != nil {
		return fmt.Errorf("failed to read UndoNextLSN: %w", err)
	}
	record.UndoNextLSN = primitives.LSN(undoNextLSN)

	afterImage, err := deserializeImage(buf)
	if err != nil {
		return fmt.Errorf("failed to deserialize AfterImage: %w", err)
	}
	record.AfterImage = afterImage

	return nil
}

// deserializePageID deserializes a PageID from the buffer.
// PageID is serialized as two uint32 values: tableID and pageNo.
func deserializePageID(buf *bytes.Reader) (primitives.PageID, error) {
	var tableID, pageNo uint32

	if err := binary.Read(buf, binary.BigEndian, &tableID); err != nil {
		return nil, fmt.Errorf("failed to read PageID tableID: %w", err)
	}

	if err := binary.Read(buf, binary.BigEndian, &pageNo); err != nil {
		return nil, fmt.Errorf("failed to read PageID pageNo: %w", err)
	}

	return heap.NewHeapPageID(int(tableID), int(pageNo)), nil
}

// deserializeImage deserializes a byte slice image (BeforeImage or AfterImage).
// The format is: [length:4][data:length] where length is uint32.
func deserializeImage(buf *bytes.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read image length: %w", err)
	}

	if length == 0 {
		return nil, nil
	}

	const maxImageSize = 100 * 1024 * 1024 // 100MB limit
	if length > maxImageSize {
		return nil, fmt.Errorf("image size too large: %d bytes (max %d)", length, maxImageSize)
	}

	image := make([]byte, length)
	n, err := buf.Read(image)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read image data: %w", err)
	}
	if n != int(length) {
		return nil, fmt.Errorf("incomplete image data: expected %d bytes, got %d", length, n)
	}

	return image, nil
}
