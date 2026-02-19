package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"storemy/pkg/primitives"
	"time"
)

type LSN = primitives.LSN

// LogRecordType represents different types of log records
type LogRecordType uint8

const (
	BeginRecord LogRecordType = iota
	CommitRecord
	AbortRecord

	UpdateRecord
	InsertRecord
	DeleteRecord

	CheckpointBegin
	CheckpointEnd

	CLRRecord
)

// LogRecord represents a single entry in the WAL
type LogRecord struct {
	LSN     LSN // Unique identifier for this record
	Type    LogRecordType
	TID     *primitives.TransactionID
	PrevLSN LSN

	PageID      primitives.PageID // Affected page
	BeforeImage []byte            // Page state before modification (for UNDO)
	AfterImage  []byte            // Page state after modification (for REDO)

	UndoNextLSN LSN // Next record to undo (for CLR records)
	Timestamp   time.Time
}

// TransactionLogInfo tracks logging information for a transaction
type TransactionLogInfo struct {
	FirstLSN, LastLSN, UndoNextLSN LSN
}

func NewLogRecord(logType LogRecordType, tid *primitives.TransactionID, pageId primitives.PageID, beforeImage, afterImage []byte, prevLSN LSN) *LogRecord {
	return &LogRecord{
		Type:        logType,
		TID:         tid,
		PageID:      pageId,
		BeforeImage: beforeImage,
		AfterImage:  afterImage,
		Timestamp:   time.Now(),
		PrevLSN:     prevLSN,
	}
}

// Serialize converts a LogRecord struct into a compact binary representation.
// The serialization format uses big-endian byte ordering for cross-platform compatibility.
//
// Binary format structure:
//
//	[Size:4][Type:1][TID:8][PrevLSN:8][Timestamp:8][Type-specific data]
//
// Type-specific data varies based on record type:
//   - UpdateRecord/InsertRecord/DeleteRecord: PageID + BeforeImage + AfterImage
//   - CLRRecord: PageID + UndoNextLSN + AfterImage
//   - BeginRecord/CommitRecord/AbortRecord: No additional data
//   - CheckpointBegin/CheckpointEnd: No additional data (checkpoint records handled separately)
//
// The Size field at the start includes the entire record length for efficient log scanning.
// PrevLSN creates a linked list of records per transaction, crucial for ARIES rollback.
//
// Returns serialized byte slice, or error if serialization fails.
func (l *LogRecord) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	tidVal := uint64(0)
	if l.TID != nil {
		tidVal = uint64(l.TID.ID()) // #nosec G115
	}

	writes := []any{
		byte(l.Type),
		tidVal,
		uint64(l.PrevLSN),
		uint64(l.Timestamp.Unix()), // #nosec G115
	}

	for _, v := range writes {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return nil, fmt.Errorf("failed to write base field: %w", err)
		}
	}

	switch l.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		if err := l.serializeDataModification(&buf); err != nil {
			return nil, err
		}
	case CLRRecord:
		if err := l.serializeCLR(&buf); err != nil {
			return nil, err
		}
	}

	data := buf.Bytes()
	result := make([]byte, RecordSize+len(data))
	binary.BigEndian.PutUint32(result, uint32(len(result))) // #nosec G115
	copy(result[RecordSize:], data)

	return result, nil
}

// serializeDataModification serializes data modification records (Insert, Update, Delete).
// These records contain a PageID, BeforeImage (for updates/deletes), and AfterImage.
func (l *LogRecord) serializeDataModification(buf *bytes.Buffer) error {
	if err := l.serializePageID(buf); err != nil {
		return err
	}
	if err := l.serializeImage(buf, l.BeforeImage); err != nil {
		return err
	}
	return l.serializeImage(buf, l.AfterImage)
}

// serializeCLR serializes Compensation Log Records (CLR).
// CLRs are used during transaction rollback and contain PageID, UndoNextLSN, and AfterImage.
func (l *LogRecord) serializeCLR(buf *bytes.Buffer) error {
	if err := l.serializePageID(buf); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(l.UndoNextLSN)); err != nil {
		return fmt.Errorf("failed to write UndoNextLSN: %w", err)
	}
	return l.serializeImage(buf, l.AfterImage)
}

// serializePageID serializes a PageID as two uint32 fields: FileID and PageNo.
func (l *LogRecord) serializePageID(buf *bytes.Buffer) error {
	if l.PageID == nil {
		return nil
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(l.PageID.FileID())); err != nil {
		return fmt.Errorf("failed to write PageID fileID: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(l.PageID.PageNo())); err != nil {
		return fmt.Errorf("failed to write PageID pageNo: %w", err)
	}
	return nil
}

// serializeImage serializes a byte slice image (BeforeImage or AfterImage).
// The format is: [length:4][data:length] where length is uint32.
// If the image is nil, only a zero length is written.
func (l *LogRecord) serializeImage(buf *bytes.Buffer, image []byte) error {
	length := uint32(0)
	if image != nil {
		length = uint32(len(image)) // #nosec G115
	}

	if err := binary.Write(buf, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write image length: %w", err)
	}

	if image != nil {
		if _, err := buf.Write(image); err != nil {
			return fmt.Errorf("failed to write image data: %w", err)
		}
	}
	return nil
}
