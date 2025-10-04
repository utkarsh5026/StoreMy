package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
//
// Binary format structure:
//
//	[Size:4][Type:1][TID:8][PrevLSN:8][Timestamp:8][Type-specific data]
//
// Type-specific data varies based on record type:
//   - UpdateRecord/InsertRecord/DeleteRecord: PageID + BeforeImage + AfterImage
//   - CLRRecord: PageID + UndoNextLSN + AfterImage
func SerializeLogRecord(record *LogRecord) ([]byte, error) {
	var buf bytes.Buffer

	tidVal := uint64(0)
	if record.TID != nil {
		tidVal = uint64(record.TID.ID())
	}

	writes := []any{
		byte(record.Type),
		tidVal,
		uint64(record.PrevLSN),
		uint64(record.Timestamp.Unix()),
	}

	for _, v := range writes {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return nil, fmt.Errorf("failed to write base field: %w", err)
		}
	}

	switch record.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		if err := serializeDataModification(&buf, record); err != nil {
			return nil, err
		}
	case CLRRecord:
		if err := serializeCLR(&buf, record); err != nil {
			return nil, err
		}
	}

	data := buf.Bytes()
	result := make([]byte, RecordSize+len(data))
	binary.BigEndian.PutUint32(result, uint32(len(result)))
	copy(result[RecordSize:], data)

	return result, nil
}

// serializeDataModification serializes data modification records (Insert, Update, Delete).
// These records contain a PageID, BeforeImage (for updates/deletes), and AfterImage.
func serializeDataModification(buf *bytes.Buffer, record *LogRecord) error {
	if err := serializePageID(buf, record.PageID); err != nil {
		return err
	}
	if err := serializeImage(buf, record.BeforeImage); err != nil {
		return err
	}
	return serializeImage(buf, record.AfterImage)
}

// serializeCLR serializes Compensation Log Records (CLR).
// CLRs are used during transaction rollback and contain PageID, UndoNextLSN, and AfterImage.
func serializeCLR(buf *bytes.Buffer, record *LogRecord) error {
	if err := serializePageID(buf, record.PageID); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(record.UndoNextLSN)); err != nil {
		return fmt.Errorf("failed to write UndoNextLSN: %w", err)
	}
	return serializeImage(buf, record.AfterImage)
}

// serializePageID serializes a PageID by calling its Serialize() method.
func serializePageID(buf *bytes.Buffer, pageID interface{ Serialize() []int }) error {
	if pageID == nil {
		return nil
	}
	for _, b := range pageID.Serialize() {
		if err := binary.Write(buf, binary.BigEndian, uint32(b)); err != nil {
			return fmt.Errorf("failed to write PageID: %w", err)
		}
	}
	return nil
}

// serializeImage serializes a byte slice image (BeforeImage or AfterImage).
// The format is: [length:4][data:length] where length is uint32.
// If the image is nil, only a zero length is written.
func serializeImage(buf *bytes.Buffer, image []byte) error {
	length := uint32(0)
	if image != nil {
		length = uint32(len(image))
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
