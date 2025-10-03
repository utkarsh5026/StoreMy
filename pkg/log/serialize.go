package log

import (
	"bytes"
	"encoding/binary"
)

// Size of the serialized log record in bytes
const (
	RecordSize    = 4
	TypeSize      = 1
	TIDSize       = 8
	PrevLSNSize   = 8
	TimestampSize = 8
)

// Use binary encoding for compact representation
// Format: [Size][Type][TID][PrevLSN][Timestamp][Type-specific data]
func SerializeLogRecord(record *LogRecord) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(byte(record.Type))

	tidVal := uint64(0)
	if record.TID != nil {
		tidVal = uint64(record.TID.ID())
	}
	binary.Write(&buf, binary.BigEndian, tidVal)

	binary.Write(&buf, binary.BigEndian, uint64(record.PrevLSN))
	binary.Write(&buf, binary.BigEndian, uint64(record.Timestamp.Unix()))

	switch record.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		serializeDataModification(&buf, record)
	case CLRRecord:
		serializeCLR(&buf, record)
	}

	data := buf.Bytes()
	result := make([]byte, RecordSize+len(data))
	binary.BigEndian.PutUint32(result, uint32(len(result)))
	copy(result[RecordSize:], data)

	return result, nil
}

func serializeDataModification(buf *bytes.Buffer, record *LogRecord) {
	if record.PageID != nil {
		pageIDBytes := record.PageID.Serialize()
		for _, b := range pageIDBytes {
			binary.Write(buf, binary.BigEndian, uint32(b))
		}
	}

	serializeImage(buf, record.BeforeImage)
	serializeImage(buf, record.AfterImage)
}

func serializeCLR(buf *bytes.Buffer, record *LogRecord) {
	if record.PageID != nil {
		pageIDBytes := record.PageID.Serialize()
		for _, b := range pageIDBytes {
			binary.Write(buf, binary.BigEndian, uint32(b))
		}
	}

	binary.Write(buf, binary.BigEndian, uint64(record.UndoNextLSN))
	if record.AfterImage != nil {
		binary.Write(buf, binary.BigEndian, uint32(len(record.AfterImage)))
		buf.Write(record.AfterImage)
	} else {
		binary.Write(buf, binary.BigEndian, uint32(0))
	}
}

func serializeImage(buf *bytes.Buffer, image []byte) {
	if image != nil {
		binary.Write(buf, binary.BigEndian, uint32(len(image)))
		buf.Write(image)
	} else {
		binary.Write(buf, binary.BigEndian, uint32(0))
	}
}
