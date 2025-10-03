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

	if err := buf.WriteByte(byte(record.Type)); err != nil {
		return nil, err
	}

	tidVal := uint64(0)
	if record.TID != nil {
		tidVal = uint64(record.TID.ID())
	}
	if err := binary.Write(&buf, binary.BigEndian, tidVal); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, uint64(record.PrevLSN)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint64(record.Timestamp.Unix())); err != nil {
		return nil, err
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

func serializeDataModification(buf *bytes.Buffer, record *LogRecord) error {
	if record.PageID != nil {
		pageIDBytes := record.PageID.Serialize()
		for _, b := range pageIDBytes {
			if err := binary.Write(buf, binary.BigEndian, uint32(b)); err != nil {
				return err
			}
		}
	}

	if err := serializeImage(buf, record.BeforeImage); err != nil {
		return err
	}
	if err := serializeImage(buf, record.AfterImage); err != nil {
		return err
	}
	return nil
}

func serializeCLR(buf *bytes.Buffer, record *LogRecord) error {
	if record.PageID != nil {
		pageIDBytes := record.PageID.Serialize()
		for _, b := range pageIDBytes {
			if err := binary.Write(buf, binary.BigEndian, uint32(b)); err != nil {
				return err
			}
		}
	}

	if err := binary.Write(buf, binary.BigEndian, uint64(record.UndoNextLSN)); err != nil {
		return err
	}
	if record.AfterImage != nil {
		if err := binary.Write(buf, binary.BigEndian, uint32(len(record.AfterImage))); err != nil {
			return err
		}
		if _, err := buf.Write(record.AfterImage); err != nil {
			return err
		}
	} else {
		if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
			return err
		}
	}
	return nil
}

func serializeImage(buf *bytes.Buffer, image []byte) error {
	if image != nil {
		if err := binary.Write(buf, binary.BigEndian, uint32(len(image))); err != nil {
			return err
		}
		if _, err := buf.Write(image); err != nil {
			return err
		}
	} else {
		if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
			return err
		}
	}
	return nil
}
