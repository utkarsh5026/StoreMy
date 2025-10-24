package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"storemy/pkg/log/record"
	"storemy/pkg/primitives"
)

const (
	MaxLogRecordSize = 10 * 1024 * 1024 // 10 MB max record size
)

// LogReader reads and deserializes log records from a WAL file
// It provides sequential access to all records in the log
type LogReader struct {
	file   *os.File
	offset int64
}

// NewLogReader creates a new log reader for the specified file
func NewLogReader(logPath string) (*LogReader, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return &LogReader{
		file:   file,
		offset: 0,
	}, nil
}

// ReadNext reads the next log record from the file
// Returns nil when EOF is reached
func (lr *LogReader) ReadNext() (*record.LogRecord, error) {
	recLen, err := readHeader(lr.file, lr.offset)
	if err != nil {
		return nil, err
	}

	recordBuf, err := readRecordBytes(lr.file, int64(recLen-record.RecordSize), lr.offset+record.RecordSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read record bytes at offset %d: %w", lr.offset, err)
	}

	fullRecord := make([]byte, recLen)
	binary.BigEndian.PutUint32(fullRecord[0:record.RecordSize], recLen)
	copy(fullRecord[record.RecordSize:], recordBuf)

	rec, err := record.DeserializeLogRecord(fullRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize record at offset %d: %w", lr.offset, err)
	}

	rec.LSN = primitives.LSN(lr.offset)
	lr.offset += int64(recLen)
	return rec, nil
}

// ReadAll reads all log records from the file
func (lr *LogReader) ReadAll() ([]*record.LogRecord, error) {
	var records []*record.LogRecord

	for {
		rec, err := lr.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

// Reset resets the reader to the beginning of the file
func (lr *LogReader) Reset() error {
	lr.offset = 0
	return nil
}

// Close closes the underlying file
func (lr *LogReader) Close() error {
	if lr.file != nil {
		return lr.file.Close()
	}
	return nil
}

// GetFileSize returns the total size of the log file
func (lr *LogReader) GetFileSize() (int64, error) {
	stat, err := lr.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func readHeader(file *os.File, offset int64) (uint32, error) {
	sizeBuf := make([]byte, record.RecordSize)
	n, err := file.ReadAt(sizeBuf, offset)
	if err == io.EOF || n == 0 {
		return 0, io.EOF
	}

	if err != nil {
		return 0, fmt.Errorf("failed to read record size: %w", err)
	}

	recordSize := binary.BigEndian.Uint32(sizeBuf)
	if recordSize == 0 || recordSize > MaxLogRecordSize { // Sanity check: max 10MB per record
		return 0, fmt.Errorf("invalid record size: %d at offset %d", recordSize, offset)
	}

	return recordSize, nil
}

func readRecordBytes(file *os.File, size, offset int64) ([]byte, error) {
	recordBuf := make([]byte, size)
	n, err := file.ReadAt(recordBuf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read record data: %w", err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("incomplete record: expected %d bytes, got %d", size, n)
	}

	return recordBuf, nil
}
