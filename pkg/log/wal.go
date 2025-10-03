package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"sync"
	"time"
)

const (
	FirstLSN LSN = 0
)

// WAL manages the write-ahead log
type WAL struct {
	file       *os.File // The log file
	currentLSN LSN      // Current position in log
	flushedLSN LSN      // Last LSN written to disk

	// Transaction tracking
	activeTxns map[*transaction.TransactionID]*TransactionLogInfo // Active transactions

	// Page tracking for recovery
	dirtyPages map[tuple.PageID]LSN // Dirty pages and their recLSN

	// Synchronization
	mutex     sync.RWMutex // Protects WAL structures
	flushCond *sync.Cond   // For coordinating flushes

	// Configuration
	bufferSize   int    // Size of write buffer
	writeBuffer  []byte // Buffer for batching writes
	bufferOffset int    // Current position in buffer
}

// NewWAL creates a new WAL instance
func NewWAL(logPath string, bufferSize int) (*WAL, error) {
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to end of WAL: %v", err)
	}

	w := &WAL{
		file:         file,
		currentLSN:   LSN(pos),
		flushedLSN:   LSN(pos),
		activeTxns:   make(map[*transaction.TransactionID]*TransactionLogInfo),
		dirtyPages:   make(map[tuple.PageID]LSN),
		bufferSize:   bufferSize,
		writeBuffer:  make([]byte, bufferSize),
		bufferOffset: 0,
	}

	w.flushCond = sync.NewCond(&w.mutex)
	return w, nil
}

func (w *WAL) LogBegin(tid *transaction.TransactionID) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	record := &LogRecord{
		Type:      BeginRecord,
		TID:       tid,
		PrevLSN:   FirstLSN,
		Timestamp: time.Now(),
	}

	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	w.activeTxns[tid] = &TransactionLogInfo{
		FirstLSN: lsn,
		LastLSN:  lsn,
	}
	return lsn, nil
}

// LogUpdate logs a page update with before and after images
// This is called BEFORE the page is actually modified in memory
func (w *WAL) LogUpdate(tid *transaction.TransactionID,
	pageID tuple.PageID,
	beforeImage, afterImage []byte) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return 0, fmt.Errorf("transaction %v not found in active transactions", tid)
	}

	record := NewLogRecord(UpdateRecord, tid, pageID, beforeImage, afterImage, txnInfo.LastLSN)

	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	txnInfo.LastLSN = lsn

	if _, exists := w.dirtyPages[pageID]; !exists {
		w.dirtyPages[pageID] = lsn
	}

	return lsn, nil
}

// LogInsert logs a tuple insertion
// Only needs after image - there's nothing to undo to (tuple didn't exist)
// During recovery, we REDO the insert by applying the after image
func (w *WAL) LogInsert(tid *transaction.TransactionID, pageID tuple.PageID, afterImage []byte) (LSN, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	txnInfo, exists := w.activeTxns[tid]
	if !exists {
		return 0, fmt.Errorf("transaction %v not found in active transactions", tid)
	}

	record := NewLogRecord(InsertRecord, tid, pageID, nil, afterImage, txnInfo.LastLSN)

	lsn, err := w.writeRecord(record)
	if err != nil {
		return 0, err
	}

	txnInfo.LastLSN = lsn
	if _, exists := w.dirtyPages[pageID]; !exists {
		w.dirtyPages[pageID] = lsn
	}

	return lsn, nil
}

func (w *WAL) writeRecord(record *LogRecord) (LSN, error) {
	data, err := w.serializeRecord(record)
	if err != nil {
		return 0, err
	}

	record.LSN = w.currentLSN
	if w.bufferOffset+len(data) > w.bufferSize {
		if err := w.flushBuffer(); err != nil {
			return 0, err
		}
	}

	copy(w.writeBuffer[w.bufferOffset:], data)
	w.bufferOffset += len(data)

	w.currentLSN += LSN(len(data))
	return record.LSN, nil
}

// Force ensures all log records up to the given LSN are on disk
// This is called during commit to ensure durability
func (w *WAL) Force(lsn LSN) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if lsn < w.currentLSN && w.flushedLSN >= w.currentLSN {
		return nil
	}

	if w.currentLSN > w.flushedLSN {
		return w.flushBuffer()
	}

	return nil
}

// flushBuffer writes buffered log records to disk
func (w *WAL) flushBuffer() error {
	if w.bufferOffset == 0 {
		return nil
	}

	n, err := w.file.Write(w.writeBuffer[:w.bufferOffset])
	if err != nil {
		return fmt.Errorf("failed to write WAL buffer: %v", err)
	}

	if n != w.bufferOffset {
		return fmt.Errorf("partial write to WAL: wrote %d of %d bytes", n, w.bufferOffset)
	}

	w.flushedLSN = w.currentLSN
	w.bufferOffset = 0
	w.flushCond.Broadcast()

	return nil
}

// serializeRecord converts a LogRecord to bytes
func (w *WAL) serializeRecord(record *LogRecord) ([]byte, error) {
	// Format: [RecordSize][Type][TID][PrevLSN][Timestamp][Type-specific data]

	baseSize := 4 + 1 + 8 + 8 + 8 // Size + Type + TID + PrevLSN + Timestamp
	dataSize := 0

	switch record.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		dataSize = 8
		if record.BeforeImage != nil {
			dataSize += 4 + len(record.BeforeImage)
		}
		if record.AfterImage != nil {
			dataSize += 4 + len(record.AfterImage)
		}
	case CLRRecord:
		dataSize = 8 // UndoNextLSN
	}

	totalSize := baseSize + dataSize
	buf := make([]byte, totalSize)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalSize))
	offset += 4

	buf[offset] = byte(record.Type)
	offset++

	tid := uint64(0) // Convert your TransactionID to uint64
	if record.TID != nil {
		// You'll need to implement a method to get numeric ID from TransactionID
		tid = uint64(record.TID.ID())
	}
	binary.BigEndian.PutUint64(buf[offset:], tid)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(record.PrevLSN))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(record.Timestamp.Unix()))
	offset += 8

	switch record.Type {
	case UpdateRecord, InsertRecord, DeleteRecord:
		// Write PageID (you'll need to serialize your PageID type)
		// This is a simplified version
		pageIDBytes := record.PageID.Serialize()
		for _, b := range pageIDBytes {
			binary.BigEndian.PutUint32(buf[offset:], uint32(b))
			offset += 4
		}

		// Write before image if present
		if record.BeforeImage != nil {
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(record.BeforeImage)))
			offset += 4
			copy(buf[offset:], record.BeforeImage)
			offset += len(record.BeforeImage)
		}

		// Write after image if present
		if record.AfterImage != nil {
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(record.AfterImage)))
			offset += 4
			copy(buf[offset:], record.AfterImage)
			offset += len(record.AfterImage)
		}
	}

	return buf, nil
}
