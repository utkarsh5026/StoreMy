package log

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"time"
)

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

// LSN (Log Sequence Number) uniquely identifies each log record
// It's monotonically increasing and represents the byte offset in the log file
type LSN uint64

// LogRecord represents a single entry in the WAL
type LogRecord struct {
	LSN     LSN // Unique identifier for this record
	Type    LogRecordType
	TID     *transaction.TransactionID
	PrevLSN LSN

	PageID      tuple.PageID // Affected page
	BeforeImage []byte       // Page state before modification (for UNDO)
	AfterImage  []byte       // Page state after modification (for REDO)

	UndoNextLSN LSN // Next record to undo (for CLR records)
	Timestamp   time.Time
}

// TransactionLogInfo tracks logging information for a transaction
type TransactionLogInfo struct {
	FirstLSN    LSN
	LastLSN     LSN
	UndoNextLSN LSN
}

func NewLogRecord(logType LogRecordType, tid *transaction.TransactionID, pageId tuple.PageID, beforeImage, afterImage []byte, prevLSN LSN) *LogRecord {
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
