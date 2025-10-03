package log

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"time"
)

// LogRecordType represents different types of log records
type LogRecordType uint8

const (
	BeginRecord  LogRecordType = iota // Transaction start
	CommitRecord                      // Transaction commit
	AbortRecord                       // Transaction abort

	UpdateRecord // Page update (contains before and after images)
	InsertRecord // Tuple insertion
	DeleteRecord // Tuple deletion

	CheckpointBegin // Start of checkpoint
	CheckpointEnd   // End of checkpoint

	CLRRecord // Used during rollback
)

// LSN (Log Sequence Number) uniquely identifies each log record
// It's monotonically increasing and represents the byte offset in the log file
type LSN uint64

// LogRecord represents a single entry in the WAL
type LogRecord struct {
	LSN     LSN                        // Unique identifier for this record
	Type    LogRecordType              // Type of operation
	TID     *transaction.TransactionID // Transaction that created this record
	PrevLSN LSN                        // Previous LSN for this transaction (for backward scanning)

	PageID      tuple.PageID // Affected page
	BeforeImage []byte       // Page state before modification (for UNDO)
	AfterImage  []byte       // Page state after modification (for REDO)

	UndoNextLSN LSN // Next record to undo (for CLR records)
	Timestamp   time.Time
}

// TransactionLogInfo tracks logging information for a transaction
type TransactionLogInfo struct {
	FirstLSN    LSN // First log record for this transaction
	LastLSN     LSN // Most recent log record
	UndoNextLSN LSN // Next record to undo during rollback
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
