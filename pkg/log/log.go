package log

import (
	"storemy/pkg/primitives"
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

// LogRecord represents a single entry in the WAL
type LogRecord struct {
	LSN     primitives.LSN // Unique identifier for this record
	Type    LogRecordType
	TID     *primitives.TransactionID
	PrevLSN primitives.LSN

	PageID      tuple.PageID // Affected page
	BeforeImage []byte       // Page state before modification (for UNDO)
	AfterImage  []byte       // Page state after modification (for REDO)

	UndoNextLSN primitives.LSN // Next record to undo (for CLR records)
	Timestamp   time.Time
}

// TransactionLogInfo tracks logging information for a transaction
type TransactionLogInfo struct {
	FirstLSN    primitives.LSN
	LastLSN     primitives.LSN
	UndoNextLSN primitives.LSN
}

func NewLogRecord(logType LogRecordType, tid *primitives.TransactionID, pageId tuple.PageID, beforeImage, afterImage []byte, prevLSN primitives.LSN) *LogRecord {
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
