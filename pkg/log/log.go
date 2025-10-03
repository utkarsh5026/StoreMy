package log

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"time"
)

// LogRecordType represents different types of log records
type LogRecordType uint8

const (
	// Transaction control records
	BeginRecord  LogRecordType = iota // Transaction start
	CommitRecord                      // Transaction commit
	AbortRecord                       // Transaction abort

	// Data modification records
	UpdateRecord // Page update (contains before and after images)
	InsertRecord // Tuple insertion
	DeleteRecord // Tuple deletion

	// Checkpoint records for recovery optimization
	CheckpointBegin // Start of checkpoint
	CheckpointEnd   // End of checkpoint

	// Compensation Log Record for undo operations
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

	// For data modification records
	PageID      tuple.PageID // Affected page
	BeforeImage []byte       // Page state before modification (for UNDO)
	AfterImage  []byte       // Page state after modification (for REDO)

	// For CLR records
	UndoNextLSN LSN // Next record to undo (for CLR records)

	// Timestamp for debugging and analysis
	Timestamp time.Time
}
