package primitives

// LSN (Log Sequence Number) uniquely identifies each log record
// It's monotonically increasing and represents the byte offset in the log file
type LSN uint64

// HashCode represents a hash value (e.g., for keys, page IDs, etc.)
// It is typically computed for fast comparisons or lookups.
type HashCode uint64

// TableID uniquely identifies a database table within the system.
// It is used by pages, indexes, and transactions to refer to a table.
type TableID uint64

// SlotID represents a slot number within a page (for tuple storage)
type SlotID uint16

// PageNumber represents a page number within a table
type PageNumber uint64

// LockID uniquely identifies a lock (could be hash of resource)
type LockID uint64

// Timestamp represents a logical or physical timestamp
type Timestamp uint64

// ColumnID identifies a column within a table
type ColumnID uint32

// IndexID uniquely identifies an index
type IndexID uint64

// CheckpointID identifies a checkpoint in the recovery log
type CheckpointID uint64

// Offset represents a byte offset (within page, file, or log)
type Offset uint32

// RowID uniquely identifies a row within a table
type RowID uint64
