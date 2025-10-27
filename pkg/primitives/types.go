package primitives

// LSN (Log Sequence Number) uniquely identifies each log record
// It's monotonically increasing and represents the byte offset in the log file
type LSN uint64

// HashCode represents a hash value (e.g., for keys, page IDs, etc.)
// It is typically computed for fast comparisons or lookups.
type HashCode uint64

// FileID is the base type representing a unique file identifier derived from hashing a file path.
// It serves as the foundation for both TableID and IndexID, representing the physical file's identity.
//
// FileID is generated using FNV-1a hash of the file path and provides:
//   - Deterministic identification: Same path always produces same ID
//   - Fast lookups in hash-based data structures
//   - Collision resistance for different paths
//
// This type is typically not used directly - instead use TableID or IndexID for semantic clarity.
type FileID uint64

// TableID uniquely identifies a database table within the system.
// It represents the hash of the table's heap file path and is used throughout
// the system to reference tables in pages, indexes, transactions, and the buffer pool.
//
// TableID is fundamentally a FileID but maintains type safety to prevent
// accidental mixing with IndexID or other file identifiers.
type TableID FileID

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

// IndexID uniquely identifies an index within the system.
// It represents the hash of the index file path and is used throughout
// the system to reference indexes in the buffer pool and index manager.
//
// IndexID is fundamentally a FileID but maintains type safety to prevent
// accidental mixing with TableID or other file identifiers.
type IndexID FileID

// CheckpointID identifies a checkpoint in the recovery log
type CheckpointID uint64

// Offset represents a byte offset (within page, file, or log)
type Offset uint32

// RowID uniquely identifies a row within a table
type RowID uint64
