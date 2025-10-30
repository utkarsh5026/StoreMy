package primitives

import "math"

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

// CheckpointID identifies a checkpoint in the recovery log
type CheckpointID uint64

// Offset represents a byte offset (within page, file, or log)
type Offset uint32

// RowID uniquely identifies a row within a table
type RowID uint64

// Sentinel values for invalid/unset identifiers
const (
	// InvalidPageNumber represents an invalid or unset page number
	// Used for: no parent page, no next/prev leaf, uninitialized references
	InvalidPageNumber PageNumber = 0

	// InvalidFileID represents an invalid or unset file ID
	InvalidFileID FileID = 0

	// InvalidSlotID represents an invalid or unset slot ID
	InvalidSlotID SlotID = 0

	InvalidColumnID ColumnID = math.MaxUint32
)
