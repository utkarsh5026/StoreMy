package primitives

import "fmt"

// FileID Methods
// =============================================================================

// IsValid checks if the FileID is a valid non-zero identifier.
// A FileID of 0 is typically considered invalid or uninitialized.
func (f FileID) IsValid() bool {
	return f != 0
}

// AsUint64 returns the FileID as a uint64 for serialization or storage.
func (f FileID) AsUint64() uint64 {
	return uint64(f)
}

// String returns a string representation of the FileID.
func (f FileID) String() string {
	return fmt.Sprintf("FileID(%d)", f)
}

// TableID Methods
// =============================================================================

// ToFileID converts a TableID to its base FileID type.
// This is useful when you need to work with the underlying file identifier.
func (t TableID) ToFileID() FileID {
	return FileID(t)
}

// IsValid checks if the TableID is a valid non-zero identifier.
// A TableID of 0 is typically considered invalid or uninitialized.
func (t TableID) IsValid() bool {
	return t != 0
}

// AsUint64 returns the TableID as a uint64 for serialization or storage.
func (t TableID) AsUint64() uint64 {
	return uint64(t)
}

// String returns a string representation of the TableID.
func (t TableID) String() string {
	return fmt.Sprintf("TableID(%d)", t)
}

// AsIndexID converts a TableID to an IndexID.
// This should only be used in specific cases where a table file is being
// treated as an index file, or for internal conversions.
//
// WARNING: Use with caution. This bypasses type safety and should only be
// used when you're certain the semantic meaning is correct.
func (t TableID) AsIndexID() IndexID {
	return IndexID(t)
}

// IndexID Methods
// =============================================================================

// ToFileID converts an IndexID to its base FileID type.
// This is useful when you need to work with the underlying file identifier.
func (i IndexID) ToFileID() FileID {
	return FileID(i)
}

// IsValid checks if the IndexID is a valid non-zero identifier.
// An IndexID of 0 is typically considered invalid or uninitialized.
func (i IndexID) IsValid() bool {
	return i != 0
}

// AsUint64 returns the IndexID as a uint64 for serialization or storage.
func (i IndexID) AsUint64() uint64 {
	return uint64(i)
}

// String returns a string representation of the IndexID.
func (i IndexID) String() string {
	return fmt.Sprintf("IndexID(%d)", i)
}

// AsTableID converts an IndexID to a TableID.
// This should only be used in specific cases where an index file is being
// treated as a table file, or for internal conversions.
//
// WARNING: Use with caution. This bypasses type safety and should only be
// used when you're certain the semantic meaning is correct.
func (i IndexID) AsTableID() TableID {
	return TableID(i)
}

// Factory Functions
// =============================================================================

// NewFileIDFromUint64 creates a FileID from a uint64 value.
// This is useful when deserializing or loading IDs from storage.
func NewFileIDFromUint64(id uint64) FileID {
	return FileID(id)
}

// NewTableIDFromUint64 creates a TableID from a uint64 value.
// This is useful when deserializing or loading table IDs from storage.
func NewTableIDFromUint64(id uint64) TableID {
	return TableID(id)
}

// NewIndexIDFromUint64 creates an IndexID from a uint64 value.
// This is useful when deserializing or loading index IDs from storage.
func NewIndexIDFromUint64(id uint64) IndexID {
	return IndexID(id)
}

// NewTableIDFromFileID creates a TableID from a FileID.
// This is the safe way to convert a FileID to a TableID.
func NewTableIDFromFileID(fileID FileID) TableID {
	return TableID(fileID)
}

// NewIndexIDFromFileID creates an IndexID from a FileID.
// This is the safe way to convert a FileID to an IndexID.
func NewIndexIDFromFileID(fileID FileID) IndexID {
	return IndexID(fileID)
}
