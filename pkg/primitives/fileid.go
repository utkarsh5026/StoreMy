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
