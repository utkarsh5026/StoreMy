package primitives

import (
	"hash/fnv"
	"os"
	"path/filepath"
)

// Filepath is a type-safe wrapper around file paths used throughout the database system.
// It provides convenient methods for path manipulation and file operations while maintaining
// type safety and reducing the need for string conversions.
//
// The Filepath type is used for:
//   - Heap file paths (table data storage)
//   - Index file paths (B-tree, hash indexes)
//   - WAL log file paths
//   - System catalog file paths
//
// Example usage:
//
//	dataDir := primitives.Filepath("/data")
//	tablePath := dataDir.Join("users.dat")
//	if tablePath.Exists() {
//	    tablePath.Remove()
//	}
type Filepath string

// Hash generates a unique FileID from the file path using FNV-1a hashing.
// This hash is used as the physical file identifier throughout the system.
//
// The hash provides:
//   - Deterministic IDs for the same file path
//   - Fast lookup in hash-based data structures
//   - Collision resistance for different paths
//
// Returns:
//   - FileID: A 64-bit hash value representing this file path
//
// Example:
//
//	path := primitives.Filepath("/data/users.dat")
//	fileID := path.Hash() // Returns consistent ID for this path
//	tableID := path.HashAsTableID() // Or get directly as TableID
func (f Filepath) Hash() FileID {
	h := fnv.New64a()
	h.Write([]byte(f))
	return FileID(h.Sum64())
}

// Dir returns the directory portion of the file path.
// This is a wrapper around filepath.Dir that works directly with the Filepath type.
//
// Returns:
//   - string: The directory path containing this file
//
// Example:
//
//	path := primitives.Filepath("/data/indexes/users_id.idx")
//	dir := path.Dir() // Returns "/data/indexes"
func (f Filepath) Dir() string {
	return filepath.Dir(string(f))
}

// String converts the Filepath to a standard string.
// This implements the Stringer interface and provides cleaner conversion
// than explicit type casting.
//
// Returns:
//   - string: The file path as a string
//
// Example:
//
//	path := primitives.Filepath("/data/users.dat")
//	str := path.String() // Returns "/data/users.dat"
func (f Filepath) String() string {
	return string(f)
}

// Join concatenates path elements to this path and returns a new Filepath.
// This is a type-safe wrapper around filepath.Join that maintains the Filepath type.
//
// Parameters:
//   - elem: Variable number of path elements to append
//
// Returns:
//   - Filepath: A new Filepath with the elements joined
//
// Example:
//
//	dataDir := primitives.Filepath("/data")
//	tablePath := dataDir.Join("tables", "users.dat")
//	// Returns Filepath("/data/tables/users.dat")
func (f Filepath) Join(elem ...string) Filepath {
	parts := append([]string{string(f)}, elem...)
	return Filepath(filepath.Join(parts...))
}

// Base returns the last element of the path (the filename).
// This is a wrapper around filepath.Base.
//
// Returns:
//   - string: The filename without directory path
//
// Example:
//
//	path := primitives.Filepath("/data/indexes/users_id.idx")
//	base := path.Base() // Returns "users_id.idx"
func (f Filepath) Base() string {
	return filepath.Base(string(f))
}

// Exists checks whether the file exists on the filesystem.
// This provides a cleaner interface than checking os.Stat errors.
//
// Returns:
//   - bool: true if the file exists, false otherwise
//
// Example:
//
//	path := primitives.Filepath("/data/users.dat")
//	if path.Exists() {
//	    // File exists, proceed with operation
//	}
func (f Filepath) Exists() bool {
	_, err := os.Stat(string(f))
	return err == nil
}

// Remove deletes the file from the filesystem.
// This operation is idempotent - it succeeds if the file doesn't exist.
//
// Returns:
//   - error: nil if file was deleted or doesn't exist, error otherwise
//
// Example:
//
//	indexPath := primitives.Filepath("/data/indexes/old_index.idx")
//	if err := indexPath.Remove(); err != nil {
//	    return fmt.Errorf("failed to remove index: %w", err)
//	}
func (f Filepath) Remove() error {
	if !f.Exists() {
		return nil // Idempotent operation
	}
	return os.Remove(string(f))
}

// IsEmpty checks whether the filepath is an empty string.
// This is useful for validation before file operations.
//
// Returns:
//   - bool: true if the path is empty, false otherwise
//
// Example:
//
//	path := primitives.Filepath("")
//	if path.IsEmpty() {
//	    return errors.New("filepath cannot be empty")
//	}
func (f Filepath) IsEmpty() bool {
	return string(f) == ""
}

// MkdirAll creates the parent directory and any necessary parents.
// This is a convenience method that creates directories with the specified permissions.
//
// Parameters:
//   - perm: File permissions for created directories (e.g., 0755)
//
// Returns:
//   - error: nil on success, error if directory creation fails
//
// Example:
//
//	indexPath := primitives.Filepath("/data/indexes/users_id.idx")
//	if err := indexPath.MkdirAll(0755); err != nil {
//	    return fmt.Errorf("failed to create directory: %w", err)
//	}
func (f Filepath) MkdirAll(perm os.FileMode) error {
	return os.MkdirAll(f.Dir(), perm)
}

// Clean returns the shortest path name equivalent to the path by purely lexical processing.
// This is a wrapper around filepath.Clean that returns a Filepath type.
//
// Returns:
//   - Filepath: The cleaned path
//
// Example:
//
//	path := primitives.Filepath("/data/../data/./users.dat")
//	clean := path.Clean() // Returns "/data/users.dat"
func (f Filepath) Clean() Filepath {
	return Filepath(filepath.Clean(string(f)))
}
