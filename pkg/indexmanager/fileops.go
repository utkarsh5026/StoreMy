package indexmanager

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/types"
)

type IndexFileOps struct {
	f primitives.Filepath
}

// CreatePhysicalIndex creates the physical index file on disk and returns its actual file ID.
// This method initializes the appropriate index structure (Hash or BTree)
// based on the specified index type and creates the file with proper
// headers and metadata. The parent directory is created if it doesn't exist.
//
// The returned indexID is the file's natural ID from BaseFile (hash of filename).
// This ID should be used when registering the index in the catalog to ensure
// proper separation of concerns between physical and metadata layers.
//
// Supported index types:
//   - HashIndex: Creates a hash-based index with default bucket configuration
//   - BTreeIndex: Creates a B-tree index optimized for range queries
//
// Parameters:
//   - filePath: Absolute or relative path where the index file should be created.
//     Parent directories will be created automatically if they don't exist.
//   - keyType: The data type of the indexed column (e.g., IntType, StringType).
//     This determines how keys are stored and compared in the index.
//   - indexType: The type of index structure to create (HashIndex or BTreeIndex)
//
// Returns:
//   - indexID: The actual file ID from the created index file (from BaseFile)
//   - error: nil on success, or an error if:
//   - Directory creation fails
//   - Index file creation fails
//   - Index type is unsupported
//   - File system I/O errors occur
//
// Example:
//
//	indexID, err := im.CreatePhysicalIndex("data/indexes/user_id.idx", types.IntType, index.HashIndex)
func (i *IndexFileOps) CreatePhysicalIndex(keyType types.Type, indexType index.IndexType) (primitives.FileID, error) {
	if err := i.f.MkdirAll(0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %v", err)
	}

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(i.f, keyType, hash.DefaultBuckets)
		if err != nil {
			return 0, fmt.Errorf("failed to create hash index file: %v", err)
		}
		indexID := hashFile.GetID()
		if err := hashFile.Close(); err != nil {
			return 0, fmt.Errorf("failed to close hash index file: %v", err)
		}
		return indexID, nil

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(i.f, keyType)
		if err != nil {
			return 0, fmt.Errorf("failed to create btree index file: %v", err)
		}
		indexID := btreeFile.GetID()
		if err := btreeFile.Close(); err != nil {
			return 0, fmt.Errorf("failed to close btree index file: %v", err)
		}
		return indexID, nil

	default:
		return 0, fmt.Errorf("unsupported index type: %s", indexType)
	}
}

// DeletePhysicalIndex removes the physical index file from the file system.
// This method is typically called during a DROP INDEX operation after the
// index has been removed from the catalog. It safely handles the case where
// the file has already been deleted.
//
// The method does not perform any catalog updates or transaction management;
// it only handles the physical file deletion. Catalog cleanup should be
// performed separately before calling this method.
//
// Parameters:
//   - filePath: Absolute or relative path to the index file to delete
//
// Returns:
//   - error: nil if the file doesn't exist (idempotent operation) or was
//     successfully deleted. Returns an error if the file exists but cannot
//     be deleted due to permission issues or file system errors.
//
// Example:
//
//	err := im.DeletePhysicalIndex("data/indexes/user_id.idx")
func (i *IndexFileOps) DeletePhysicalIndex() error {
	if !i.f.Exists() {
		return nil
	}

	if err := i.f.Remove(); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", i.f.String(), err)
	}

	return nil
}
