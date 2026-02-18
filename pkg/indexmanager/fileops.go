package indexmanager

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
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
// Parameters:
//   - keyType: The data type of the indexed column (e.g., IntType, StringType).
//     This determines how keys are stored and compared in the index.
//   - indexType: The type of index structure to create (HashIndex or BTreeIndex)
//
// Returns:
//   - indexID: The actual file ID from the created index file (from BaseFile)
func (i *IndexFileOps) CreatePhysicalIndex(keyType types.Type, indexType index.IndexType) (primitives.FileID, error) {
	if err := i.f.MkdirAll(0o755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %v", err)
	}

	switch indexType {
	case index.HashIndex:
		hashFile, err := index.NewHashFile(i.f, keyType, index.DefaultBuckets)
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
// index has been removed from the catalog.
//
// Parameters:
//   - filePath: Absolute or relative path to the index file to delete
//
// Returns:
//   - error: nil if the file doesn't exist (idempotent operation) or was
//     successfully deleted. Returns an error if the file exists but cannot
//     be deleted due to permission issues or file system errors.
func (i *IndexFileOps) DeletePhysicalIndex() error {
	if !i.f.Exists() {
		return nil
	}

	if err := i.f.Remove(); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", i.f.String(), err)
	}

	return nil
}
