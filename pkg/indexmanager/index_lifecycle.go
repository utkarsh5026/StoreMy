package indexmanager

import (
	"fmt"
	"os"
	"storemy/pkg/execution/query"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// insertIntoIndex scans a table and inserts all tuples into an index.
// It performs a sequential scan of the table file and for each tuple,
// extracts the value at the specified column index and inserts it into
// the index using the provided insert function.
//
// The method skips tuples with nil key values and validates that each
// tuple has a valid record ID before insertion.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - tableFile: The heap file containing the table data to index
//   - columnIndex: Zero-based index of the column to extract keys from
//   - insertFunc: Function that performs the actual insertion into the index,
//     taking a key and tuple record ID as parameters
//
// Returns:
//   - error: nil on success, or an error if the sequential scan fails,
//     tuple access fails, or index insertion fails
func (im *IndexManager) insertIntoIndex(ctx TxCtx, tableFile *heap.HeapFile, columnIndex primitives.ColumnID, insertFunc func(key types.Field, rid *tuple.TupleRecordID) error) error {
	seqScan, err := query.NewSeqScan(ctx, tableFile.GetID(), tableFile, im.pageStore)
	if err != nil {
		return fmt.Errorf("failed to create sequential scan: %v", err)
	}

	if err := seqScan.Open(); err != nil {
		return fmt.Errorf("failed to open sequential scan: %v", err)
	}
	defer seqScan.Close()

	for {
		t, err := seqScan.Next()
		if t == nil {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to scan tuple: %v", err)
		}

		key, err := t.GetField(columnIndex)
		if err != nil {
			return fmt.Errorf("failed to get field at index %d: %v", columnIndex, err)
		}

		if key == nil {
			continue
		}

		if t.TableNotAssigned() {
			return fmt.Errorf("tuple missing record ID")
		}

		if err := insertFunc(key, t.RecordID); err != nil {
			return fmt.Errorf("failed to insert key into index: %v", err)
		}
	}

	return nil
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
func (im *IndexManager) CreatePhysicalIndex(f primitives.Filepath, keyType types.Type, indexType index.IndexType) (primitives.FileID, error) {
	dir := f.Dir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %v", err)
	}

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(f, keyType, hash.DefaultBuckets)
		if err != nil {
			return 0, fmt.Errorf("failed to create hash index file: %v", err)
		}
		indexID := hashFile.GetID()
		if err := hashFile.Close(); err != nil {
			return 0, fmt.Errorf("failed to close hash index file: %v", err)
		}
		return indexID, nil

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(f, keyType)
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

// PopulateIndex builds an index by scanning an existing table and inserting all tuples.
// This method is typically called after creating a new index to populate it with
// existing data from the table. It performs the following steps:
//
// 1. Opens the index file based on the specified index type
// 2. Creates an in-memory index wrapper (Hash or BTree)
// 3. Performs a sequential scan of the table
// 4. Extracts the key value from each tuple's specified column
// 5. Inserts the key-value pair (key, record ID) into the index
//
// The method validates that the column index is within the valid range for
// the table's schema before beginning the population process.
//
// Parameters:
//   - ctx: Transaction context for the index population operation
//   - filePath: Path to the physical index file on disk
//   - indexID: Unique identifier for this index in the catalog
//   - tableFile: The database file containing the table data to index.
//     Must be a heap file implementation.
//   - columnIndex: Zero-based index of the column to create the index on.
//     Must be within the valid range [0, numColumns-1].
//   - keyType: The data type of the indexed column
//   - indexType: The type of index structure (HashIndex or BTreeIndex)
//
// Returns:
//   - error: nil on success, or an error if:
//   - The index file cannot be opened
//   - The table file is not a valid heap file
//   - The column index is out of range
//   - The sequential scan fails
//   - Any tuple insertion into the index fails
//   - The index type is unsupported
//
// Example:
//
//	err := im.PopulateIndex(ctx, "data/indexes/user_id.idx", 1, tableFile, 0,
//	                       types.IntType, index.HashIndex)
func (im *IndexManager) PopulateIndex(
	ctx TxCtx,
	filePath primitives.Filepath,
	indexID primitives.FileID,
	tableFile page.DbFile,
	columnIndex primitives.ColumnID,
	keyType types.Type,
	indexType index.IndexType,
) error {
	var insertFunc func(key types.Field, rid *tuple.TupleRecordID) error

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(filePath, keyType, hash.DefaultBuckets)
		if err != nil {
			return fmt.Errorf("failed to open hash index: %v", err)
		}
		defer hashFile.Close()

		// Use the file's actual ID, which should match the indexID passed from catalog
		hashIdx := hashindex.NewHashIndex(hashFile.GetID(), keyType, hashFile, im.pageStore, ctx)
		insertFunc = func(key types.Field, rid *tuple.TupleRecordID) error {
			return hashIdx.Insert(key, rid)
		}

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(filePath, keyType)
		if err != nil {
			return fmt.Errorf("failed to open btree index: %v", err)
		}
		defer btreeFile.Close()

		// Use the file's actual ID, which should match the indexID passed from catalog
		btreeIdx := btreeindex.NewBTree(btreeFile.GetID(), keyType, btreeFile, ctx, im.pageStore)
		insertFunc = func(key types.Field, rid *tuple.TupleRecordID) error {
			return btreeIdx.Insert(key, rid)
		}

	default:
		return fmt.Errorf("unsupported index type: %s", indexType)
	}

	heapFile, ok := tableFile.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("expected heap file for table, got %T", tableFile)
	}

	tupleDesc := heapFile.GetTupleDesc()
	if tupleDesc == nil {
		return fmt.Errorf("table has no schema definition")
	}
	numFields := tupleDesc.NumFields()
	if columnIndex < 0 || columnIndex >= numFields {
		return fmt.Errorf("column index %d is out of range for table with %d columns", columnIndex, numFields)
	}

	return im.insertIntoIndex(ctx, heapFile, columnIndex, insertFunc)
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
func (im *IndexManager) DeletePhysicalIndex(f primitives.Filepath) error {
	path := string(f)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", path, err)
	}

	return nil
}
