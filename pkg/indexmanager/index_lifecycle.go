package indexmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// indexLifecycle handles physical index file operations (create, populate, delete).
type indexLifecycle struct {
	pageStore *memory.PageStore
}

// newIndexLifecycle creates a new index lifecycle handler.
func newIndexLifecycle(pageStore *memory.PageStore) *indexLifecycle {
	return &indexLifecycle{
		pageStore: pageStore,
	}
}

// CreatePhysicalIndex creates the actual index file on disk based on the index type.
// This method creates the appropriate index structure (Hash or BTree) and initializes
// the file with the proper header and metadata.
//
// Parameters:
//   - filePath: Path where the index file should be created
//   - keyType: Type of the key field that will be indexed
//   - indexType: Type of index to create (HashIndex or BTreeIndex)
//
// Returns an error if the index file cannot be created or if the index type is unsupported.
func (il *indexLifecycle) CreatePhysicalIndex(
	filePath string,
	keyType types.Type,
	indexType index.IndexType,
) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(filePath, keyType, hash.DefaultBuckets)
		if err != nil {
			return fmt.Errorf("failed to create hash index file: %v", err)
		}
		return hashFile.Close()

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(filePath, keyType)
		if err != nil {
			return fmt.Errorf("failed to create btree index file: %v", err)
		}
		return btreeFile.Close()

	default:
		return fmt.Errorf("unsupported index type: %s", indexType)
	}
}

// PopulateIndex scans a table and inserts all existing tuples into the specified index.
// This is typically called after index creation to build the initial index state from
// existing table data.
//
// Parameters:
//   - ctx: Transaction context for the operation
//   - filePath: Path to the index file to populate
//   - indexID: ID of the index being populated
//   - tableFile: The heap file containing the table data to index
//   - columnIndex: Index of the column to extract for the index key
//   - keyType: Type of the key field being indexed
//   - indexType: Type of index (HashIndex or BTreeIndex)
//
// Returns an error if the index cannot be opened, populated, or if tuple scanning fails.
func (il *indexLifecycle) PopulateIndex(
	ctx *transaction.TransactionContext,
	filePath string,
	indexID int,
	tableFile page.DbFile,
	columnIndex int,
	keyType types.Type,
	indexType IndexType,
) error {
	var insertFunc func(key types.Field, rid *tuple.TupleRecordID) error

	switch indexType {
	case index.HashIndex:
		hashFile, err := hash.NewHashFile(filePath, keyType, hash.DefaultBuckets)
		if err != nil {
			return fmt.Errorf("failed to open hash index: %v", err)
		}
		defer hashFile.Close()

		hashIdx := hashindex.NewHashIndex(indexID, keyType, hashFile, il.pageStore, ctx)
		insertFunc = func(key types.Field, rid *tuple.TupleRecordID) error {
			return hashIdx.Insert(key, rid)
		}

	case index.BTreeIndex:
		btreeFile, err := btree.NewBTreeFile(filePath, keyType)
		if err != nil {
			return fmt.Errorf("failed to open btree index: %v", err)
		}
		defer btreeFile.Close()

		btreeIdx := btreeindex.NewBTree(indexID, keyType, btreeFile, ctx, il.pageStore)
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

	return il.insertIntoIndex(ctx, heapFile, columnIndex, insertFunc)
}

// insertIntoIndex is a helper method that scans a table and inserts all tuples
// into an index using the provided insert function.
func (il *indexLifecycle) insertIntoIndex(
	ctx *transaction.TransactionContext,
	tableFile *heap.HeapFile,
	columnIndex int,
	insertFunc func(key types.Field, rid *tuple.TupleRecordID) error,
) error {
	seqScan, err := query.NewSeqScan(ctx, tableFile.GetID(), tableFile, il.pageStore)
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

		if t.RecordID == nil {
			return fmt.Errorf("tuple missing record ID")
		}

		if err := insertFunc(key, t.RecordID); err != nil {
			return fmt.Errorf("failed to insert key into index: %v", err)
		}
	}

	return nil
}

// DeletePhysicalIndex removes the physical index file from disk.
// This method is typically called after removing the index from the catalog
// to clean up disk resources.
//
// Parameters:
//   - filePath: Path to the index file to delete
//
// Returns nil if the file doesn't exist (already deleted) or was successfully deleted.
// Returns an error if the file exists but cannot be deleted.
func (il *indexLifecycle) DeletePhysicalIndex(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", filePath, err)
	}

	return nil
}

// CreatePhysicalIndex is a convenience method on IndexManager that delegates to lifecycle.
func (im *IndexManager) CreatePhysicalIndex(
	filePath string,
	keyType types.Type,
	indexType index.IndexType,
) error {
	return im.lifecycle.CreatePhysicalIndex(filePath, keyType, indexType)
}

// PopulateIndex is a convenience method on IndexManager that delegates to lifecycle.
func (im *IndexManager) PopulateIndex(
	ctx *transaction.TransactionContext,
	filePath string,
	indexID int,
	tableFile page.DbFile,
	columnIndex int,
	keyType types.Type,
	indexType index.IndexType,
) error {
	return im.lifecycle.PopulateIndex(ctx, filePath, indexID, tableFile, columnIndex, keyType, indexType)
}

// DeletePhysicalIndex is a convenience method on IndexManager that delegates to lifecycle.
func (im *IndexManager) DeletePhysicalIndex(filePath string) error {
	return im.lifecycle.DeletePhysicalIndex(filePath)
}
