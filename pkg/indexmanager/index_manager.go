package indexmanager

import (
	"fmt"
	"os"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/scanner"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// TxCtx is a convenience alias for a transaction context pointer used throughout this package.
type TxCtx = *transaction.TransactionContext

// IndexType is a convenience alias for index.IndexType, re-exported so callers
// do not need to import the storage/index package directly.
type IndexType = index.IndexType

// CatalogReader defines the interface for reading index metadata from the catalog.
// This avoids circular dependencies with the catalog package.
type CatalogReader interface {
	// GetIndexesByTable retrieves index information from the catalog
	GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*systable.IndexMetadata, error)

	// GetTableSchema retrieves the schema for a table
	GetTableSchema(tableID primitives.FileID) (*schema.Schema, error)
}

// IndexMetadata represents complete, resolved metadata for a database index.
// This extends systemtable.IndexMetadata with resolved schema information needed for index operations.
type IndexMetadata struct {
	systable.IndexMetadata
	ColumnIndex primitives.ColumnID // Field index in tuple (0-based, resolved from schema)
	KeyType     types.Type          // Type of the indexed column (resolved from schema)
}

// IndexWithMetadata pairs an open index instance with its fully resolved metadata.
// The index field holds the concrete implementation (BTree or HashIndex) and
// metadata holds the catalog entry enriched with the schema-resolved column position and key type.
type IndexWithMetadata struct {
	index    index.Index    // Either *btreeindex.BTree or *hashindex.HashIndex
	metadata *IndexMetadata // Resolved metadata for this index
}

// IndexManager manages all indexes in the database.
// It handles:
//   - Loading index metadata from catalog
//   - Opening and caching index files
//   - Maintaining indexes on tuple insertions/deletions
//   - Coordinating index operations with WAL
type IndexManager struct {
	catalog   CatalogReader
	pageStore *memory.PageStore
	wal       *wal.WAL

	cacheMu sync.RWMutex
	cache   map[primitives.FileID][]*IndexWithMetadata
}

// NewIndexManager creates a new IndexManager instance.
//
// Parameters:
//   - catalog: Interface to read index metadata from system catalog
//   - pageStore: Page store for managing index pages
//   - wal: Write-ahead log for logging index operations
//
// Returns a new IndexManager ready to manage indexes.
func NewIndexManager(catalog CatalogReader, pageStore *memory.PageStore, wal *wal.WAL) *IndexManager {
	return &IndexManager{
		catalog:   catalog,
		pageStore: pageStore,
		wal:       wal,
		cache:     make(map[primitives.FileID][]*IndexWithMetadata),
	}
}

// Close releases all resources held by the IndexManager.
func (i *IndexManager) Close() error {
	var firstError error

	i.cacheMu.Lock()
	allIndexes := i.cache
	i.cache = make(map[primitives.FileID][]*IndexWithMetadata)
	i.cacheMu.Unlock()

	for tableID, indexes := range allIndexes {
		for _, idxWithMeta := range indexes {
			if err := idxWithMeta.index.Close(); err != nil && firstError == nil {
				firstError = fmt.Errorf("failed to close index for table %d: %v", tableID, err)
			}
		}
	}

	return firstError
}

// CreatePhysicalIndex creates a new on-disk index file and returns the file ID assigned to it.
// The file is opened to obtain its ID and then immediately closed; the caller is responsible
// for registering the returned ID in the catalog before the index is used.
func (i *IndexManager) CreatePhysicalIndex(filePath primitives.Filepath, keyType types.Type, indexType index.IndexType) (primitives.FileID, error) {
	if err := filePath.MkdirAll(0o755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %v", err)
	}

	file, err := index.OpenFile(indexType, filePath, keyType)
	if err != nil {
		return 0, fmt.Errorf("failed to create index file: %v", err)
	}

	indexID := file.GetID()
	if err := file.Close(); err != nil {
		return 0, fmt.Errorf("failed to close index file: %v", err)
	}

	return indexID, nil
}

// DeletePhysicalIndex removes a physical index file from disk.
// It is a no-op if the file does not exist, making it safe to call
// during cleanup even when index creation was only partially completed.
// On Windows, open file handles prevent deletion, so this method first
// closes any registered handle in the PageStore before removing the file.
func (i *IndexManager) DeletePhysicalIndex(filePath primitives.Filepath) error {
	if !filePath.Exists() {
		return nil
	}

	// Close any open PageStore handle for this file before deletion.
	// On Windows, os.Remove fails if the file has open handles.
	i.pageStore.EvictAndCloseFile(filePath.Hash())

	if err := filePath.Remove(); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", filePath.String(), err)
	}

	return nil
}

// insertIntoIndex scans a table and inserts all tuples into an index.
// It performs a sequential scan of the table file and for each tuple,
// extracts the value at the specified column index and inserts it into
// the index using the provided insert function.
func (i *IndexManager) insertIntoIndex(tx *transaction.TransactionContext, tableFile *heap.HeapFile, col primitives.ColumnID, insertFunc func(key types.Field, rid *tuple.TupleRecordID) error) error {
	seqScan, err := scanner.NewSeqScan(tx, tableFile.GetID(), tableFile, i.pageStore)
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

		key, err := t.GetField(col)
		if err != nil {
			return fmt.Errorf("failed to get field at index %d: %v", col, err)
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

// PopulateIndex builds an index by scanning an existing table and inserting all tuples.
// This is called after a new index is created on a non-empty table to backfill
// existing rows. Steps:
//  1. Opens the index file at the given path
//  2. Validates the table file is a HeapFile and that col is within range
//  3. Sequentially scans the table and inserts each row's key into the index
func (i *IndexManager) PopulateIndex(tx *transaction.TransactionContext, file primitives.Filepath, table page.DbFile, col primitives.ColumnID, keyType types.Type, indexType index.IndexType) error {
	idx, err := i.openIndex(tx, keyType, file, indexType)
	if err != nil {
		return err
	}
	defer idx.Close()

	heapFile, ok := table.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("expected heap file for table, got %T", table)
	}

	tupleDesc := heapFile.GetTupleDesc()
	if tupleDesc == nil {
		return fmt.Errorf("table has no schema definition")
	}

	numFields := tupleDesc.NumFields()
	if col >= numFields {
		return fmt.Errorf("column index %d is out of range for table with %d columns", col, numFields)
	}

	return i.insertIntoIndex(tx, heapFile, col, func(key types.Field, rid *tuple.TupleRecordID) error {
		return idx.Insert(key, rid)
	})
}

// openIndex opens an existing index file and returns a ready-to-use index instance.
// File ownership is transferred to the returned index; callers must call Close() on
// the returned index to release the file descriptor and unregister from PageStore.
func (i *IndexManager) openIndex(tx *transaction.TransactionContext, keyType types.Type, filePath primitives.Filepath, indexType index.IndexType) (index.Index, error) {
	switch indexType {
	case index.HashIndex:
		hashFile, err := index.NewHashFile(filePath, keyType, index.DefaultBuckets)
		if err != nil {
			return nil, fmt.Errorf("failed to open hash index: %v", err)
		}

		return hashindex.NewHashIndex(hashFile.GetID(), keyType, hashFile, i.pageStore, tx), nil

	case index.BTreeIndex:
		btreeFile, err := index.NewBTreeFile(filePath, keyType)
		if err != nil {
			return nil, fmt.Errorf("failed to open btree index: %v", err)
		}

		return btreeindex.NewBTree(btreeFile.GetID(), keyType, btreeFile, tx, i.pageStore), nil

	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexType)
	}
}

// operationType represents the type of operation being performed on an index.
// It is used internally to distinguish between different DML operations
// and route them to the appropriate index methods.
type operationType int

const (
	insertOp operationType = iota // index entry should be added
	deleteOp                      // index entry should be removed
)

// String returns a human-readable string representation of the operation type.
// This is useful for error messages and logging.
func (op operationType) String() string {
	switch op {
	case insertOp:
		return "insert"
	case deleteOp:
		return "delete"
	default:
		return "unknown"
	}
}

// processIndexOperation performs a specified operation (insert or delete) on all indexes
// associated with a table for a given tuple. This is the core method that coordinates
// index maintenance across all index types.
func (i *IndexManager) processIndexOperation(ctx TxCtx, tableID primitives.FileID, t *tuple.Tuple, opType operationType) error {
	if t == nil || t.TableNotAssigned() {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := i.LoadIndexes(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idx := range indexes {
		key, err := i.extractKey(t, idx.metadata)
		indexName := idx.metadata.IndexName
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", indexName, err)
		}

		var opErr error
		switch opType {
		case insertOp:
			opErr = i.applyIndexOperation(idx, key, t.RecordID, insertOp)
		case deleteOp:
			opErr = i.applyIndexOperation(idx, key, t.RecordID, deleteOp)
		}

		if opErr != nil {
			return fmt.Errorf("failed to %s index %s: %v", opType.String(), indexName, opErr)
		}
	}

	return nil
}

// applyIndexOperation applies a specific operation to a single index.
// This function handles the type-specific logic for different index implementations
// (B-Tree and Hash indexes) and delegates to their respective Insert or Delete methods.
func (i *IndexManager) applyIndexOperation(idx *IndexWithMetadata, key types.Field, rid *tuple.TupleRecordID, opType operationType) error {
	switch v := idx.index.(type) {
	case *btreeindex.BTree:
		if opType == insertOp {
			return v.Insert(key, rid)
		}
		return v.Delete(key, rid)
	case *hashindex.HashIndex:
		if opType == insertOp {
			return v.Insert(key, rid)
		}
		return v.Delete(key, rid)
	default:
		return fmt.Errorf("unsupported index type for %s", idx.metadata.IndexName)
	}
}

// extractKey extracts the indexed column value from a tuple.
// This method retrieves the field at the column position specified in the
// index metadata and validates that it matches the expected type.
func (i *IndexManager) extractKey(t *tuple.Tuple, metadata *IndexMetadata) (types.Field, error) {
	colIndex := metadata.ColumnIndex
	if colIndex >= t.TupleDesc.NumFields() {
		return nil, fmt.Errorf("invalid column index %d for tuple with %d fields",
			colIndex, t.TupleDesc.NumFields())
	}

	field, err := t.GetField(colIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get field at index %d: %v", colIndex, err)
	}

	if field.Type() != metadata.KeyType {
		return nil, fmt.Errorf("field type mismatch: expected %v, got %v",
			metadata.KeyType, field.Type())
	}

	return field, nil
}

// OnInsert maintains all indexes for a table when a new tuple is inserted.
// This method extracts the indexed column values from the tuple and inserts
// them into all applicable indexes for the table.
//
// The operation is atomic across all indexes - if any index insertion fails,
// an error is returned. However, partial insertions may have occurred before
// the failure, so callers should handle rollback at a higher level.
func (i *IndexManager) OnInsert(ctx TxCtx, tableID primitives.FileID, t *tuple.Tuple) error {
	return i.processIndexOperation(ctx, tableID, t, insertOp)
}

// OnDelete maintains all indexes for a table when a tuple is deleted.
// This method extracts the indexed column values from the tuple being deleted
// and removes the corresponding entries from all applicable indexes.
//
// The operation is atomic across all indexes - if any index deletion fails,
// an error is returned. Partial deletions may have occurred before the failure.
func (i *IndexManager) OnDelete(ctx TxCtx, tableID primitives.FileID, t *tuple.Tuple) error {
	return i.processIndexOperation(ctx, tableID, t, deleteOp)
}

// OnUpdate maintains all indexes for a table when a tuple is updated.
// This is implemented as a delete-then-insert operation to ensure proper
// index maintenance when indexed column values change.
//
// If the insertion fails after deletion, the method attempts to re-insert
// the old tuple to maintain consistency, though this rollback may also fail.
// Callers should implement proper transaction rollback mechanisms.
func (i *IndexManager) OnUpdate(ctx TxCtx, tableID primitives.FileID, old, updated *tuple.Tuple) error {
	if err := i.OnDelete(ctx, tableID, old); err != nil {
		return err
	}

	if err := i.OnInsert(ctx, tableID, updated); err != nil {
		_ = i.OnInsert(ctx, tableID, old)
		return err
	}

	return nil
}

// LoadIndexes loads and opens all indexes registered for a table.
// Index metadata is fetched from the catalog and each file is opened via the PageStore.
// If a single index file cannot be opened a warning is written to stderr and that index
// is skipped; the remaining indexes are still returned. This allows the database to
// remain operational even when an index file is missing or corrupt.
//
// Parameters:
//   - tx: Transaction context used for catalog reads
//   - tableID: ID of the table whose indexes should be loaded
//
// Returns:
//   - []*IndexWithMetadata: Successfully opened indexes with their resolved metadata
//   - error: Non-nil only when the catalog itself cannot be read
func (i *IndexManager) LoadIndexes(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*IndexWithMetadata, error) {
	metadataList, err := i.loadFromCatalog(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	indexes := make([]*IndexWithMetadata, 0, len(metadataList))
	for _, m := range metadataList {
		idx, err := i.openIndex(tx, m.KeyType, m.FilePath, m.IndexType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open index %s: %v\n", m.IndexName, err)
			continue
		}

		indexes = append(indexes, &IndexWithMetadata{
			index:    idx,
			metadata: m,
		})
	}

	return indexes, nil
}

// LoadIndexForCol opens the index on the specified column of a table.
// Returns an error if no index exists for that column or if the file cannot be opened.
// Unlike LoadIndexes, this method returns exactly one index targeted at a specific
// column — it is used by the planner when an index scan has already been chosen.
func (i *IndexManager) LoadIndexForCol(tx *transaction.TransactionContext, colID primitives.ColumnID, tableID primitives.FileID) (index.Index, error) {
	indexes, err := i.loadFromCatalog(tx, tableID)
	if err != nil {
		return nil, err
	}

	colIdx := slices.IndexFunc(indexes, func(i *IndexMetadata) bool {
		return i.ColumnIndex == colID
	})
	if colIdx == -1 {
		return nil, fmt.Errorf("col %d not found", colID)
	}

	meta := indexes[colIdx]
	return i.openIndex(tx, meta.KeyType, meta.FilePath, meta.IndexType)
}

// loadFromCatalog loads raw index info from catalog and resolves it
// with schema information (ColumnIndex, KeyType) to create full IndexMetadata.
// This method enriches the catalog metadata with schema-specific information
// needed for index operations.
func (i *IndexManager) loadFromCatalog(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*IndexMetadata, error) {
	catalogIndexes, err := i.catalog.GetIndexesByTable(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	if len(catalogIndexes) == 0 {
		return []*IndexMetadata{}, nil
	}

	schema, err := i.catalog.GetTableSchema(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	return resolveIndexMetadata(catalogIndexes, schema), nil
}

// resolveIndexMetadata resolves catalog metadata with schema to create complete IndexMetadata.
// It matches column names from the catalog with actual column positions and types
// from the table schema.
func resolveIndexMetadata(catalogIndexes []*systable.IndexMetadata, schema *schema.Schema) []*IndexMetadata {
	result := make([]*IndexMetadata, 0, len(catalogIndexes))

	for _, catIdx := range catalogIndexes {
		columnIndex, keyType, err := schema.TupleDesc.FindFieldWithType(catIdx.ColumnName)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: column %s not found in schema for index %s\n",
				catIdx.ColumnName, catIdx.IndexName)
			continue
		}

		result = append(result, &IndexMetadata{
			IndexMetadata: *catIdx,
			ColumnIndex:   columnIndex,
			KeyType:       keyType,
		})
	}

	return result
}
