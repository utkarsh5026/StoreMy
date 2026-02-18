package indexmanager

import (
	"fmt"
	"os"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
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
)

type TxCtx = *transaction.TransactionContext
type IndexType = index.IndexType

// CatalogReader defines the interface for reading index metadata from the catalog.
// This avoids circular dependencies with the catalog package.
type CatalogReader interface {
	// GetIndexesByTable retrieves index information from the catalog
	GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*systemtable.IndexMetadata, error)

	// GetTableSchema retrieves the schema for a table
	GetTableSchema(tableID primitives.FileID) (*schema.Schema, error)
}

// IndexMetadata represents complete, resolved metadata for a database index.
// This extends systemtable.IndexMetadata with resolved schema information needed for index operations.
type IndexMetadata struct {
	systemtable.IndexMetadata
	ColumnIndex primitives.ColumnID // Field index in tuple (0-based, resolved from schema)
	KeyType     types.Type          // Type of the indexed column (resolved from schema)
}

// indexWithMetadata wraps an index (BTree or HashIndex) with its metadata
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
//
// Responsibilities:
//   - Index lifecycle management (open, close, cache)
//   - Index maintenance on DML operations
//   - Transaction-aware index operations
//   - Thread-safe access to index structures
type IndexManager struct {
	catalog   CatalogReader
	pageStore *memory.PageStore
	wal       *wal.WAL

	cache *indexCache
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
	im := &IndexManager{
		catalog:   catalog,
		pageStore: pageStore,
		wal:       wal,
	}

	im.cache = newIndexCache()
	return im
}

// InvalidateCache removes cached indexes for a table.
// This is typically called when indexes are created or dropped for a table.
func (im *IndexManager) InvalidateCache(tableID primitives.FileID) {
	im.cache.Invalidate(tableID)
}

// Close releases all resources held by the IndexManager.
func (im *IndexManager) Close() error {
	var firstError error

	allIndexes := im.cache.Clear()
	for tableID, indexes := range allIndexes {
		for _, idxWithMeta := range indexes {
			if err := closeIndexFile(idxWithMeta.index); err != nil && firstError == nil {
				firstError = fmt.Errorf("failed to close index for table %d: %v", tableID, err)
			}
		}
	}

	return firstError
}

func closeIndexFile(index index.Index) error {
	if hashIdx, ok := index.(*hashindex.HashIndex); ok {
		if err := hashIdx.Close(); err != nil {
			return fmt.Errorf("failed to close hash index: %v", err)
		}
	}

	if btreeIdx, ok := index.(*btreeindex.BTree); ok {
		if err := btreeIdx.Close(); err != nil {
			return fmt.Errorf("failed to close btree index: %v", err)
		}
	}

	return nil
}

// CreatePhysicalIndex creates a physical index file and returns its file ID.
// This is a convenience wrapper that delegates to IndexFileOps.
func (im *IndexManager) CreatePhysicalIndex(filePath primitives.Filepath, keyType types.Type, indexType index.IndexType) (primitives.FileID, error) {
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
// This is a convenience wrapper that delegates to IndexFileOps.
func (im *IndexManager) DeletePhysicalIndex(filePath primitives.Filepath) error {
	if !filePath.Exists() {
		return nil
	}

	if err := filePath.Remove(); err != nil {
		return fmt.Errorf("failed to remove file %s: %v", filePath.String(), err)
	}

	return nil
}

// insertIntoIndex scans a table and inserts all tuples into an index.
// It performs a sequential scan of the table file and for each tuple,
// extracts the value at the specified column index and inserts it into
// the index using the provided insert function.
func (im *IndexManager) insertIntoIndex(tx *transaction.TransactionContext, tableFile *heap.HeapFile, col primitives.ColumnID, insertFunc func(key types.Field, rid *tuple.TupleRecordID) error) error {
	seqScan, err := scanner.NewSeqScan(tx, tableFile.GetID(), tableFile, im.pageStore)
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
// This method is typically called after creating a new index to populate it with
// existing data from the table. It performs the following steps:
//
// The method validates that the column index is within the valid range for
// the table's schema before beginning the population process.
//
// Parameters:
//   - tx: Transaction context for the index population operation
//   - filePath: Path to the physical index file on disk
//   - tableFile: The database file containing the table data to index.
//     Must be a heap file implementation.
//   - columnIndex: Zero-based index of the column to create the index on.
//     Must be within the valid range [0, numColumns-1].
//   - keyType: The data type of the indexed column
//   - indexType: The type of index structure (HashIndex or BTreeIndex)
func (im *IndexManager) PopulateIndex(tx *transaction.TransactionContext, file primitives.Filepath, table page.DbFile, col primitives.ColumnID, keyType types.Type, indexType index.IndexType) error {
	idx, err := im.openIndex(tx, keyType, file, indexType)
	if err != nil {
		return err
	}

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

	return im.insertIntoIndex(tx, heapFile, col, func(key types.Field, rid *tuple.TupleRecordID) error {
		return idx.Insert(key, rid)
	})
}

func (im *IndexManager) openIndex(tx *transaction.TransactionContext, keyType types.Type, filePath primitives.Filepath, indexType index.IndexType) (index.Index, error) {
	switch indexType {
	case index.HashIndex:
		hashFile, err := index.NewHashFile(filePath, keyType, index.DefaultBuckets)
		if err != nil {
			return nil, fmt.Errorf("failed to open hash index: %v", err)
		}
		defer hashFile.Close()

		return hashindex.NewHashIndex(hashFile.GetID(), keyType, hashFile, im.pageStore, tx), nil

	case index.BTreeIndex:
		btreeFile, err := index.NewBTreeFile(filePath, keyType)
		if err != nil {
			return nil, fmt.Errorf("failed to open btree index: %v", err)
		}
		defer btreeFile.Close()

		return btreeindex.NewBTree(btreeFile.GetID(), keyType, btreeFile, tx, im.pageStore), nil

	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexType)
	}
}

// operationType represents the type of operation being performed on an index.
// It is used internally to distinguish between different DML operations
// and route them to the appropriate index methods.
type operationType int

const (
	insertOp operationType = iota
	deleteOp
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

	indexes, err := i.getIndexesForTable(ctx, tableID)
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
func (im *IndexManager) OnDelete(ctx TxCtx, tableID primitives.FileID, t *tuple.Tuple) error {
	return im.processIndexOperation(ctx, tableID, t, deleteOp)
}

// OnUpdate maintains all indexes for a table when a tuple is updated.
// This is implemented as a delete-then-insert operation to ensure proper
// index maintenance when indexed column values change.
//
// If the insertion fails after deletion, the method attempts to re-insert
// the old tuple to maintain consistency, though this rollback may also fail.
// Callers should implement proper transaction rollback mechanisms.
func (im *IndexManager) OnUpdate(ctx TxCtx, tableID primitives.FileID, old, updated *tuple.Tuple) error {
	if err := im.OnDelete(ctx, tableID, old); err != nil {
		return err
	}

	if err := im.OnInsert(ctx, tableID, updated); err != nil {
		_ = im.OnInsert(ctx, tableID, old)
		return err
	}

	return nil
}

// loadAndOpenIndexes loads index metadata from catalog and opens all index files for a table.
// This is the main entry point for initializing all indexes associated with a table.
// It performs the following steps:
//  1. Loads index metadata from the catalog
//  2. Opens each index file
//  3. Returns a slice of indexes with their metadata
//
// If an index file fails to open, a warning is printed but the process continues
// with the remaining indexes.
//
// Parameters:
//   - ctx: Transaction context for catalog operations
//   - tableID: The ID of the table whose indexes should be loaded
//
// Returns:
//   - A slice of indexWithMetadata containing successfully opened indexes
//   - An error if catalog access fails
func (i *IndexManager) LoadIndexes(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*IndexWithMetadata, error) {
	metadataList, err := i.loadFromCatalog(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	indexes := make([]*IndexWithMetadata, 0, len(metadataList))
	for _, metadata := range metadataList {
		idx, err := i.openIndex(tx, metadata.KeyType, metadata.FilePath, metadata.IndexType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open index %s: %v\n", metadata.IndexName, err)
			continue
		}

		indexes = append(indexes, &IndexWithMetadata{
			index:    idx,
			metadata: metadata,
		})
	}

	return indexes, nil
}

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
//
// Parameters:
//   - tableID: The ID of the table whose index metadata should be loaded
//
// Returns:
//   - A slice of complete IndexMetadata with resolved schema information
//   - An error if catalog access or schema retrieval fails
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
//
// If a column referenced by an index is not found in the schema, a warning is printed
// and that index is skipped.
//
// Parameters:
//   - catalogIndexes: Raw index metadata from the catalog
//   - schema: The table schema containing column information
//
// Returns a slice of IndexMetadata with resolved column indices and key types.
func resolveIndexMetadata(catalogIndexes []*systemtable.IndexMetadata, schema *schema.Schema) []*IndexMetadata {
	result := make([]*IndexMetadata, 0, len(catalogIndexes))

	for _, catIdx := range catalogIndexes {
		columnIndex, keyType, err := findColumnInfo(schema, catIdx.ColumnName)

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

// findColumnInfo finds the column index and type in the schema.
// This helper function searches for a column by name in the table schema.
//
// Parameters:
//   - schema: The table schema to search
//   - columnName: The name of the column to find
//
// Returns:
//   - The zero-based index of the column in the schema
//   - The data type of the column
//   - Returns (-1, IntType) if the column is not found
func findColumnInfo(schema *schema.Schema, columnName string) (primitives.ColumnID, types.Type, error) {
	var i primitives.ColumnID
	for i = 0; i < schema.TupleDesc.NumFields(); i++ {
		fieldName, _ := schema.TupleDesc.GetFieldName(i)
		if fieldName == columnName {
			return i, schema.TupleDesc.Types[i], nil
		}
	}
	return 0, types.IntType, fmt.Errorf("column %s not found", columnName)
}
