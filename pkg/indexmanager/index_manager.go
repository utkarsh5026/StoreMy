package indexmanager

import (
	"fmt"
	"os"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

// CatalogReader defines the interface for reading index metadata from the catalog.
// This avoids circular dependencies with the catalog package.
type CatalogReader interface {
	// GetIndexesByTable retrieves all indexes for a given table
	GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]*IndexMetadata, error)

	// GetTableSchema retrieves the schema for a table
	GetTableSchema(tableID int) (*schema.Schema, error)
}

// IndexMetadata represents metadata for a database index from catalog
type IndexMetadata struct {
	IndexID    int
	IndexName  string
	TableID    int
	ColumnName string
	IndexType  string
	FilePath   string
	CreatedAt  int64
}

// enrichedIndexMetadata extends IndexMetadata with schema information
type enrichedIndexMetadata struct {
	IndexID     int
	IndexName   string
	TableID     int
	ColumnName  string
	ColumnIndex int // Field index in tuple (0-based)
	IndexType   index.IndexType
	KeyType     types.Type
	FilePath    string
	CreatedAt   int64
}

// indexWithMetadata wraps an index (BTree or HashIndex) with its metadata
// Using interface{} to avoid interface implementation issues
type indexWithMetadata struct {
	index    index.Index // Either *btreeindex.BTree or *hashindex.HashIndex
	metadata *enrichedIndexMetadata
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
	wal       *log.WAL

	// Cache of loaded indexes: tableID -> []*indexWithMetadata
	// Uses lazy loading: indexes are loaded on first access
	indexCache map[int][]*indexWithMetadata
	cacheMu    sync.RWMutex
}

// NewIndexManager creates a new IndexManager instance.
//
// Parameters:
//   - catalog: Interface to read index metadata from system catalog
//   - pageStore: Page store for managing index pages
//   - wal: Write-ahead log for logging index operations
//
// Returns a new IndexManager ready to manage indexes.
func NewIndexManager(catalog CatalogReader, pageStore *memory.PageStore, wal *log.WAL) *IndexManager {
	return &IndexManager{
		catalog:    catalog,
		pageStore:  pageStore,
		wal:        wal,
		indexCache: make(map[int][]*indexWithMetadata),
	}
}

// getIndexesForTable retrieves all indexes for a given table (internal method).
// This method uses lazy loading and caching:
//  1. Check cache first (fast path)
//  2. If not cached, load from catalog and open index files
//  3. Store in cache for future access
//
// Parameters:
//   - ctx: Transaction context for reading catalog
//   - tableID: Table whose indexes to retrieve
//
// Returns a slice of indexWithMetadata for the table, or an error if catalog access fails.
//
// Thread-safe: Uses read/write locks for cache access.
func (im *IndexManager) getIndexesForTable(ctx *transaction.TransactionContext, tableID int) ([]*indexWithMetadata, error) {
	// Fast path: check cache with read lock
	im.cacheMu.RLock()
	cached, exists := im.indexCache[tableID]
	im.cacheMu.RUnlock()

	if exists {
		return cached, nil
	}

	// Slow path: load from catalog
	im.cacheMu.Lock()
	defer im.cacheMu.Unlock()

	// Double-check pattern: another goroutine might have loaded while we waited
	if cached, exists := im.indexCache[tableID]; exists {
		return cached, nil
	}

	// Load index metadata from catalog
	metadataList, err := im.loadIndexMetadataFromCatalog(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	// Open index files
	indexes := make([]*indexWithMetadata, 0, len(metadataList))
	for _, metadata := range metadataList {
		idx, err := im.openIndex(ctx, metadata)
		if err != nil {
			// Log error but continue with other indexes
			fmt.Fprintf(os.Stderr, "Warning: failed to open index %s: %v\n", metadata.IndexName, err)
			continue
		}
		indexes = append(indexes, &indexWithMetadata{
			index:    idx,
			metadata: metadata,
		})
	}

	// Cache the loaded indexes
	im.indexCache[tableID] = indexes

	return indexes, nil
}

// loadIndexMetadataFromCatalog loads index metadata from the system catalog
// and enriches it with schema information (ColumnIndex, KeyType).
func (im *IndexManager) loadIndexMetadataFromCatalog(ctx *transaction.TransactionContext, tableID int) ([]*enrichedIndexMetadata, error) {
	// Get indexes from catalog
	catalogIndexes, err := im.catalog.GetIndexesByTable(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes from catalog: %v", err)
	}

	if len(catalogIndexes) == 0 {
		return []*enrichedIndexMetadata{}, nil
	}

	// Get table schema to map column names to indices
	schema, err := im.catalog.GetTableSchema(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	// Convert to enrichedIndexMetadata with ColumnIndex and KeyType
	result := make([]*enrichedIndexMetadata, 0, len(catalogIndexes))
	for _, catIdx := range catalogIndexes {
		// Find column index in schema
		columnIndex := -1
		var keyType types.Type

		for i := 0; i < schema.TupleDesc.NumFields(); i++ {
			fieldName, _ := schema.TupleDesc.GetFieldName(i)
			if fieldName == catIdx.ColumnName {
				columnIndex = i
				keyType = schema.TupleDesc.Types[i]
				break
			}
		}

		if columnIndex == -1 {
			fmt.Fprintf(os.Stderr, "Warning: column %s not found in schema for index %s\n",
				catIdx.ColumnName, catIdx.IndexName)
			continue
		}

		result = append(result, &enrichedIndexMetadata{
			IndexID:     catIdx.IndexID,
			IndexName:   catIdx.IndexName,
			TableID:     catIdx.TableID,
			ColumnName:  catIdx.ColumnName,
			ColumnIndex: columnIndex,
			IndexType:   index.IndexType(catIdx.IndexType),
			KeyType:     keyType,
			FilePath:    catIdx.FilePath,
			CreatedAt:   catIdx.CreatedAt,
		})
	}

	return result, nil
}

// openIndex opens an index file based on its metadata.
func (im *IndexManager) openIndex(ctx *transaction.TransactionContext, metadata *enrichedIndexMetadata) (index.Index, error) {
	switch metadata.IndexType {
	case index.BTreeIndex:
		return im.openBTreeIndex(ctx, metadata)
	case index.HashIndex:
		return im.openHashIndex(ctx, metadata)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", metadata.IndexType)
	}
}

// openBTreeIndex opens a B+Tree index file.
func (im *IndexManager) openBTreeIndex(ctx *transaction.TransactionContext, metadata *enrichedIndexMetadata) (*btreeindex.BTree, error) {
	file, err := btree.NewBTreeFile(metadata.FilePath, metadata.KeyType)
	if err != nil {
		return nil, fmt.Errorf("failed to open BTree file: %v", err)
	}

	btreeIdx := btreeindex.NewBTree(metadata.IndexID, metadata.KeyType, file, ctx, im.pageStore)
	return btreeIdx, nil
}

// openHashIndex opens a hash index file.
func (im *IndexManager) openHashIndex(ctx *transaction.TransactionContext, metadata *enrichedIndexMetadata) (*hashindex.HashIndex, error) {
	numBuckets := hash.DefaultBuckets

	file, err := hash.NewHashFile(metadata.FilePath, metadata.KeyType, numBuckets)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash file: %v", err)
	}

	// Register the file with the PageStore
	im.pageStore.RegisterDbFile(metadata.IndexID, file)

	hashIdx := hashindex.NewHashIndex(metadata.IndexID, metadata.KeyType, file, im.pageStore, ctx)
	return hashIdx, nil
}

// OnInsert maintains all indexes for a table when a tuple is inserted.
func (im *IndexManager) OnInsert(ctx *transaction.TransactionContext, tableID int, t *tuple.Tuple) error {
	if t == nil || t.RecordID == nil {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := im.getIndexesForTable(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idxWithMeta := range indexes {
		key, err := im.extractKey(t, idxWithMeta.metadata)
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", idxWithMeta.metadata.IndexName, err)
		}

		if btreeIdx, ok := idxWithMeta.index.(*btreeindex.BTree); ok {
			if err := btreeIdx.Insert(key, t.RecordID); err != nil {
				return fmt.Errorf("failed to insert into index %s: %v", idxWithMeta.metadata.IndexName, err)
			}
		} else if hashIdx, ok := idxWithMeta.index.(*hashindex.HashIndex); ok {
			if err := hashIdx.Insert(key, t.RecordID); err != nil {
				return fmt.Errorf("failed to insert into index %s: %v", idxWithMeta.metadata.IndexName, err)
			}
		} else {
			return fmt.Errorf("unsupported index type for %s", idxWithMeta.metadata.IndexName)
		}
	}

	return nil
}

// OnDelete maintains all indexes for a table when a tuple is deleted.
func (im *IndexManager) OnDelete(ctx *transaction.TransactionContext, tableID int, t *tuple.Tuple) error {
	if t == nil || t.RecordID == nil {
		return fmt.Errorf("tuple must be non-nil and have a RecordID")
	}

	indexes, err := im.getIndexesForTable(ctx, tableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes for table %d: %v", tableID, err)
	}

	if len(indexes) == 0 {
		return nil
	}

	for _, idxWithMeta := range indexes {
		key, err := im.extractKey(t, idxWithMeta.metadata)
		if err != nil {
			return fmt.Errorf("failed to extract key for index %s: %v", idxWithMeta.metadata.IndexName, err)
		}

		if btreeIdx, ok := idxWithMeta.index.(*btreeindex.BTree); ok {
			if err := btreeIdx.Delete(key, t.RecordID); err != nil {
				return fmt.Errorf("failed to delete from index %s: %v", idxWithMeta.metadata.IndexName, err)
			}
		} else if hashIdx, ok := idxWithMeta.index.(*hashindex.HashIndex); ok {
			if err := hashIdx.Delete(key, t.RecordID); err != nil {
				return fmt.Errorf("failed to delete from index %s: %v", idxWithMeta.metadata.IndexName, err)
			}
		} else {
			return fmt.Errorf("unsupported index type for %s", idxWithMeta.metadata.IndexName)
		}
	}

	return nil
}

// OnUpdate maintains all indexes for a table when a tuple is updated.
func (im *IndexManager) OnUpdate(ctx *transaction.TransactionContext, tableID int, oldTuple *tuple.Tuple, newTuple *tuple.Tuple) error {
	if err := im.OnDelete(ctx, tableID, oldTuple); err != nil {
		return err
	}

	if err := im.OnInsert(ctx, tableID, newTuple); err != nil {
		_ = im.OnInsert(ctx, tableID, oldTuple)
		return err
	}

	return nil
}

// extractKey extracts the indexed column value from a tuple.
func (im *IndexManager) extractKey(t *tuple.Tuple, metadata *enrichedIndexMetadata) (types.Field, error) {
	if metadata.ColumnIndex < 0 || metadata.ColumnIndex >= t.TupleDesc.NumFields() {
		return nil, fmt.Errorf("invalid column index %d for tuple with %d fields",
			metadata.ColumnIndex, t.TupleDesc.NumFields())
	}

	field, err := t.GetField(metadata.ColumnIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get field at index %d: %v", metadata.ColumnIndex, err)
	}

	if field.Type() != metadata.KeyType {
		return nil, fmt.Errorf("field type mismatch: expected %v, got %v",
			metadata.KeyType, field.Type())
	}

	return field, nil
}

// InvalidateCache removes cached indexes for a table.
func (im *IndexManager) InvalidateCache(tableID int) {
	im.cacheMu.Lock()
	defer im.cacheMu.Unlock()
	delete(im.indexCache, tableID)
}

// Close releases all resources held by the IndexManager.
func (im *IndexManager) Close() error {
	im.cacheMu.Lock()
	defer im.cacheMu.Unlock()

	var firstError error

	for tableID, indexes := range im.indexCache {
		for _, idxWithMeta := range indexes {
			// Only close BTree indexes (they have Close method)
			if btreeIdx, ok := idxWithMeta.index.(*btreeindex.BTree); ok {
				if err := btreeIdx.Close(); err != nil && firstError == nil {
					firstError = fmt.Errorf("failed to close index for table %d: %v", tableID, err)
				}
			}
			// HashIndex doesn't have Close method, skip it
		}
	}

	im.indexCache = make(map[int][]*indexWithMetadata)

	return firstError
}
