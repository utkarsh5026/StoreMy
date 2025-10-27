package indexmanager

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
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
type indexWithMetadata struct {
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

	cache  *indexCache
	loader *indexLoader
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
	im.loader = newIndexLoader(catalog, pageStore)
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
