package indexmanager

import (
	"storemy/pkg/primitives"
	"sync"
)

// indexCache manages the caching of loaded indexes for tables.
// It provides thread-safe access to cached indexes with lazy loading support.
type indexCache struct {
	// Cache of loaded indexes: tableID -> []*IndexWithMetadata
	cache map[primitives.FileID][]*IndexWithMetadata
	mu    sync.RWMutex
}

// newIndexCache creates a new index cache.
func newIndexCache() *indexCache {
	return &indexCache{
		cache: make(map[primitives.FileID][]*IndexWithMetadata),
	}
}

// Get retrieves cached indexes for a table.
// Returns (indexes, true) if cached, (nil, false) if not found.
func (ic *indexCache) Get(tableID primitives.FileID) ([]*IndexWithMetadata, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	indexes, exists := ic.cache[tableID]
	return indexes, exists
}

// Set stores indexes for a table in the cache.
// This method uses double-check locking pattern to prevent race conditions
// when multiple goroutines try to load indexes for the same table.
func (ic *indexCache) Set(tableID primitives.FileID, indexes []*IndexWithMetadata) []*IndexWithMetadata {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if cached, exists := ic.cache[tableID]; exists {
		return cached
	}

	ic.cache[tableID] = indexes
	return indexes
}

// Invalidate removes cached indexes for a table.
// This is typically called when indexes are created or dropped for a table.
func (ic *indexCache) Invalidate(tableID primitives.FileID) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	delete(ic.cache, tableID)
}

// Clear removes all cached indexes and returns them for cleanup.
// This is typically called during shutdown.
func (ic *indexCache) Clear() map[primitives.FileID][]*IndexWithMetadata {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	old := ic.cache
	ic.cache = make(map[primitives.FileID][]*IndexWithMetadata)
	return old
}

// GetOrLoad attempts to get indexes from cache, or loads them using the provided loader function.
// This implements the full lazy-loading pattern with double-check locking.
func (ic *indexCache) GetOrLoad(tableID primitives.FileID, loader func() ([]*IndexWithMetadata, error)) ([]*IndexWithMetadata, error) {
	if indexes, exists := ic.Get(tableID); exists {
		return indexes, nil
	}

	indexes, err := loader()
	if err != nil {
		return nil, err
	}

	return ic.Set(tableID, indexes), nil
}

// getIndexesForTable retrieves all indexes for a given table with caching.
// This is a helper method that combines cache lookup with loading from catalog.
//
// IMPORTANT: Caching is currently disabled because index wrappers (BTree/HashIndex)
// contain transaction contexts. Caching them causes issues when a new transaction
// tries to use an index created with an old transaction's context.
// TODO: Implement proper transaction-aware caching or cache index files separately
func (im *IndexManager) getIndexesForTable(ctx TxCtx, tableID primitives.FileID) ([]*IndexWithMetadata, error) {
	// Temporarily disable caching to fix transaction context issues
	// Always reload indexes for each transaction
	return im.NewLoader(ctx).LoadIndexes(tableID)

	// Old cached implementation (causes transaction ID errors):
	// return im.cache.GetOrLoad(tableID, func() ([]*IndexWithMetadata, error) {
	// 	return im.loadAndOpenIndexes(ctx, tableID)
	// })
}
