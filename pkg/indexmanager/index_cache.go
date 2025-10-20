package indexmanager

import (
	"storemy/pkg/concurrency/transaction"
	"sync"
)

// indexCache manages the caching of loaded indexes for tables.
// It provides thread-safe access to cached indexes with lazy loading support.
type indexCache struct {
	// Cache of loaded indexes: tableID -> []*indexWithMetadata
	cache map[int][]*indexWithMetadata
	mu    sync.RWMutex
}

// newIndexCache creates a new index cache.
func newIndexCache() *indexCache {
	return &indexCache{
		cache: make(map[int][]*indexWithMetadata),
	}
}

// Get retrieves cached indexes for a table.
// Returns (indexes, true) if cached, (nil, false) if not found.
func (ic *indexCache) Get(tableID int) ([]*indexWithMetadata, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	indexes, exists := ic.cache[tableID]
	return indexes, exists
}

// Set stores indexes for a table in the cache.
// This method uses double-check locking pattern to prevent race conditions
// when multiple goroutines try to load indexes for the same table.
func (ic *indexCache) Set(tableID int, indexes []*indexWithMetadata) []*indexWithMetadata {
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
func (ic *indexCache) Invalidate(tableID int) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	delete(ic.cache, tableID)
}

// Clear removes all cached indexes and returns them for cleanup.
// This is typically called during shutdown.
func (ic *indexCache) Clear() map[int][]*indexWithMetadata {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	old := ic.cache
	ic.cache = make(map[int][]*indexWithMetadata)
	return old
}

// GetOrLoad attempts to get indexes from cache, or loads them using the provided loader function.
// This implements the full lazy-loading pattern with double-check locking.
func (ic *indexCache) GetOrLoad(
	tableID int,
	loader func() ([]*indexWithMetadata, error),
) ([]*indexWithMetadata, error) {
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
func (im *IndexManager) getIndexesForTable(
	ctx *transaction.TransactionContext,
	tableID int,
) ([]*indexWithMetadata, error) {
	return im.cache.GetOrLoad(tableID, func() ([]*indexWithMetadata, error) {
		return im.loadAndOpenIndexes(ctx, tableID)
	})
}
