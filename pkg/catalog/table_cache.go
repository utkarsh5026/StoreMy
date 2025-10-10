package catalog

import (
	"container/list"
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// tableInfo holds metadata about a table in the cache
// Enhanced with statistics caching and pre-computed metadata
type tableInfo struct {
	File         page.DbFile
	Schema       *schema.Schema
	Stats        *TableStatistics
	StatsExpiry  time.Time
	LastAccessed time.Time
	lruElement   *list.Element
}

// newTableInfo creates a new table info instance with pre-computed metadata
func newTableInfo(file page.DbFile, schema *schema.Schema) *tableInfo {
	return &tableInfo{
		File:         file,
		Schema:       schema,
		LastAccessed: time.Now(),
	}
}

// GetID returns the table's unique identifier
func (ti *tableInfo) GetFileID() int {
	return ti.File.GetID()
}

// cacheTTLConfig holds time-to-live settings for cached data
type cacheTTLConfig struct {
	StatsTTL time.Duration // How long to cache statistics
}

// DefaultCacheTTL returns default TTL configuration
func DefaultCacheTTL() cacheTTLConfig {
	return cacheTTLConfig{
		StatsTTL: 5 * time.Minute, // Statistics valid for 5 minutes
	}
}

// cacheMetrics tracks cache performance for observability
type cacheMetrics struct {
	hits          atomic.Int64
	misses        atomic.Int64
	statsHits     atomic.Int64
	statsMisses   atomic.Int64
	evictions     atomic.Int64
	columnLookups atomic.Int64
}

// tableCache is an internal in-memory cache for table metadata.
// It maintains bidirectional mappings between table names and IDs for efficient lookups.
// This is private to the catalog package and should only be accessed via CatalogManager.
//
// Design:
//   - Acts as a performance optimization layer over disk-based SystemCatalog
//   - Provides O(1) lookups for table metadata by name or ID
//   - Thread-safe for concurrent access
//   - LRU eviction policy to prevent unbounded memory growth
//   - Caches statistics and pre-computed metadata for query optimization
//   - Does NOT handle persistence - that's CatalogManager's responsibility
type tableCache struct {
	nameToTable map[string]*tableInfo
	idToTable   map[int]*tableInfo
	lruList     *list.List
	maxSize     int
	ttl         cacheTTLConfig
	metrics     cacheMetrics
	mutex       sync.RWMutex
}

// newTableCache creates a new empty tableCache instance.
// If maxSize is 0, the cache grows without bounds (original behavior).
// If maxSize > 0, LRU eviction is used when the limit is reached.
func newTableCache() *tableCache {
	return &tableCache{
		nameToTable: make(map[string]*tableInfo),
		idToTable:   make(map[int]*tableInfo),
		lruList:     list.New(),
		maxSize:     0, // Unlimited by default
		ttl:         DefaultCacheTTL(),
	}
}

// newTableCacheWithLimit creates a cache with a maximum size limit
func newTableCacheWithLimit(maxSize int) *tableCache {
	return &tableCache{
		nameToTable: make(map[string]*tableInfo),
		idToTable:   make(map[int]*tableInfo),
		lruList:     list.New(),
		maxSize:     maxSize,
		ttl:         DefaultCacheTTL(),
	}
}

// addTable adds a new table to the cache with the specified database file and schema.
// If a table with the same name or ID already exists, it will be replaced.
// Implements LRU eviction if maxSize is configured and cache is full.
func (tc *tableCache) addTable(f page.DbFile, schema *schema.Schema) error {
	if f == nil {
		return fmt.Errorf("file cannot be nil")
	}
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	name := schema.TableName
	if name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info := newTableInfo(f, schema)
	tid := f.GetID()

	tc.removeExistingTable(name, tid)

	if tc.maxSize > 0 && len(tc.idToTable) >= tc.maxSize {
		tc.evictLRU()
	}

	tc.addTableToMaps(name, tid, info)
	info.lruElement = tc.lruList.PushFront(tid)

	return nil
}

// getTableID retrieves the unique identifier for a table given its name.
// Updates LRU position on access.
func (tc *tableCache) getTableID(tableName string) (int, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.nameToTable[tableName]
	if !exists {
		tc.metrics.misses.Add(1)
		return 0, fmt.Errorf("table '%s' not found", tableName)
	}

	tc.metrics.hits.Add(1)
	tc.markAsUsed(info)

	return info.GetFileID(), nil
}

// getTupleDesc retrieves the tuple description for a table by ID.
// Updates LRU position on access.
func (tc *tableCache) getTupleDesc(tableId int) (*tuple.TupleDescription, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.idToTable[tableId]
	if !exists {
		tc.metrics.misses.Add(1)
		return nil, fmt.Errorf("table with ID %d not found", tableId)
	}

	tc.metrics.hits.Add(1)
	tc.markAsUsed(info)

	return info.Schema.TupleDesc, nil
}

// getDbFile retrieves the database file for a table by ID.
// Updates LRU position on access.
func (tc *tableCache) getDbFile(tableId int) (page.DbFile, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.idToTable[tableId]
	if !exists {
		tc.metrics.misses.Add(1)
		return nil, fmt.Errorf("table with ID %d not found", tableId)
	}

	tc.metrics.hits.Add(1)
	tc.markAsUsed(info)

	return info.File, nil
}

// removeTable removes a table from the cache and closes its associated database file.
// This operation is irreversible and will close the underlying file handle.
func (tc *tableCache) removeTable(name string) error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.nameToTable[name]
	if !exists {
		return fmt.Errorf("table '%s' not found", name)
	}

	if info.lruElement != nil {
		tc.lruList.Remove(info.lruElement)
	}

	if info.File != nil {
		if err := info.File.Close(); err != nil {
			fmt.Printf("Warning: failed to close file for table '%s': %v\n", name, err)
		}
	}

	delete(tc.nameToTable, name)
	delete(tc.idToTable, info.GetFileID())
	return nil
}

// clear removes all tables from the cache and closes all associated database files.
// This operation cannot be undone. File closure errors are logged as warnings.
func (tc *tableCache) clear() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	for _, info := range tc.idToTable {
		if info.File == nil {
			continue
		}

		if err := info.File.Close(); err != nil {
			fmt.Printf("Warning: failed to close file for table '%s': %v\n", info.Schema.TableName, err)
		}
	}

	clear(tc.nameToTable)
	clear(tc.idToTable)
	tc.lruList.Init()
}

// validateIntegrity performs internal consistency checks on the cache data structures.
// This method verifies that the bidirectional mappings between names and IDs are consistent.
//
// Returns an error if any integrity violations are detected, nil otherwise.
func (tc *tableCache) validateIntegrity() error {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	if len(tc.nameToTable) != len(tc.idToTable) {
		return fmt.Errorf("cache integrity violation: map size mismatch")
	}

	for name, table := range tc.nameToTable {
		if t, exists := tc.idToTable[table.GetFileID()]; !exists {
			return fmt.Errorf("cache integrity violation: table %s missing from ID map", name)
		} else if t != table {
			return fmt.Errorf("cache integrity violation: table %s reference mismatch", name)
		}
	}

	for id, table := range tc.idToTable {
		if otherTable, exists := tc.nameToTable[table.Schema.TableName]; !exists {
			return fmt.Errorf("cache integrity violation: table ID %d missing from name map", id)
		} else if otherTable != table {
			return fmt.Errorf("cache integrity violation: table ID %d reference mismatch", id)
		}
	}

	return nil
}

// getAllTableNames returns a slice containing the names of all tables in the cache.
// The returned slice is a copy and can be safely modified without affecting the cache.
func (tc *tableCache) getAllTableNames() []string {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return slices.Collect(maps.Keys(tc.nameToTable))
}

// removeExistingTable removes any existing table with the given name or ID from both maps.
// This is a helper method used internally during table addition to handle replacements.
// Must be called with write lock held.
func (tc *tableCache) removeExistingTable(name string, tableID int) {
	if t, exists := tc.nameToTable[name]; exists {
		delete(tc.idToTable, t.GetFileID())
	}

	if t, exists := tc.idToTable[tableID]; exists {
		delete(tc.nameToTable, t.Schema.TableName)
	}
}

// addTableToMaps adds a table to both the name-to-table and ID-to-table mappings.
// This is a helper method used internally during table addition.
// Must be called with write lock held.
func (tc *tableCache) addTableToMaps(name string, tableID int, info *tableInfo) {
	tc.nameToTable[name] = info
	tc.idToTable[tableID] = info
}

// tableExists checks whether a table with the given name exists in the cache.
func (tc *tableCache) tableExists(name string) bool {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	_, exists := tc.nameToTable[name]
	return exists
}

// getTableInfo retrieves the full table info for a table by ID.
// Updates LRU position on access.
func (tc *tableCache) getTableInfo(tableID int) (*tableInfo, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.idToTable[tableID]
	if !exists {
		tc.metrics.misses.Add(1)
		return nil, fmt.Errorf("table with ID %d not found", tableID)
	}

	tc.metrics.hits.Add(1)
	tc.markAsUsed(info)

	return info, nil
}

// renameTable changes the name of an existing table in the cache.
// The operation maintains all other table metadata and file associations.
func (tc *tableCache) renameTable(oldName, newName string) error {
	if oldName == "" || newName == "" {
		return fmt.Errorf("table names cannot be empty")
	}
	if strings.TrimSpace(newName) != newName {
		return fmt.Errorf("new table name cannot have leading or trailing whitespace")
	}

	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, exists := tc.nameToTable[oldName]
	if !exists {
		return fmt.Errorf("table '%s' not found", oldName)
	}

	if _, exists := tc.nameToTable[newName]; exists {
		return fmt.Errorf("table '%s' already exists", newName)
	}

	info.Schema.TableName = newName
	delete(tc.nameToTable, oldName)
	tc.nameToTable[newName] = info
	return nil
}
