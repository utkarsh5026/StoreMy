# Index Manager Package

The `indexmanager` package provides comprehensive index management functionality for the StoreMy database. It handles the complete lifecycle of database indexes including creation, population, maintenance, caching, and cleanup.

## Overview

The IndexManager coordinates all index-related operations in the database:
- **Index Lifecycle**: Creating, populating, and deleting physical index files
- **Index Loading**: Loading index metadata from catalog and opening index files
- **Index Caching**: Caching opened indexes for performance
- **Index Maintenance**: Automatically maintaining indexes during DML operations (INSERT, UPDATE, DELETE)
- **Transaction Integration**: Coordinating index operations with transactions and WAL

## Architecture

The package follows a modular design with separation of concerns:

```
IndexManager (main coordinator)
├── indexCache       - Thread-safe index caching with lazy loading
├── indexLoader      - Loads metadata from catalog and opens index files
├── indexLifecycle   - Physical index file operations (create, populate, delete)
└── indexMaintenance - DML operation handlers (insert, delete, update)
```

## Core Components

### IndexManager

The main entry point for all index operations.

```go
type IndexManager struct {
    catalog   CatalogReader       // Interface to read from system catalog
    pageStore *memory.PageStore   // Page management
    wal       *wal.WAL            // Write-ahead logging

    cache       *indexCache
    loader      *indexLoader
    maintenance *indexMaintenance
    lifecycle   *indexLifecycle
}
```

**Key Responsibilities:**
- Coordinate all index operations
- Manage index cache and lifecycle
- Provide unified API for index operations
- Ensure transaction-aware index updates

### IndexMetadata

Complete metadata for a database index, combining catalog information with resolved schema data.

```go
type IndexMetadata struct {
    // From catalog
    IndexID    int
    IndexName  string
    TableID    int
    ColumnName string
    IndexType  index.IndexType  // HashIndex or BTreeIndex
    FilePath   string
    CreatedAt  int64

    // Resolved from schema
    ColumnIndex int         // Field index in tuple (0-based)
    KeyType     types.Type  // Type of indexed column
}
```

### CatalogReader Interface

Abstraction to avoid circular dependencies with the catalog package.

```go
type CatalogReader interface {
    GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]*systemtable.IndexMetadata, error)
    GetTableSchema(tableID int) (*schema.Schema, error)
}
```

## Usage Examples

### Creating an IndexManager

```go
import (
    "storemy/pkg/indexmanager"
    "storemy/pkg/catalog"
    "storemy/pkg/memory"
    "storemy/pkg/log"
)

// Initialize dependencies
catalog := catalog.NewCatalog(...)
pageStore := memory.NewPageStore(...)
wal := wal.NewWAL(...)

// Create IndexManager
indexMgr := indexmanager.NewIndexManager(catalog, pageStore, wal)
defer indexMgr.Close()
```

### Creating a New Index

```go
// Step 1: Create physical index file
err := indexMgr.CreatePhysicalIndex(
    "/data/indexes/idx_user_email.dat",
    types.StringType,           // Key type
    index.HashIndex,            // Index type
)
if err != nil {
    // handle error
}

// Step 2: Populate index from existing table data
err = indexMgr.PopulateIndex(
    ctx,                        // Transaction context
    "/data/indexes/idx_user_email.dat",
    indexID,                    // Index ID from catalog
    tableFile,                  // Heap file for the table
    2,                          // Column index (email column)
    types.StringType,
    index.HashIndex,
)
if err != nil {
    // handle error
}

// Step 3: Invalidate cache to pick up new index
indexMgr.InvalidateCache(tableID)
```

### Maintaining Indexes on Insert

```go
// Insert tuple into table
tuple := createTuple(...)
tableFile.InsertTuple(ctx, tuple)

// Automatically maintain all indexes for the table
err := indexMgr.OnInsert(ctx, tableID, tuple)
if err != nil {
    // handle error - may need to rollback table insert
}
```

### Maintaining Indexes on Delete

```go
// Delete tuple from table
tuple := getTupleToDelete(...)
tableFile.DeleteTuple(ctx, tuple.RecordID)

// Automatically maintain all indexes for the table
err := indexMgr.OnDelete(ctx, tableID, tuple)
if err != nil {
    // handle error
}
```

### Maintaining Indexes on Update

```go
// Update tuple in table
oldTuple := getOldTuple(...)
newTuple := createNewTuple(...)

// Update performs delete + insert with automatic rollback on failure
err := indexMgr.OnUpdate(ctx, tableID, oldTuple, newTuple)
if err != nil {
    // handle error - old tuple keys are automatically restored
}
```

### Deleting an Index

```go
// Step 1: Remove from catalog (application responsibility)
catalog.DropIndex(ctx, indexID)

// Step 2: Invalidate cache
indexMgr.InvalidateCache(tableID)

// Step 3: Delete physical file
err := indexMgr.DeletePhysicalIndex("/data/indexes/idx_user_email.dat")
if err != nil {
    // handle error
}
```

## Component Details

### Index Cache ([index_cache.go](index_cache.go))

Thread-safe caching with lazy loading and double-check locking pattern.

**Key Features:**
- Thread-safe read/write operations using sync.RWMutex
- Lazy loading with GetOrLoad pattern
- Cache invalidation for table-level changes
- Bulk clear for shutdown

**Methods:**
```go
Get(tableID int) ([]*indexWithMetadata, bool)
Set(tableID int, indexes []*indexWithMetadata) []*indexWithMetadata
GetOrLoad(tableID int, loader func() (...)) ([]*indexWithMetadata, error)
Invalidate(tableID int)
Clear() map[int][]*indexWithMetadata
```

### Index Loader ([index_loader.go](index_loader.go))

Loads index metadata from catalog and opens physical index files.

**Key Features:**
- Loads metadata from system catalog
- Resolves column information from table schema
- Opens appropriate index type (BTree or Hash)
- Handles missing columns gracefully with warnings

**Methods:**
```go
loadAndOpenIndexes(ctx TxCtx, tableID int) ([]*indexWithMetadata, error)
loadFromCatalog(ctx TxCtx, tableID int) ([]*IndexMetadata, error)
openIndex(ctx TxCtx, m *IndexMetadata) (index.Index, error)
```

### Index Lifecycle ([index_lifecycle.go](index_lifecycle.go))

Manages physical index file operations.

**Key Features:**
- Creates appropriate index files (Hash or BTree)
- Populates indexes by scanning table data
- Supports sequential scan for bulk loading
- Handles index file deletion

**Methods:**
```go
CreatePhysicalIndex(filePath string, keyType types.Type, indexType IndexType) error
PopulateIndex(ctx TxCtx, filePath string, indexID int, tableFile page.DbFile, ...) error
DeletePhysicalIndex(filePath string) error
```

### Index Maintenance ([index_maintenance.go](index_maintenance.go))

Maintains indexes during DML operations.

**Key Features:**
- Automatically updates all indexes on tuple changes
- Type-agnostic (works with both BTree and Hash indexes)
- Rollback support on update failures
- Validates tuple and RecordID requirements

**Methods:**
```go
OnInsert(ctx TxCtx, tableID int, t *tuple.Tuple) error
OnDelete(ctx TxCtx, tableID int, t *tuple.Tuple) error
OnUpdate(ctx TxCtx, tableID int, oldTuple, newTuple *tuple.Tuple) error
```

## Supported Index Types

### Hash Index
- **Type**: `index.HashIndex`
- **Implementation**: `hashindex.HashIndex`
- **Best for**: Equality lookups (WHERE col = value)
- **Storage**: Hash buckets with chaining
- **Default buckets**: Configurable via `hash.DefaultBuckets`

### B+Tree Index
- **Type**: `index.BTreeIndex`
- **Implementation**: `btreeindex.BTree`
- **Best for**: Range queries (WHERE col > value, ORDER BY col)
- **Storage**: Balanced tree structure
- **Features**: Ordered traversal, range scans

## Transaction Integration

All index operations are transaction-aware:
- Index updates are logged to WAL
- Supports rollback via transaction context
- Consistent with table modifications
- ACID compliance through transaction coordination

## Performance Considerations

### Caching Strategy
- Indexes are cached per table after first access
- Cache invalidation on structural changes (CREATE/DROP INDEX)
- Thread-safe concurrent access
- Minimal locking with RWMutex

### Lazy Loading
- Indexes loaded only when needed
- Double-check locking prevents duplicate loads
- Graceful handling of missing indexes

### Bulk Operations
- PopulateIndex uses sequential scan for efficiency
- All indexes updated in single pass during DML
- Minimal overhead for tables without indexes

## Error Handling

The package provides detailed error messages:
- Index file I/O errors
- Catalog lookup failures
- Schema resolution failures
- Type mismatches
- Invalid column indexes

All errors are wrapped with context for debugging.

## Best Practices

1. **Always call Close()**: Ensure proper cleanup of resources
   ```go
   defer indexMgr.Close()
   ```

2. **Invalidate cache after structural changes**: After CREATE/DROP INDEX
   ```go
   indexMgr.InvalidateCache(tableID)
   ```

3. **Handle tuple updates atomically**: Use OnUpdate for automatic rollback
   ```go
   err := indexMgr.OnUpdate(ctx, tableID, oldTuple, newTuple)
   ```

4. **Validate RecordID**: Ensure tuples have RecordID before maintenance
   ```go
   if tuple.RecordID == nil {
       return fmt.Errorf("tuple missing RecordID")
   }
   ```

5. **Use appropriate index type**: Hash for equality, BTree for ranges
   ```go
   // For WHERE user_id = ?
   indexType := index.HashIndex

   // For WHERE age > ? OR ORDER BY age
   indexType := index.BTreeIndex
   ```

## Integration Points

The IndexManager integrates with:
- **Catalog**: Reads index metadata via CatalogReader interface
- **Storage**: Uses heap files and index files for persistence
- **Memory**: Leverages PageStore for page management
- **WAL**: Logs index operations for crash recovery
- **Transactions**: Coordinates with TransactionContext for ACID
- **Execution**: Used by query executors for index scans

## File Organization

```
pkg/indexmanager/
├── index_manager.go        # Main IndexManager and coordination
├── index_cache.go          # Thread-safe index caching
├── index_loader.go         # Load metadata and open files
├── index_lifecycle.go      # Physical file operations
├── index_maintenance.go    # DML operation handlers
├── index_manager_test.go   # Unit tests
└── README.md               # This file
```

## Testing

The package includes comprehensive unit tests in [index_manager_test.go](index_manager_test.go) covering:
- Index creation and population
- Cache operations
- DML maintenance
- Error conditions
- Concurrent access

Run tests with:
```bash
go test ./pkg/indexmanager -v
```

## Future Enhancements

Potential improvements:
- Composite indexes (multi-column)
- Partial indexes (filtered indexes)
- Expression indexes
- Index statistics collection
- Automatic index recommendation
- Index rebuild/reorganization
- Online index creation (non-blocking)
