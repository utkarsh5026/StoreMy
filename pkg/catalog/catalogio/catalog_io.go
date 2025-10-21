package catalogio

import (
	"fmt"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
)

// CatalogIO provides concrete implementations of catalog I/O operations.
// It encapsulates all the low-level I/O logic for interacting with catalog tables,
// including MVCC-aware iteration, tuple insertion, and deletion.
//
// Design Philosophy:
//   - Pure I/O: Only handles actual disk reads/writes
//   - Cache-dependent: Requires tables to be pre-loaded by SystemCatalog
//   - No metadata logic: File lookups delegated to TableCache
//   - Transaction-aware: All operations respect MVCC and locking
//
// Responsibilities:
//   - IterateTable: Scan pages, apply MVCC, yield visible tuples
//   - InsertRow: Write tuples to pages within transaction
//   - DeleteRow: Mark tuples deleted (MVCC soft delete)
//
// NOT responsible for:
//   - Loading tables from CATALOG_TABLES (that's SystemCatalog's job)
//   - Cache management (that's TableCache's job)
//   - Schema reconstruction (that's SystemCatalog's job)
//
// This type can be used directly or embedded in higher-level catalog structures.
type CatalogIO struct {
	store  *memory.PageStore
	cache  *tablecache.TableCache
	tupMgr *table.TupleManager
}

// NewCatalogIO creates a new CatalogIO instance with the given infrastructure components.
//
// Parameters:
//   - store: PageStore for transaction and page management
//   - cache: TableCache for accessing table files (must be pre-populated)
//
// Returns a fully initialized CatalogIO ready to perform I/O operations.
func NewCatalogIO(store *memory.PageStore, cache *tablecache.TableCache) *CatalogIO {
	return &CatalogIO{
		store:  store,
		cache:  cache,
		tupMgr: table.NewTupleManager(store),
	}
}

// IterateTable implements CatalogReader.IterateTable.
// Scans all tuples in a table and applies a processing function to each.
//
// This is the core primitive for all catalog queries. It handles:
//   - MVCC visibility through the transaction context
//   - Page-level locking via the lock manager
//   - Iterator lifecycle management
//
// Parameters:
//   - tableID: ID of the table to scan
//   - tx: Transaction context for locking and MVCC visibility
//   - processFunc: Function to apply to each tuple. Return error to stop iteration.
//
// Returns an error if the iterator cannot be opened or if processFunc returns an error.
func (cio *CatalogIO) IterateTable(
	tableID int,
	tx *transaction.TransactionContext,
	processFunc func(*tuple.Tuple) error,
) error {
	file, err := cio.cache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	heapFile, ok := file.(*heap.HeapFile)
	if !ok {
		return fmt.Errorf("table %d is not a heap file", tableID)
	}

	iter, err := query.NewSeqScan(tx, tableID, heapFile, cio.store)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}

	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}

// InsertRow implements CatalogWriter.InsertRow.
// Inserts a tuple into a table within a transaction.
//
// This operation:
//   - Acquires the necessary locks via the transaction context
//   - Allocates space on an appropriate page
//   - Writes the tuple with proper MVCC metadata
//
// Parameters:
//   - tableID: ID of the table to insert into
//   - tx: Transaction context for the insertion
//   - tup: Tuple to insert
//
// Returns an error if the table cannot be found or insertion fails.
func (cio *CatalogIO) InsertRow(
	tableID int,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	file, err := cio.cache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file for ID %d: %w", tableID, err)
	}
	return cio.tupMgr.InsertTuple(tx, file, tup)
}

// DeleteRow implements CatalogWriter.DeleteRow.
// Deletes a tuple from a table within a transaction.
//
// This operation:
//   - Acquires necessary locks via the transaction context
//   - Marks the tuple as deleted (MVCC soft delete)
//   - The actual space reclamation happens during vacuum
//
// Parameters:
//   - tableID: ID of the table to delete from
//   - tx: Transaction context for the deletion
//   - tup: Tuple to delete
//
// Returns an error if the table cannot be found or deletion fails.
func (cio *CatalogIO) DeleteRow(
	tableID int,
	tx *transaction.TransactionContext,
	tup *tuple.Tuple,
) error {
	file, err := cio.cache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file for ID %d: %w", tableID, err)
	}
	return cio.tupMgr.DeleteTuple(tx, file, tup)
}

// GetCache returns the underlying TableCache.
// This is useful when higher-level components need direct cache access.
func (cio *CatalogIO) Cache() *tablecache.TableCache {
	return cio.cache
}

// GetStore returns the underlying PageStore.
// This is useful when higher-level components need direct store access.
func (cio *CatalogIO) Store() *memory.PageStore {
	return cio.store
}

// GetTupleManager returns the underlying TupleManager.
// This is useful when higher-level components need direct tuple management.
func (cio *CatalogIO) TupleManager() *table.TupleManager {
	return cio.tupMgr
}
