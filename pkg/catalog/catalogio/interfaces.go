package catalogio

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
)

// CatalogReader provides read-only I/O access to catalog data.
// This interface abstracts table iteration, allowing operations to work
// with catalogs without tight coupling.
//
// Note: This interface only contains actual I/O operations (reading pages from disk).
// Cache lookups and metadata queries belong at higher layers (SystemCatalog).
type CatalogReader interface {
	// IterateTable scans all tuples in a table and applies a processing function to each.
	// This is MVCC-aware and follows proper concurrency protocols.
	//
	// This is real I/O - it reads pages from disk, applies MVCC visibility rules,
	// and yields visible tuples to the processing function.
	//
	// Parameters:
	//   - tableID: ID of the table to scan
	//   - tx: Transaction context for locking and MVCC visibility
	//   - processFunc: Function to apply to each tuple. Return error to stop iteration.
	//
	// Returns an error if the iterator cannot be opened or if processFunc returns an error.
	IterateTable(
		tableID int,
		tx *transaction.TransactionContext,
		processFunc func(*tuple.Tuple) error,
	) error
}

// CatalogWriter provides write access to catalog data.
// This interface abstracts tuple insertion and deletion,
// handling file lookups and transaction coordination.
type CatalogWriter interface {
	// InsertRow inserts a tuple into a table within a transaction.
	//
	// Parameters:
	//   - tableID: ID of the table to insert into
	//   - tx: Transaction context for the insertion
	//   - tup: Tuple to insert
	//
	// Returns an error if the table cannot be found or insertion fails.
	InsertRow(
		tableID int,
		tx *transaction.TransactionContext,
		tup *tuple.Tuple,
	) error

	// DeleteRow deletes a tuple from a table within a transaction.
	//
	// Parameters:
	//   - tableID: ID of the table to delete from
	//   - tx: Transaction context for the deletion
	//   - tup: Tuple to delete
	//
	// Returns an error if the table cannot be found or deletion fails.
	DeleteRow(
		tableID int,
		tx *transaction.TransactionContext,
		tup *tuple.Tuple,
	) error
}

// CatalogAccess combines read and write access for system table implementations.
// This is the primary interface that domain operations depend on.
type CatalogAccess interface {
	CatalogReader
	CatalogWriter
}
