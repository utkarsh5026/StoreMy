package page

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// DbFile represents a database file that stores tuples and provides operations for
// reading, writing, and managing data pages. It serves as the primary interface
// for file-based storage operations in the database system.
type DbFile interface {
	// ReadPage retrieves a specific page from the database file by its page ID.
	// The page contains multiple tuples and metadata about the stored data.
	// Returns the requested page or an error if the page cannot be read.
	ReadPage(pid tuple.PageID) (Page, error)

	// WritePage persists a page to the database file.
	// The page will be written to its designated location based on its page ID.
	// Returns an error if the write operation fails.
	WritePage(p Page) error

	// AddTuple inserts a new tuple into the database file within the given transaction context.
	// The tuple will be placed on an appropriate page with available space.
	// Returns a slice of pages that were modified during the insertion and an error if the operation fails.
	AddTuple(tid *transaction.TransactionID, t *tuple.Tuple) ([]Page, error)

	// DeleteTuple removes the specified tuple from the database file within the given transaction context.
	// The tuple must exist in the file for the operation to succeed.
	// Returns the page that was modified during deletion and an error if the operation fails.
	DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) (Page, error)

	// Iterator creates and returns a new iterator for traversing all tuples in the database file
	// within the context of the specified transaction. The iterator allows sequential access
	// to each tuple, respecting the visibility and access control of the transaction.
	Iterator(tid *transaction.TransactionID) iterator.DbFileIterator

	// GetID returns the unique identifier of the database file.
	GetID() int

	// GetTupleDesc returns the tuple description associated with the database file.
	// The tuple description defines the schema and structure of the tuples stored in the file.
	GetTupleDesc() *tuple.TupleDescription

	// Close releases any resources held by the database file and prepares it for garbage collection.
	// It is important to close the file to ensure all changes are flushed and resources are released.
	Close() error
}
