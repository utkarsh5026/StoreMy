package page

import (
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// DbFile represents a database file that stores tuples and provides operations for
// reading, writing, and managing data pages. It serves as the primary interface
// for file-based storage operations in the database system.
type DbFile interface {
	// ReadPage retrieves a specific page from the database file by its page ID.
	// The page contains multiple tuples and metadata about the stored data.
	// Returns the requested page or an error if the page cannot be read.
	ReadPage(pid primitives.PageID) (Page, error)

	// WritePage persists a page to the database file.
	// The page will be written to its designated location based on its page ID.
	// Returns an error if the write operation fails.
	WritePage(p Page) error

	// GetID returns the unique identifier of the database file.
	GetID() int

	// GetTupleDesc returns the tuple description associated with the database file.
	// The tuple description defines the schema and structure of the tuples stored in the file.
	GetTupleDesc() *tuple.TupleDescription

	// Close releases any resources held by the database file and prepares it for garbage collection.
	// It is important to close the file to ensure all changes are flushed and resources are released.
	Close() error
}
