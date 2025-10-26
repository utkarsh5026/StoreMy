package page

import (
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// PageIO defines the minimal interface for reading and writing pages.
// This is the interface used by buffer pools (like PageStore) that only need
// to perform I/O operations without managing file lifecycle.
//
// By separating I/O operations from lifecycle management (Open/Close),
// this interface ensures that buffer pools cannot accidentally close files
// they don't own, preventing resource management bugs.
type PageIO interface {
	// ReadPage retrieves a specific page from the database file by its page ID.
	// The page contains multiple tuples and metadata about the stored data.
	// Returns the requested page or an error if the page cannot be read.
	ReadPage(pid primitives.PageID) (Page, error)

	// WritePage persists a page to the database file.
	// The page will be written to its designated location based on its page ID.
	// Returns an error if the write operation fails.
	WritePage(p Page) error
}

// DbFile represents a database file that stores tuples and provides operations for
// reading, writing, and managing data pages. It serves as the primary interface
// for file-based storage operations in the database system.
//
// DbFile extends PageIO with additional methods for lifecycle management and
// metadata access. File owners (CatalogManager, IndexManager) use this full
// interface, while buffer pools use only the PageIO subset.
type DbFile interface {
	PageIO // Embeds PageIO for read/write operations (ReadPage, WritePage)

	// GetID returns the unique identifier of the database file.
	GetID() int

	// GetTupleDesc returns the tuple description associated with the database file.
	// The tuple description defines the schema and structure of the tuples stored in the file.
	GetTupleDesc() *tuple.TupleDescription

	// Close releases any resources held by the database file and prepares it for garbage collection.
	// It is important to close the file to ensure all changes are flushed and resources are released.
	Close() error
}
