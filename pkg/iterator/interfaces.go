package iterator

import "storemy/pkg/tuple"

// DbIterator defines the contract for all database iterators in the execution engine.
// It provides a standardized interface for traversing through collections of tuples
// from various data sources such as tables, indexes, or intermediate query results.
//
// DbIterator extends TupleIterator with additional lifecycle and schema methods.
type DbIterator interface {
	TupleIterator // Embeds HasNext() and Next()

	// Open initializes the iterator and prepares it for tuple retrieval.
	// This method must be called before any other iterator operations.
	// Multiple calls to Open() on an already opened iterator should be idempotent.
	Open() error

	// Rewind resets the iterator position to the beginning of the data sequence.
	// After rewinding, the next call to Next() should return the first tuple again.
	// The iterator must be opened before calling this method.
	Rewind() error

	// Close releases all resources associated with the iterator and marks it as closed.
	// After closing, the iterator cannot be used until reopened with Open().
	// Calling Close() on an already closed iterator should be safe and idempotent.
	Close() error

	// GetTupleDesc returns the schema description for tuples produced by this iterator.
	// The tuple description defines the structure, types, and metadata of the tuples
	// that will be returned by Next(). This method can be called regardless of iterator state.
	GetTupleDesc() *tuple.TupleDescription
}

// DbFileIterator defines the interface for iterating over tuples in a database file.
// This is a lower-level interface used by storage layer implementations like HeapFile.
//
// DbFileIterator extends TupleIterator with lifecycle methods but does not include
// schema information (GetTupleDesc), as that is managed at a higher level.
type DbFileIterator interface {
	TupleIterator // Embeds HasNext() and Next()

	// Open prepares the iterator for use by initializing internal state and resources.
	// This method must be called before any other iterator operations.
	// Returns an error if the iterator cannot be initialized.
	Open() error

	// Rewind resets the iterator to the beginning of the tuple sequence.
	// After calling Rewind(), the iterator behaves as if it was just opened.
	// Returns an error if the rewind operation fails.
	Rewind() error

	// Close releases any resources held by the iterator and marks it as closed.
	// After calling Close(), the iterator should not be used until Open() is called again.
	// Returns an error if cleanup fails.
	Close() error
}

// TupleIterator is a minimal interface that captures the common iteration methods
// shared by both DbIterator and DbFileIterator. This allows writing generic
// utility functions that work with any iterator type.
type TupleIterator interface {
	// HasNext checks if there are more tuples available without consuming them.
	HasNext() (bool, error)

	// Next retrieves and returns the next tuple from the iterator.
	Next() (*tuple.Tuple, error)
}
