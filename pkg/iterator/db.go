package iterator

import (
	"storemy/pkg/tuple"
)

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
