package abstract

import "StoreMy/src/pkg/fields"

// DatabaseIterator is the iterator interface that all database operators should implement.
// If the iterator is not open, none of the methods should work and should throw an error.
// In addition to any resource allocation/deallocation, an Open method should call any
// child iterator open methods, and in a Close method, an iterator should call its children's close methods.
type DatabaseIterator interface {
	// Open opens the iterator. This must be called before any of the other methods.
	// Returns an error if there are problems opening/accessing the database.
	Open() error

	// HasNext returns true if the iterator has more tuples.
	// Returns an error if the iterator has not been opened.
	HasNext() (bool, error)

	// Next returns the next tuple from the operator (typically implemented by reading
	// from a child operator or an access method).
	// Returns the next tuple in the iteration.
	// Returns an error if there are no more tuples or if the iterator has not been opened.
	Next() (*fields.Tuple, error)

	// TupleDesc returns the TupleDescription associated with this DatabaseIterator.
	// Returns the TupleDescription associated with this DatabaseIterator.
	TupleDesc() *fields.TupleDescription

	// Close closes the iterator. When the iterator is closed, calling Next(),
	// HasNext(), or any other method should fail by returning an error.
	Close() error
}
