package iterator

import "storemy/pkg/tuple"

// DbFileIterator defines the interface for iterating over tuples in a database file.
type DbFileIterator interface {
	// Open prepares the iterator for use by initializing internal state and resources.
	// This method must be called before any other iterator operations.
	// Returns an error if the iterator cannot be initialized.
	Open() error

	// HasNext returns true if there are more tuples available in the iteration sequence.
	// This method should be called before Next() to check if more data is available.
	// Returns a boolean indicating availability and an error if the check fails.
	HasNext() (bool, error)

	// Next returns the next tuple in the iteration sequence.
	// Should only be called after HasNext() returns true.
	// Returns the next tuple or an error if no more tuples are available or if an error occurs.
	Next() (*tuple.Tuple, error)

	// Rewind resets the iterator to the beginning of the tuple sequence.
	// After calling Rewind(), the iterator behaves as if it was just opened.
	// Returns an error if the rewind operation fails.
	Rewind() error

	// Close releases any resources held by the iterator and marks it as closed.
	// After calling Close(), the iterator should not be used until Open() is called again.
	// Returns an error if cleanup fails.
	Close() error
}
