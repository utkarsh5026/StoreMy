package iterator

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
