package iterator

import "storemy/pkg/tuple"

// DbIterator defines the contract for all database iterators in the execution engine.
// It provides a standardized interface for traversing through collections of tuples
// from various data sources such as tables, indexes, or intermediate query results.
type DbIterator interface {
	// Open initializes the iterator and prepares it for tuple retrieval.
	// This method must be called before any other iterator operations.
	// Multiple calls to Open() on an already opened iterator should be idempotent.
	//
	// Returns:
	//   - error: nil on successful initialization, non-nil if initialization fails
	Open() error

	// HasNext checks if there are more tuples available without consuming them.
	// This method provides lookahead capability and can be called multiple times
	// without advancing the iterator position. The iterator must be opened before calling.
	//
	// Returns:
	//   - bool: true if more tuples are available, false if end of data is reached
	//   - error: nil on success, non-nil if the check fails or iterator is not opened
	HasNext() (bool, error)

	// Next retrieves and returns the next tuple from the iterator, advancing the position.
	// The iterator must be opened and have available tuples before calling this method.
	// Use HasNext() to check availability before calling Next().
	//
	// Returns:
	//   - *tuple.Tuple: the next tuple in the iteration sequence
	//   - error: nil on success, non-nil if no tuples available, iterator not opened, or read fails
	Next() (*tuple.Tuple, error)

	// Rewind resets the iterator position to the beginning of the data sequence.
	// After rewinding, the next call to Next() should return the first tuple again.
	// The iterator must be opened before calling this method.
	//
	// Returns:
	//   - error: nil on successful rewind, non-nil if rewind fails or iterator not opened
	Rewind() error

	// Close releases all resources associated with the iterator and marks it as closed.
	// After closing, the iterator cannot be used until reopened with Open().
	// Calling Close() on an already closed iterator should be safe and idempotent.
	//
	// Returns:
	//   - error: nil on successful cleanup, non-nil if cleanup fails
	Close() error

	// GetTupleDesc returns the schema description for tuples produced by this iterator.
	// The tuple description defines the structure, types, and metadata of the tuples
	// that will be returned by Next(). This method can be called regardless of iterator state.
	//
	// Returns:
	//   - *tuple.TupleDescription: schema description of tuples produced by this iterator
	GetTupleDesc() *tuple.TupleDescription
}

func LoadAllTuples(iter DbIterator) ([]*tuple.Tuple, error) {
	tuples := make([]*tuple.Tuple, 0)

	for {
		hasNext, err := iter.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		t, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if t != nil {
			tuples = append(tuples, t)
		}
	}

	return tuples, nil
}
