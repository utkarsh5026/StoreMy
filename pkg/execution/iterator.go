package execution

import (
	"fmt"
	"storemy/pkg/tuple"
)

// ReadNextFunc is the function signature for reading the next tuple from an iterator.
// Returns:
//   - *tuple.Tuple: Next tuple from the data source, or nil if no more tuples
//   - error: Error if reading fails, nil on success or end of data
type ReadNextFunc func() (*tuple.Tuple, error)

// BaseIterator implements the caching logic and state management for database iterators.
// It provides a common foundation for all iterator implementations in the execution engine,
// handling tuple caching, open/close state, and delegation to specific read functions.
type BaseIterator struct {
	nextTuple    *tuple.Tuple // Cached next tuple for lookahead operations
	opened       bool         // Flag indicating if the iterator has been opened
	readNextFunc ReadNextFunc // Function to read the next tuple from the underlying source
}

// NewBaseIterator creates a new base iterator with the given readNext function.
// The iterator starts in a closed state and must be opened before use.
//
// Parameters:
//   - readNextFunc: Function that will be called to read tuples from the underlying data source
//
// Returns:
//   - *BaseIterator: New base iterator instance ready to be opened and used
func NewBaseIterator(readNextFunc ReadNextFunc) *BaseIterator {
	return &BaseIterator{
		readNextFunc: readNextFunc,
	}
}

// HasNext checks if there is a next tuple available without consuming it.
// This method implements lookahead by caching the next tuple if not already cached.
//
// Returns:
//   - bool: True if a next tuple is available, false if end of data is reached
//   - error: Error if the iterator is not opened or if reading the next tuple fails
func (it *BaseIterator) HasNext() (bool, error) {
	if !it.opened {
		return false, fmt.Errorf("iterator not opened")
	}

	if it.nextTuple == nil {
		var err error
		it.nextTuple, err = it.readNextFunc()
		if err != nil {
			return false, err
		}
	}
	return it.nextTuple != nil, nil
}

// Next returns the next tuple from the iterator and advances the iterator position.
// If a tuple was previously cached by HasNext(), it returns that tuple and clears the cache.
// Otherwise, it reads the next tuple from the underlying source.
// The iterator must be opened before calling this method.
//
// Returns:
//   - *tuple.Tuple: Next tuple from the iterator
//   - error: Error if the iterator is not opened, no more tuples are available, or reading fails
func (it *BaseIterator) Next() (*tuple.Tuple, error) {
	if !it.opened {
		return nil, fmt.Errorf("iterator not opened")
	}

	if it.nextTuple == nil {
		var err error
		it.nextTuple, err = it.readNextFunc()
		if err != nil {
			return nil, err
		}
		if it.nextTuple == nil {
			return nil, fmt.Errorf("no more tuples")
		}
	}

	result := it.nextTuple
	it.nextTuple = nil
	return result, nil
}

// Close releases resources associated with the iterator and marks it as closed.
// After closing, the iterator cannot be used until it is reopened.
// This method clears any cached tuples and resets the iterator state.
//
// Returns:
//   - error: Always returns nil for the base iterator implementation
func (it *BaseIterator) Close() error {
	it.nextTuple = nil
	it.opened = false
	return nil
}

// MarkOpened marks the iterator as opened and ready for use.
func (it *BaseIterator) MarkOpened() {
	it.opened = true
	it.nextTuple = nil
}
