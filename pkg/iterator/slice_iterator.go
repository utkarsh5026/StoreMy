package iterator

import "fmt"

// SliceIterator provides a generic iterator over a slice of any type T.
// This encapsulates the common pattern of iterating through materialized data
// stored in a slice, eliminating duplicate slice+index logic across operators.
//
// Design Philosophy:
//   - Simple and lightweight: just wraps a slice with a read position
//   - No lifecycle management: always ready to use after construction
//   - Cheap to create: just create new iterators instead of resetting
//   - Not thread-safe: use separate iterator per goroutine
//
// Common use cases:
//   - Sort operator: materializes and sorts tuples, then iterates through them
//   - Aggregation operators: materializes grouped results, then iterates
//   - Any operator that buffers data in memory before streaming it out
//
// Example usage:
//
//	// Create iterator with data (ready to use immediately)
//	iter := NewSliceIterator([]int{1, 2, 3, 4, 5})
//
//	// Iterate through elements
//	for iter.HasNext() {
//	    val, err := iter.Next()
//	    if err != nil {
//	        return err
//	    }
//	    process(val)
//	}
type SliceIterator[T any] struct {
	data         []T // The underlying slice to iterate over
	currentIndex int // Current position in the slice
}

// NewSliceIterator creates a new iterator over the given slice.
// The iterator is immediately ready to use - no lifecycle management needed.
//
// Parameters:
//   - data: Slice to iterate over (can be nil or empty)
//
// Returns:
//   - *SliceIterator positioned at the beginning, ready to iterate
//
// Usage:
//
//	iter := NewSliceIterator(data)  // Ready to use!
//	for iter.HasNext() {
//	    val, _ := iter.Next()
//	}
func NewSliceIterator[T any](data []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data:         data,
		currentIndex: 0,
	}
}

// HasNext checks if there are more elements available.
// Returns true if there is at least one more element to consume.
func (it *SliceIterator[T]) HasNext() bool {
	return it.currentIndex < len(it.data)
}

// Next returns the next element from the slice and advances the position.
// Returns an error if there are no more elements.
func (it *SliceIterator[T]) Next() (T, error) {
	var zero T

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	element := it.data[it.currentIndex]
	it.currentIndex++
	return element, nil
}

// Peek returns the next element without advancing the position.
// Returns an error if there are no more elements.
func (it *SliceIterator[T]) Peek() (T, error) {
	var zero T

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	return it.data[it.currentIndex], nil
}

// Rewind resets the iterator position to the beginning of the slice.
// Does not modify the underlying data, just resets the read position.
// Useful when you need to iterate over the same data multiple times.
func (it *SliceIterator[T]) Rewind() error {
	it.currentIndex = 0
	return nil
}

// Len returns the total number of elements in the slice.
func (it *SliceIterator[T]) Len() int {
	return len(it.data)
}

// Remaining returns the number of elements left to iterate.
func (it *SliceIterator[T]) Remaining() int {
	if it.currentIndex >= len(it.data) {
		return 0
	}
	return len(it.data) - it.currentIndex
}

// CurrentIndex returns the current position in the slice (0-based).
// This is the index of the next element that will be returned by Next().
func (it *SliceIterator[T]) CurrentIndex() int {
	return it.currentIndex
}

// GetData returns the underlying slice.
// Note: This provides direct access to the internal data structure.
func (it *SliceIterator[T]) GetData() []T {
	return it.data
}
