package iterator

import "fmt"

// SliceIterator provides a generic iterator over a slice of any type T.
// This encapsulates the common pattern of iterating through materialized data
// stored in a slice, eliminating duplicate slice+index logic across operators.
//
// Common use cases:
//   - Sort operator: materializes and sorts tuples, then iterates through them
//   - Aggregation operators: materializes grouped results, then iterates
//   - Any operator that buffers data in memory before streaming it out
//
// Example usage:
//
//	// Create iterator with data
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
//
//	// Or iterate using ForEach
//	err := iter.ForEach(func(val int) error {
//	    return process(val)
//	})
type SliceIterator[T any] struct {
	data         []T  // The underlying slice to iterate over
	currentIndex int  // Current position in the slice
	opened       bool // Whether the iterator has been opened
}

// NewSliceIterator creates a new iterator over the given slice.
// The iterator starts in an opened state and positioned at the beginning.
//
// Parameters:
//   - data: Slice to iterate over (can be nil or empty)
//
// Returns:
//   - *SliceIterator ready to use
func NewSliceIterator[T any](data []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data:         data,
		currentIndex: 0,
		opened:       true,
	}
}

// NewClosedSliceIterator creates a new iterator in closed state.
// The iterator must be explicitly opened before use.
// This is useful when you want to control when iteration starts.
func NewClosedSliceIterator[T any](data []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		data:         data,
		currentIndex: 0,
		opened:       false,
	}
}

// HasNext checks if there are more elements available.
// Returns true if there is at least one more element to consume.
func (it *SliceIterator[T]) HasNext() bool {
	if !it.opened {
		return false
	}
	return it.currentIndex < len(it.data)
}

// Next returns the next element from the slice and advances the position.
// Returns an error if the iterator is not opened or if there are no more elements.
func (it *SliceIterator[T]) Next() (T, error) {
	var zero T

	if !it.opened {
		return zero, fmt.Errorf("slice iterator not opened")
	}

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	element := it.data[it.currentIndex]
	it.currentIndex++
	return element, nil
}

// Peek returns the next element without advancing the position.
// Returns an error if the iterator is not opened or if there are no more elements.
func (it *SliceIterator[T]) Peek() (T, error) {
	var zero T

	if !it.opened {
		return zero, fmt.Errorf("slice iterator not opened")
	}

	if it.currentIndex >= len(it.data) {
		return zero, fmt.Errorf("no more elements in slice iterator")
	}

	return it.data[it.currentIndex], nil
}

// Rewind resets the iterator position to the beginning of the slice.
// Does not modify the underlying data, just resets the read position.
func (it *SliceIterator[T]) Rewind() error {
	if !it.opened {
		return fmt.Errorf("slice iterator not opened")
	}

	it.currentIndex = 0
	return nil
}

// Reset replaces the underlying slice with new data and rewinds to the beginning.
// Useful for reusing the same iterator with different data.
func (it *SliceIterator[T]) Reset(data []T) {
	it.data = data
	it.currentIndex = 0
}

// Open marks the iterator as opened and ready for use.
// Resets the position to the beginning.
func (it *SliceIterator[T]) Open() {
	it.opened = true
	it.currentIndex = 0
}

// Close marks the iterator as closed.
// The underlying slice is NOT cleared (use Clear() for that).
func (it *SliceIterator[T]) Close() {
	it.opened = false
}

// Clear releases the underlying slice and resets the iterator.
// This allows the garbage collector to reclaim memory.
func (it *SliceIterator[T]) Clear() {
	it.data = nil
	it.currentIndex = 0
	it.opened = false
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
