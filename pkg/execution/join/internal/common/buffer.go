package common

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// JoinMatchBuffer manages a buffer of matched tuples for join operations.
// This provides a common abstraction for buffering join results that need
// to be returned across multiple Next() calls.
//
// Internally wraps SliceIterator to reuse common iteration logic while
// providing join-specific convenience methods.
type JoinMatchBuffer struct {
	iter *iterator.SliceIterator[*tuple.Tuple]
}

// NewJoinMatchBuffer creates a new empty match buffer.
func NewJoinMatchBuffer() *JoinMatchBuffer {
	return &JoinMatchBuffer{
		iter: iterator.NewSliceIterator([]*tuple.Tuple(nil)),
	}
}

// HasNext returns true if there are more buffered results to return.
func (jmb *JoinMatchBuffer) HasNext() bool {
	return jmb.iter.HasNext()
}

// Next returns the next buffered tuple and advances the index.
// Should only be called when HasNext() returns true.
func (jmb *JoinMatchBuffer) Next() *tuple.Tuple {
	result, err := jmb.iter.Next()
	if err != nil {
		return nil
	}
	return result
}

// Reset clears the buffer and resets the index.
func (jmb *JoinMatchBuffer) Reset() {
	jmb.iter = iterator.NewSliceIterator([]*tuple.Tuple(nil))
}

// SetMatches sets the buffer to the provided slice and resets index to start.
// Returns the first tuple if any matches exist, nil otherwise.
func (jmb *JoinMatchBuffer) SetMatches(matches []*tuple.Tuple) *tuple.Tuple {
	if len(matches) == 0 {
		jmb.Reset()
		return nil
	}
	jmb.iter = iterator.NewSliceIterator(matches)
	first, err := jmb.iter.Next()
	if err != nil {
		return nil
	}
	return first
}

// StartNew initializes a new empty buffer for accumulating matches.
func (jmb *JoinMatchBuffer) StartNew() {
	jmb.iter = iterator.NewSliceIterator(make([]*tuple.Tuple, 0))
}

// Add appends a tuple to the buffer.
// Note: Preserves the current iteration position.
func (jmb *JoinMatchBuffer) Add(t *tuple.Tuple) {
	currentData := jmb.iter.GetData()
	currentPos := jmb.iter.CurrentIndex()

	// Append new tuple and create new iterator with updated data
	currentData = append(currentData, t)
	jmb.iter = iterator.NewSliceIterator(currentData)

	// Manually restore the iteration position
	for i := 0; i < currentPos; i++ {
		if _, err := jmb.iter.Next(); err != nil {
			break
		}
	}
}

// Len returns the number of tuples in the buffer.
func (jmb *JoinMatchBuffer) Len() int {
	return jmb.iter.Len()
}

// GetFirstAndAdvance returns the first tuple and sets up iteration for the rest.
// Returns nil if buffer is empty.
func (jmb *JoinMatchBuffer) GetFirstAndAdvance() *tuple.Tuple {
	if jmb.iter.Len() == 0 {
		return nil
	}
	if err := jmb.iter.Rewind(); err != nil {
		return nil
	}
	first, err := jmb.iter.Next()
	if err != nil {
		return nil
	}
	return first
}
