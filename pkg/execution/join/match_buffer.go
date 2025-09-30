package join

import "storemy/pkg/tuple"

// JoinMatchBuffer manages a buffer of matched tuples for join operations.
// This provides a common abstraction for buffering join results that need
// to be returned across multiple Next() calls.
type JoinMatchBuffer struct {
	buffer []*tuple.Tuple
	index  int
}

// NewJoinMatchBuffer creates a new empty match buffer.
func NewJoinMatchBuffer() *JoinMatchBuffer {
	return &JoinMatchBuffer{
		index: -1,
	}
}

// HasNext returns true if there are more buffered results to return.
func (jmb *JoinMatchBuffer) HasNext() bool {
	return jmb.index >= 0 && jmb.index < len(jmb.buffer)
}

// Next returns the next buffered tuple and advances the index.
// Should only be called when HasNext() returns true.
func (jmb *JoinMatchBuffer) Next() *tuple.Tuple {
	if !jmb.HasNext() {
		return nil
	}
	result := jmb.buffer[jmb.index]
	jmb.index++
	return result
}

// Reset clears the buffer and resets the index.
func (jmb *JoinMatchBuffer) Reset() {
	jmb.buffer = nil
	jmb.index = -1
}

// SetMatches sets the buffer to the provided slice and resets index to start.
// Returns the first tuple if any matches exist, nil otherwise.
func (jmb *JoinMatchBuffer) SetMatches(matches []*tuple.Tuple) *tuple.Tuple {
	if len(matches) == 0 {
		jmb.Reset()
		return nil
	}
	jmb.buffer = matches
	jmb.index = 1 // First tuple will be returned immediately, next call starts at 1
	return matches[0]
}

// StartNew initializes a new empty buffer for accumulating matches.
func (jmb *JoinMatchBuffer) StartNew() {
	jmb.buffer = make([]*tuple.Tuple, 0)
	jmb.index = 0
}

// Add appends a tuple to the buffer.
func (jmb *JoinMatchBuffer) Add(t *tuple.Tuple) {
	jmb.buffer = append(jmb.buffer, t)
}

// Len returns the number of tuples in the buffer.
func (jmb *JoinMatchBuffer) Len() int {
	return len(jmb.buffer)
}

// GetFirstAndAdvance returns the first tuple and sets up iteration for the rest.
// Returns nil if buffer is empty.
func (jmb *JoinMatchBuffer) GetFirstAndAdvance() *tuple.Tuple {
	if len(jmb.buffer) == 0 {
		return nil
	}
	jmb.index = 1
	return jmb.buffer[0]
}
