package setops

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Union represents a UNION set operation that combines tuples from left and right inputs.
//
// UNION follows SQL standard semantics:
//   - UNION (without ALL): Returns distinct tuples from both inputs (set union)
//   - UNION ALL: Returns all tuples from both inputs, preserving duplicates
//
// Example:
//
//	Left:  {1, 2, 2, 3}
//	Right: {2, 3, 3, 4}
//	UNION:     {1, 2, 3, 4}     (distinct tuples from both)
//	UNION ALL: {1, 2, 2, 3, 2, 3, 3, 4} (all tuples, duplicates preserved)
//
// Implementation:
//   - Streams left input first, then right input (no hash set building required)
//   - For UNION: Uses seen set to track and eliminate duplicates across both inputs
//   - For UNION ALL: No duplicate elimination, simply concatenates both streams
type Union struct {
	*SetOp
}

// NewUnion creates a new Union operator.
//
// Parameters:
//   - left: The left input iterator
//   - right: The right input iterator
//   - unionAll: If true, preserves duplicates (UNION ALL); if false, returns distinct tuples
//
// Returns an error if:
//   - Left and right schemas are incompatible
//   - Either input iterator is nil
func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	base, err := NewSetOperationBase(left, right, SetUnion, unionAll)
	if err != nil {
		return nil, err
	}

	u := &Union{SetOp: base}
	err = u.setBinaryOperator(left, right, u.readNext)
	return u, err
}

// readNext implements the UNION logic using streaming approach.
//
// Algorithm:
//  1. Stream all tuples from left input first
//     - For UNION: Track seen tuples, skip duplicates
//     - For UNION ALL: Return all tuples as-is
//  2. Once left exhausted, stream tuples from right input
//     - For UNION: Skip tuples already seen from left or right
//     - For UNION ALL: Return all tuples as-is
//
// Returns:
//   - Next tuple from left (if not exhausted) or right input
//   - nil when both inputs are exhausted
//   - Error if iteration fails
func (u *Union) readNext() (*tuple.Tuple, error) {
	for !u.leftDone {
		t, err := u.FetchLeft()
		if err != nil {
			return nil, err
		}

		if t == nil {
			u.leftDone = true
			break
		}

		if !u.preserveAll {
			if !u.tracker.MarkSeen(t) {
				continue
			}
		}
		return t, nil
	}

	for {
		t, err := u.FetchRight()
		if err != nil || t == nil {
			return t, err
		}

		if !u.preserveAll {
			if !u.tracker.MarkSeen(t) {
				continue
			}
		}
		return t, nil
	}
}
