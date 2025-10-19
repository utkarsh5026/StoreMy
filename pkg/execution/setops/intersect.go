package setops

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Intersect represents an INTERSECT set operation that returns tuples that appear in both inputs.
//
// INTERSECT follows SQL standard semantics:
//   - INTERSECT (without ALL): Returns distinct tuples that exist in both inputs
//   - INTERSECT ALL: Returns tuples preserving duplicates based on minimum count in either input
//
// Example:
//
//	Left:  {1, 2, 2, 3}
//	Right: {2, 3, 3, 4}
//	INTERSECT:     {2, 3}    (distinct tuples in both)
//	INTERSECT ALL: {2, 3}    (min count: 2 appears once in right, 3 appears once to match left)
type Intersect struct {
	*SetOp
}

// NewIntersect creates a new Intersect operator.
//
// Parameters:
//   - left: The left input iterator
//   - right: The right input iterator
//   - intersectAll: If true, preserves duplicates (INTERSECT ALL); if false, returns distinct tuples
func NewIntersect(left, right iterator.DbIterator, intersectAll bool) (*Intersect, error) {
	base, err := NewSetOperationBase(left, right, SetIntersect, intersectAll)
	if err != nil {
		return nil, err
	}

	in := &Intersect{SetOp: base}
	in.base = query.NewBaseIterator(in.readNext)
	return in, nil
}

// readNext implements the INTERSECT logic using hash-based matching.
//
// Algorithm:
//  1. Build hash set from entire right input with reference counts (lazy initialization)
//  2. Scan left input tuple-by-tuple
//  3. Skip tuples not found in right set
//  4. For INTERSECT ALL:
//     - Check if right count > 0 (matches remaining)
//     - Decrement count and return tuple
//  5. For INTERSECT:
//     - Check if right count > 0 (not yet output)
//     - Remove from right set to prevent duplicate output
//     - Return tuple
func (in *Intersect) readNext() (*tuple.Tuple, error) {
	if err := in.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := in.leftChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if !in.tracker.ContainsInRight(t) {
			continue
		}

		if in.preserveAll {
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.DecrementRight(t)
		} else {
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.rightSet.Remove(t)
		}
		return t, nil
	}
}
