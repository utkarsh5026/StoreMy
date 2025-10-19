package setops

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Intersect represents an INTERSECT operator that returns only tuples that appear in both inputs.
type Intersect struct {
	*SetOp
}

// NewIntersect creates a new Intersect operator.
func NewIntersect(left, right iterator.DbIterator, intersectAll bool) (*Intersect, error) {
	base, err := NewSetOperationBase(left, right, SetIntersect, intersectAll)
	if err != nil {
		return nil, err
	}

	in := &Intersect{SetOp: base}
	in.base = query.NewBaseIterator(in.readNext)
	return in, nil
}

// readNext implements the intersect logic.
func (in *Intersect) readNext() (*tuple.Tuple, error) {
	if err := in.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := in.leftChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		// Check if tuple exists in right set
		if !in.tracker.ContainsInRight(t) {
			continue // Not in right set, skip
		}

		if in.preserveAll {
			// For INTERSECT ALL, decrement count and check if we can still return it
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.DecrementRight(t)
		} else {
			// For INTERSECT, mark as used to avoid duplicates
			count := in.tracker.GetRightCount(t)
			if count <= 0 {
				continue
			}
			in.tracker.rightSet.Remove(t)
		}

		return t, nil
	}
}
