package setops

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

type Union struct {
	*SetOp
}

func NewUnion(left, right iterator.DbIterator, unionAll bool) (*Union, error) {
	base, err := NewSetOperationBase(left, right, SetUnion, unionAll)
	if err != nil {
		return nil, err
	}

	u := &Union{SetOp: base}
	u.base = query.NewBaseIterator(u.readNext)
	return u, nil
}

// readNext implements the union logic.
func (u *Union) readNext() (*tuple.Tuple, error) {
	for !u.leftDone {
		t, err := u.leftChild.FetchNext()
		if err != nil {
			return nil, err
		}

		if t == nil {
			u.leftDone = true
			break
		}

		if !u.preserveAll && !u.tracker.MarkSeen(t) {
			continue // Already seen, skip it
		}
		return t, nil
	}

	// Then, process tuples from the right child
	for {
		t, err := u.rightChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if !u.preserveAll {
			// For UNION (not UNION ALL), check if we've already seen this tuple
			if !u.tracker.MarkSeen(t) {
				continue // Already seen, skip it
			}
		}

		return t, nil
	}
}
