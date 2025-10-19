package setops

import (
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Except represents an EXCEPT operator that returns tuples from left not in right.
type Except struct {
	*SetOp
}

// NewExcept creates a new Except operator.
func NewExcept(left, right iterator.DbIterator, exceptAll bool) (*Except, error) {
	base, err := NewSetOperationBase(left, right, SetExcept, exceptAll)
	if err != nil {
		return nil, err
	}

	ex := &Except{SetOp: base}
	ex.base = query.NewBaseIterator(ex.readNext)
	return ex, nil
}

// readNext implements the except logic.
func (ex *Except) readNext() (*tuple.Tuple, error) {
	if err := ex.buildRightHashSet(); err != nil {
		return nil, err
	}

	for {
		t, err := ex.leftChild.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		if ex.preserveAll {
			count := ex.tracker.GetRightCount(t)
			if count > 0 {
				ex.tracker.DecrementRight(t)
				continue
			}
			return t, nil
		} else {
			existsInRight := ex.tracker.ContainsInRight(t)
			alreadyOutput := ex.tracker.WasSeen(t)

			if existsInRight || alreadyOutput {
				continue
			}

			ex.tracker.MarkSeen(t)
			return t, nil
		}
	}
}
