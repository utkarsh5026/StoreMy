package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Filter represents a filtering operator that applies a predicate to each tuple
// from its source operator, only returning tuples that satisfy the predicate condition.
type Filter struct {
	*iterator.UnaryOperator
	predicate *Predicate
}

// NewFilter creates a new Filter operator with the specified predicate and source iterator.
// The Filter will evaluate the predicate against each tuple from the source operator,
// passing through only those tuples that satisfy the condition.
func NewFilter(predicate *Predicate, source iterator.DbIterator) (*Filter, error) {
	if predicate == nil {
		return nil, fmt.Errorf("predicate cannot be nil")
	}

	f := &Filter{
		predicate: predicate,
	}

	unaryOp, err := iterator.NewUnaryOperator(source, f.readNext)
	if err != nil {
		return nil, err
	}
	f.UnaryOperator = unaryOp

	return f, nil
}

// readNext is the internal method that implements the filtering logic.
// It continuously reads tuples from the source operator and evaluates them
// against the predicate until it finds a tuple that satisfies the condition
// or reaches the end of the input stream.
func (f *Filter) readNext() (*tuple.Tuple, error) {
	for {
		t, err := f.FetchNext()
		if err != nil || t == nil {
			return t, err
		}

		passes, err := f.predicate.Filter(t)
		if err != nil {
			return nil, fmt.Errorf("predicate evaluation failed: %v", err)
		}

		if passes {
			return t, nil
		}
	}
}
