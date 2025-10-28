package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

type LimitOperator struct {
	*iterator.UnaryOperator
	limit  int
	offset int
	count  int
}

func NewLimitOperator(child iterator.DbIterator, limit, offset int) (*LimitOperator, error) {
	if child == nil {
		return nil, fmt.Errorf("child operator cannot be nil")
	}

	if limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative")
	}

	if offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}

	lo := &LimitOperator{
		limit:  limit,
		offset: offset,
	}

	unaryOp, err := iterator.NewUnaryOperator(child, lo.readNext)
	if err != nil {
		return nil, err
	}
	lo.UnaryOperator = unaryOp

	return lo, nil
}

func (lo *LimitOperator) Open() error {
	if err := lo.UnaryOperator.Open(); err != nil {
		return err
	}

	// Skip offset tuples
	for i := 0; i < lo.offset; i++ {
		t, err := lo.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}
	}

	lo.count = 0
	return nil
}

func (lo *LimitOperator) readNext() (*tuple.Tuple, error) {
	if lo.count >= lo.limit {
		return nil, nil
	}

	t, err := lo.FetchNext()
	if err != nil || t == nil {
		return t, err
	}

	lo.count++
	return t, nil
}

func (lo *LimitOperator) Rewind() error {
	lo.count = 0

	// Rewind using UnaryOperator's Rewind
	if err := lo.UnaryOperator.Rewind(); err != nil {
		return err
	}

	// Skip offset tuples again
	for i := 0; i < lo.offset; i++ {
		t, err := lo.FetchNext()
		if err != nil {
			return err
		}
		if t == nil {
			break
		}
	}

	return nil
}
