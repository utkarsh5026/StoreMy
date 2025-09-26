package query

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

type LimitOperator struct {
	child  iterator.DbIterator
	limit  int
	offset int
	count  int
	base   *BaseIterator
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

	return &LimitOperator{
		child:  child,
		limit:  limit,
		offset: offset,
	}, nil
}

func (lo *LimitOperator) Open() error {
	if err := lo.child.Open(); err != nil {
		return err
	}

	for i := 0; i < lo.offset; i++ {
		hasNext, err := lo.child.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}
		_, err = lo.child.Next()
		if err != nil {
			return err
		}
	}

	lo.count = 0
	lo.base = NewBaseIterator(lo.readNext)
	lo.base.MarkOpened()
	return nil
}

func (lo *LimitOperator) readNext() (*tuple.Tuple, error) {
	if lo.count >= lo.limit {
		return nil, nil
	}

	hasNext, err := lo.child.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, nil
	}

	t, err := lo.child.Next()
	if err != nil {
		return nil, err
	}

	lo.count++
	return t, nil
}

func (lo *LimitOperator) Close() error {
	if lo.child != nil {
		lo.child.Close()
	}
	if lo.base != nil {
		return lo.base.Close()
	}
	return nil
}

func (lo *LimitOperator) GetTupleDesc() *tuple.TupleDescription {
	return lo.child.GetTupleDesc()
}

func (lo *LimitOperator) HasNext() (bool, error) {
	if lo.base == nil {
		return false, fmt.Errorf("limit operator not opened")
	}
	return lo.base.HasNext()
}

func (lo *LimitOperator) Next() (*tuple.Tuple, error) {
	if lo.base == nil {
		return nil, fmt.Errorf("limit operator not opened")
	}
	return lo.base.Next()
}

func (lo *LimitOperator) Rewind() error {
	lo.count = 0
	if err := lo.child.Rewind(); err != nil {
		return err
	}

	for i := 0; i < lo.offset; i++ {
		hasNext, err := lo.child.HasNext()
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}
		_, err = lo.child.Next()
		if err != nil {
			return err
		}
	}

	lo.base.ClearCache()
	return nil
}
