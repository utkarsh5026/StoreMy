package tuple

import "fmt"

type Iterator struct {
	tuples []*Tuple
	index  int
}

func NewIterator(tuples []*Tuple) *Iterator {
	return &Iterator{
		tuples: tuples,
		index:  -1,
	}
}

func (it *Iterator) HasNext() (bool, error) {
	if it.tuples == nil {
		return false, fmt.Errorf("iterator not initialized with tuples")
	}
	return it.index+1 < len(it.tuples), nil
}

func (it *Iterator) Next() (*Tuple, error) {
	if hasNext, _ := it.HasNext(); !hasNext {
		return nil, fmt.Errorf("no more tuples")
	}

	it.index++
	return it.tuples[it.index], nil
}
