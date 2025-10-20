package tuple

import "fmt"

type Iterator struct {
	tuples    []*Tuple
	tupleDesc *TupleDescription
	index     int
	opened    bool
}

func NewIterator(tuples []*Tuple) *Iterator {
	return &Iterator{
		tuples: tuples,
		index:  -1,
		opened: false,
	}
}

func NewIteratorWithDesc(tuples []*Tuple, desc *TupleDescription) *Iterator {
	return &Iterator{
		tuples:    tuples,
		tupleDesc: desc,
		index:     -1,
		opened:    false,
	}
}

func (it *Iterator) Open() error {
	it.opened = true
	it.index = -1
	return nil
}

func (it *Iterator) Close() error {
	it.opened = false
	return nil
}

func (it *Iterator) HasNext() (bool, error) {
	if !it.opened {
		return false, fmt.Errorf("iterator not opened")
	}
	if it.tuples == nil {
		return false, fmt.Errorf("iterator not initialized with tuples")
	}
	return it.index+1 < len(it.tuples), nil
}

func (it *Iterator) Next() (*Tuple, error) {
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, nil
	}

	it.index++
	return it.tuples[it.index], nil
}

func (it *Iterator) Rewind() error {
	if !it.opened {
		return fmt.Errorf("iterator not opened")
	}
	it.index = -1
	return nil
}

func (it *Iterator) GetTupleDesc() *TupleDescription {
	return it.tupleDesc
}
