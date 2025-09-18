package heap

import (
	"fmt"
	"storemy/pkg/tuple"
)

// HeapPageIterator provides iteration over tuples in a HeapPage
type HeapPageIterator struct {
	page         *HeapPage
	currentSlot  int
	tuples       []*tuple.Tuple
	currentIndex int
}

// NewHeapPageIterator creates a new iterator for the given page
func NewHeapPageIterator(page *HeapPage) *HeapPageIterator {
	return &HeapPageIterator{
		page:         page,
		currentSlot:  -1,
		currentIndex: -1,
	}
}

// Open initializes the iterator
func (it *HeapPageIterator) Open() error {
	it.tuples = it.page.GetTuples()
	it.currentIndex = -1
	return nil
}

// HasNext returns true if there are more tuples
func (it *HeapPageIterator) HasNext() (bool, error) {
	return it.currentIndex+1 < len(it.tuples), nil
}

// Next returns the next tuple
func (it *HeapPageIterator) Next() (*tuple.Tuple, error) {
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}

	if !hasNext {
		return nil, fmt.Errorf("no more tuples")
	}

	it.currentIndex++
	return it.tuples[it.currentIndex], nil
}

// Rewind resets the iterator
func (it *HeapPageIterator) Rewind() error {
	return it.Open()
}

// Close releases iterator resources
func (it *HeapPageIterator) Close() error {
	it.tuples = nil
	it.currentIndex = -1
	return nil
}
