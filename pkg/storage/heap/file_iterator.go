package heap

import (
	"fmt"
	"storemy/pkg/transaction"
	"storemy/pkg/tuple"
)

// HeapFileIterator provides iteration over all tuples in a HeapFile
type HeapFileIterator struct {
	file        *HeapFile
	tid         *transaction.TransactionID
	currentPage int
	pageIter    *HeapPageIterator
	isOpen      bool
}

// NewHeapFileIterator creates a new iterator for the given HeapFile
func NewHeapFileIterator(file *HeapFile, tid *transaction.TransactionID) *HeapFileIterator {
	return &HeapFileIterator{
		file:        file,
		tid:         tid,
		currentPage: -1,
		isOpen:      false,
	}
}

// Open initializes the iterator
func (it *HeapFileIterator) Open() error {
	it.currentPage = -1
	it.pageIter = nil
	it.isOpen = true
	return it.moveToNextPage()
}

// HasNext returns true if there are more tuples
func (it *HeapFileIterator) HasNext() (bool, error) {
	if !it.isOpen {
		return false, fmt.Errorf("iterator not opened")
	}

	if it.pageIter != nil {
		hasNext, err := it.pageIter.HasNext()
		if err != nil {
			return false, err
		}
		if hasNext {
			return true, nil
		}
	}

	numPages, err := it.file.NumPages()
	if err != nil {
		return false, err
	}

	return it.currentPage+1 < numPages, nil
}

// Next returns the next tuple
func (it *HeapFileIterator) Next() (*tuple.Tuple, error) {
	if !it.isOpen {
		return nil, fmt.Errorf("iterator not opened")
	}

	if it.pageIter != nil {
		hasNext, err := it.pageIter.HasNext()
		if err != nil {
			return nil, err
		}
		if hasNext {
			return it.pageIter.Next()
		}
	}

	// Move to next page
	err := it.moveToNextPage()
	if err != nil {
		return nil, err
	}

	if it.pageIter == nil {
		return nil, fmt.Errorf("no more tuples")
	}

	return it.pageIter.Next()
}

// Rewind resets the iterator
func (it *HeapFileIterator) Rewind() error {
	return it.Open()
}

// Close releases iterator resources
func (it *HeapFileIterator) Close() error {
	if it.pageIter != nil {
		it.pageIter.Close()
		it.pageIter = nil
	}
	it.isOpen = false
	return nil
}

// moveToNextPage advances to the next page with tuples
func (it *HeapFileIterator) moveToNextPage() error {
	numPages, err := it.file.NumPages()
	if err != nil {
		return err
	}

	for {
		it.currentPage++
		if it.currentPage >= numPages {
			it.pageIter = nil
			return nil
		}

		pageID := NewHeapPageID(it.file.GetID(), it.currentPage)
		page, err := it.file.ReadPage(pageID)
		if err != nil {
			continue // Skip pages that can't be read
		}

		heapPage, ok := page.(*HeapPage)
		if !ok {
			continue // Skip non-heap pages
		}

		it.pageIter = NewHeapPageIterator(heapPage)
		err = it.pageIter.Open()
		if err != nil {
			continue
		}

		hasNext, err := it.pageIter.HasNext()
		if err != nil {
			continue
		}
		if hasNext {
			return nil // Found a page with tuples
		}
	}
}
