package heap

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// HeapFileIterator provides iteration over all tuples in a HeapFile across all pages.
// It implements the iterator.DbFileIterator interface.
type HeapFileIterator struct {
	heapFile        *HeapFile
	transactionID   *primitives.TransactionID
	currentPageNo   primitives.PageNumber
	currentPageIter *HeapPageIterator
	isOpen          bool
}

// NewHeapFileIterator creates a new iterator for iterating over all tuples in a heap file.
//
// Parameters:
//   - heapFile: The heap file to iterate over
//   - tid: The transaction ID for this iteration
//
// Returns:
//   - *HeapFileIterator: A new heap file iterator
func NewHeapFileIterator(heapFile *HeapFile, tid *primitives.TransactionID) *HeapFileIterator {
	return &HeapFileIterator{
		heapFile:      heapFile,
		transactionID: tid,
		currentPageNo: 0,
		isOpen:        false,
	}
}

// Open prepares the iterator for use by initializing the first page iterator.
// This method must be called before any other iterator operations.
//
// Returns:
//   - error: If the iterator cannot be initialized or the first page cannot be read
func (it *HeapFileIterator) Open() error {
	if it.heapFile == nil {
		return fmt.Errorf("heap file is nil")
	}

	it.currentPageNo = 0
	it.isOpen = true

	// Initialize the first page iterator
	return it.loadPageIterator(it.currentPageNo)
}

// loadPageIterator loads the iterator for the specified page number.
func (it *HeapFileIterator) loadPageIterator(pageNo primitives.PageNumber) error {
	// Check if page exists
	numPages, err := it.heapFile.NumPages()
	if err != nil {
		return fmt.Errorf("failed to get number of pages: %w", err)
	}

	// If we've gone past the last page, we're done
	if pageNo >= numPages {
		it.currentPageIter = nil
		return nil
	}

	// Read the page
	pageID := page.NewPageDescriptor(it.heapFile.GetID(), pageNo)
	pg, err := it.heapFile.ReadPage(pageID)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", pageNo, err)
	}

	// Cast to HeapPage
	heapPage, ok := pg.(*HeapPage)
	if !ok {
		return fmt.Errorf("page is not a HeapPage")
	}

	// Create a new page iterator
	it.currentPageIter = NewHeapPageIterator(heapPage)
	if err := it.currentPageIter.Open(); err != nil {
		return fmt.Errorf("failed to open page iterator: %w", err)
	}

	return nil
}

// HasNext checks if there are more tuples available in the file.
//
// Returns:
//   - bool: True if there are more tuples, false otherwise
//   - error: If an error occurs while checking for more tuples
func (it *HeapFileIterator) HasNext() (bool, error) {
	if !it.isOpen {
		return false, fmt.Errorf("iterator is not open")
	}

	// If no current page iterator, we're at the end
	if it.currentPageIter == nil {
		return false, nil
	}

	// Check if current page has more tuples
	hasNext, err := it.currentPageIter.HasNext()
	if err != nil {
		return false, fmt.Errorf("error checking current page: %w", err)
	}

	if hasNext {
		return true, nil
	}

	// Current page is exhausted, try to move to the next page
	return it.advanceToNextPage()
}

// advanceToNextPage moves to the next page that has tuples.
// Returns true if a page with tuples is found, false if we've reached the end.
func (it *HeapFileIterator) advanceToNextPage() (bool, error) {
	numPages, err := it.heapFile.NumPages()
	if err != nil {
		return false, fmt.Errorf("failed to get number of pages: %w", err)
	}

	// Try loading successive pages until we find one with tuples or reach the end
	for {
		it.currentPageNo++

		// Check if we've gone past the last page
		if it.currentPageNo >= numPages {
			it.currentPageIter = nil
			return false, nil
		}

		// Load the next page iterator
		if err := it.loadPageIterator(it.currentPageNo); err != nil {
			return false, err
		}

		// Check if this page has tuples
		if it.currentPageIter != nil {
			hasNext, err := it.currentPageIter.HasNext()
			if err != nil {
				return false, err
			}
			if hasNext {
				return true, nil
			}
		}
	}
}

// Next retrieves and returns the next tuple from the iterator.
//
// Returns:
//   - *tuple.Tuple: The next tuple in the iteration
//   - error: If there are no more tuples or an error occurs
func (it *HeapFileIterator) Next() (*tuple.Tuple, error) {
	if !it.isOpen {
		return nil, fmt.Errorf("iterator is not open")
	}

	// Check if there are more tuples
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, fmt.Errorf("no more tuples")
	}

	// Get the next tuple from the current page iterator
	if it.currentPageIter == nil {
		return nil, fmt.Errorf("no current page iterator")
	}

	return it.currentPageIter.Next()
}

// Rewind resets the iterator to the beginning of the file.
//
// Returns:
//   - error: If the rewind operation fails
func (it *HeapFileIterator) Rewind() error {
	if !it.isOpen {
		return fmt.Errorf("iterator is not open")
	}

	// Close the current page iterator if it exists
	if it.currentPageIter != nil {
		_ = it.currentPageIter.Close()
	}

	// Reset to the beginning
	it.currentPageNo = 0
	return it.loadPageIterator(it.currentPageNo)
}

// Close releases any resources held by the iterator.
//
// Returns:
//   - error: If cleanup fails
func (it *HeapFileIterator) Close() error {
	if it.currentPageIter != nil {
		if err := it.currentPageIter.Close(); err != nil {
			return err
		}
		it.currentPageIter = nil
	}

	it.isOpen = false
	return nil
}

// Compile-time check to ensure HeapFileIterator implements iterator.DbFileIterator
var _ iterator.DbFileIterator = (*HeapFileIterator)(nil)
