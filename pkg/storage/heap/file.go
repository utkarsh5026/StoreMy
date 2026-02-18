package heap

import (
	"fmt"
	"io"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// HeapFile represents a collection of pages stored in a single OS file on disk.
// It implements the page.DbFile interface and manages heap pages that store tuples
// in a row-oriented format with bitmap headers.
//
// Storage Layout:
//   - Each page is exactly page.PageSize bytes
//   - Pages are numbered sequentially starting from 0
//   - Page offsets are calculated as: pageNo * page.PageSize
type HeapFile struct {
	*page.BaseFile
	tupleDesc *tuple.TupleDescription // Schema definition for tuples in this file
}

// NewHeapFile creates a new HeapFile backed by the specified file on disk.
// The file will be created if it doesn't exist, or opened for read-write if it does.
func NewHeapFile(filename primitives.Filepath, td *tuple.TupleDescription) (*HeapFile, error) {
	baseFile, err := page.NewBaseFile(filename)
	if err != nil {
		return nil, err
	}

	return &HeapFile{
		BaseFile:  baseFile,
		tupleDesc: td,
	}, nil
}

// GetTupleDesc returns the schema definition for tuples stored in this file.
func (hf *HeapFile) GetTupleDesc() *tuple.TupleDescription {
	return hf.tupleDesc
}

// ReadPage reads the specified page from disk into memory.
// This method performs physical I/O and should typically be called through
// the BufferPool rather than directly.
//
// Parameters:
//   - pageID: The page identifier (must be a page.PageDescriptor)
//
// Returns:
//   - page.Page: The loaded HeapPage with tuple data
//   - error: If pageID is invalid, file is closed, or I/O fails
func (hf *HeapFile) ReadPage(pageID *page.PageDescriptor) (page.Page, error) {
	heapPageID, err := hf.validateAndConvertPageID(pageID)
	if err != nil {
		return nil, err
	}

	pageData, err := hf.ReadPageData(heapPageID.PageNo())
	if err != nil {
		if err == io.EOF {
			return NewHeapPage(heapPageID, make([]byte, page.PageSize), hf.tupleDesc)
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	return NewHeapPage(heapPageID, pageData, hf.tupleDesc)
}

// validateAndConvertPageID validates that a PageID is appropriate for this HeapFile.
//
// Parameters:
//   - pageID: The page identifier to validate
//
// Returns:
//   - page.PageDescriptor: The validated and type-cast page ID
//   - error: If validation fails
func (hf *HeapFile) validateAndConvertPageID(pageID primitives.PageID) (*page.PageDescriptor, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	heapPageID, ok := pageID.(*page.PageDescriptor)
	if !ok {
		return nil, fmt.Errorf("invalid page ID type for HeapFile")
	}

	if heapPageID == nil {
		return nil, fmt.Errorf("page descriptor cannot be nil")
	}

	if heapPageID.FileID() != hf.GetID() {
		return nil, fmt.Errorf("page ID table mismatch")
	}

	return heapPageID, nil
}

// WritePage writes the given page to disk at its designated location.
// This method performs physical I/O and syncs the file to ensure durability.
//
// Parameters:
//   - p: The page to write (must contain a valid HeapPageID)
//
// Returns:
//   - error: If page is nil, file is closed, or I/O fails
func (hf *HeapFile) WritePage(p page.Page) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	return hf.WritePageData(p.GetID().PageNo(), p.GetPageData())
}

// Iterator returns a new iterator for this heap file that iterates over all tuples
// across all pages in the file.
func (hf *HeapFile) Iterator(tid *primitives.TransactionID) *HeapFileIterator {
	return NewHeapFileIterator(hf, tid)
}

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
func (it *HeapFileIterator) Open() error {
	if it.heapFile == nil {
		return fmt.Errorf("heap file is nil")
	}

	it.currentPageNo = 0
	it.isOpen = true
	return it.loadPageIterator(it.currentPageNo)
}

// loadPageIterator loads the iterator for the specified page number.
func (it *HeapFileIterator) loadPageIterator(pageNo primitives.PageNumber) error {
	numPages, err := it.heapFile.NumPages()
	if err != nil {
		return fmt.Errorf("failed to get number of pages: %w", err)
	}

	if pageNo >= numPages {
		it.currentPageIter = nil
		return nil
	}

	pageID := page.NewPageDescriptor(it.heapFile.GetID(), pageNo)
	pg, err := it.heapFile.ReadPage(pageID)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", pageNo, err)
	}

	heapPage, ok := pg.(*HeapPage)
	if !ok {
		return fmt.Errorf("page is not a HeapPage")
	}

	it.currentPageIter = NewHeapPageIterator(heapPage)
	if err := it.currentPageIter.Open(); err != nil {
		return fmt.Errorf("failed to open page iterator: %w", err)
	}
	return nil
}

// HasNext checks if there are more tuples available in the file.
func (it *HeapFileIterator) HasNext() (bool, error) {
	if !it.isOpen {
		return false, fmt.Errorf("iterator is not open")
	}

	if it.currentPageIter == nil {
		return false, nil
	}

	hasNext, err := it.currentPageIter.HasNext()
	if err != nil {
		return false, fmt.Errorf("error checking current page: %w", err)
	}

	if hasNext {
		return true, nil
	}

	return it.advanceToNextPage()
}

// advanceToNextPage moves to the next page that has tuples.
// Returns true if a page with tuples is found, false if we've reached the end.
func (it *HeapFileIterator) advanceToNextPage() (bool, error) {
	numPages, err := it.heapFile.NumPages()
	if err != nil {
		return false, fmt.Errorf("failed to get number of pages: %w", err)
	}

	for {
		it.currentPageNo++

		if it.currentPageNo >= numPages {
			it.currentPageIter = nil
			return false, nil
		}

		if err := it.loadPageIterator(it.currentPageNo); err != nil {
			return false, err
		}

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
func (it *HeapFileIterator) Next() (*tuple.Tuple, error) {
	if !it.isOpen {
		return nil, fmt.Errorf("iterator is not open")
	}

	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, fmt.Errorf("no more tuples")
	}

	if it.currentPageIter == nil {
		return nil, fmt.Errorf("no current page iterator")
	}

	return it.currentPageIter.Next()
}

// Rewind resets the iterator to the beginning of the file.
func (it *HeapFileIterator) Rewind() error {
	if !it.isOpen {
		return fmt.Errorf("iterator is not open")
	}

	if it.currentPageIter != nil {
		_ = it.currentPageIter.Close()
	}

	it.currentPageNo = 0
	return it.loadPageIterator(it.currentPageNo)
}

// Close releases any resources held by the iterator.
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
