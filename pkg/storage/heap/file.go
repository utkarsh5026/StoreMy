package heap

import (
	"fmt"
	"io"
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
//
// Parameters:
//   - filename: Path to the heap file on disk (cannot be empty)
//   - td: Schema definition for tuples that will be stored in this file
//
// Returns:
//   - *HeapFile: The initialized heap file
//   - error: If the filename is empty or file cannot be opened
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
//
// Returns:
//   - *tuple.TupleDescription: The schema definition
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
//
// Behavior:
//   - Returns a blank page if reading past EOF
//   - Validates that pageID matches this file's table ID
//   - Acquires a read lock during the operation
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
