package heap

import (
	"fmt"
	"io"
	"os"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/utils"
	"sync"
)

type HeapFile struct {
	file      *os.File
	tupleDesc *tuple.TupleDescription
	mutex     sync.RWMutex
}

// NewHeapFile creates a new HeapFile backed by the specified file
func NewHeapFile(filename string, td *tuple.TupleDescription) (*HeapFile, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	file, err := utils.OpenFile(filename)
	if err != nil {
		return nil, err
	}

	return &HeapFile{
		file:      file,
		tupleDesc: td,
	}, nil
}

// GetID returns a unique identifier for this file based on its absolute path
func (hf *HeapFile) GetID() int {
	if hf.file == nil {
		return 0
	}

	return utils.HashString(hf.file.Name())
}

// GetTupleDesc returns the schema of tuples in this file
func (hf *HeapFile) GetTupleDesc() *tuple.TupleDescription {
	return hf.tupleDesc
}

// NumPages returns the number of pages in this HeapFile
func (hf *HeapFile) NumPages() (int, error) {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numPages()
}

// ReadPage reads the specified page from disk
func (hf *HeapFile) ReadPage(pageID tuple.PageID) (page.Page, error) {
	heapPageID, err := hf.validateAndConvertPageID(pageID)
	if err != nil {
		return nil, err
	}

	hf.mutex.RLock()
	defer hf.mutex.RUnlock()

	if hf.file == nil {
		return nil, fmt.Errorf("file is closed")
	}

	return hf.readPageData(heapPageID)
}

// validateAndConvertPageID validates and converts a PageID to HeapPageID
func (hf *HeapFile) validateAndConvertPageID(pageID tuple.PageID) (*HeapPageID, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	heapPageID, ok := pageID.(*HeapPageID)
	if !ok {
		return nil, fmt.Errorf("invalid page ID type for HeapFile")
	}

	if heapPageID.GetTableID() != hf.GetID() {
		return nil, fmt.Errorf("page ID table mismatch")
	}

	return heapPageID, nil
}

// readPageData reads the actual page data from disk
func (hf *HeapFile) readPageData(heapPageID *HeapPageID) (page.Page, error) {
	offset := int64(heapPageID.PageNo()) * int64(page.PageSize)
	pageData := make([]byte, page.PageSize)

	_, err := hf.file.ReadAt(pageData, offset)
	if err != nil {
		if err == io.EOF {
			return NewHeapPage(heapPageID, make([]byte, page.PageSize), hf.tupleDesc)
		}
		return nil, fmt.Errorf("failed to read page data: %v", err)
	}

	return NewHeapPage(heapPageID, pageData, hf.tupleDesc)
}

// WritePage writes the given page to disk
func (hf *HeapFile) WritePage(p page.Page) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	if hf.file == nil {
		return fmt.Errorf("file is closed")
	}

	return hf.writePageToDisk(p)
}

// writePageToDisk handles the actual writing of page data to disk
func (hf *HeapFile) writePageToDisk(p page.Page) error {
	heapPageID, ok := p.GetID().(*HeapPageID)
	if !ok {
		return fmt.Errorf("invalid page ID type for HeapFile")
	}

	offset := int64(heapPageID.PageNo()) * page.PageSize
	pageData := p.GetPageData()

	if _, err := hf.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %v", err)
	}

	if err := hf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}

	return nil
}

// Close closes the underlying file
func (hf *HeapFile) Close() error {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	if hf.file != nil {
		err := hf.file.Close()
		hf.file = nil
		return err
	}

	return nil
}

func (hf *HeapFile) AddTuple(tid *transaction.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	if t == nil {
		return nil, fmt.Errorf("tuple cannot be nil")
	}
	if hf.tupleDesc != nil && !t.TupleDesc.Equals(hf.tupleDesc) {
		return nil, fmt.Errorf("tuple schema does not match file schema")
	}

	var numPages int
	var err error

	hf.mutex.RLock()
	numPages, err = hf.numPages()
	hf.mutex.RUnlock()

	if err != nil {
		return nil, err
	}

	for i := 0; i < numPages; i++ {
		pageID := NewHeapPageID(hf.GetID(), i)
		p, err := hf.ReadPage(pageID)

		if err != nil {
			continue // Skip pages that can't be read
		}

		heapPage, ok := p.(*HeapPage)
		if !ok {
			continue
		}

		if heapPage.GetNumEmptySlots() > 0 {
			if err := heapPage.AddTuple(t); err == nil {
				heapPage.MarkDirty(true, tid)
				
				// Write the modified page back to disk
				if err := hf.WritePage(heapPage); err != nil {
					return nil, fmt.Errorf("failed to write modified page: %v", err)
				}
				
				return []page.Page{heapPage}, nil
			}
		}
	}

	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	currentNumPages, err := hf.numPages()
	if err != nil {
		return nil, err
	}

	newPageID := NewHeapPageID(hf.GetID(), currentNumPages)
	newPage, err := NewHeapPage(newPageID, make([]byte, page.PageSize), hf.tupleDesc)
	if err != nil {
		return nil, err
	}

	if err := newPage.AddTuple(t); err != nil {
		return nil, err
	}

	// Write the new page directly using internal method (we already hold the lock)
	if hf.file == nil {
		return nil, fmt.Errorf("file is closed")
	}

	offset := int64(newPageID.PageNo()) * page.PageSize
	pageData := newPage.GetPageData()

	if _, err := hf.file.WriteAt(pageData, offset); err != nil {
		return nil, fmt.Errorf("failed to write page data: %v", err)
	}

	if err := hf.file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync file: %v", err)
	}

	return []page.Page{newPage}, nil
}

// DeleteTuple removes a tuple from the file
func (hf *HeapFile) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) (page.Page, error) {
	if err := hf.validateTupleForDeletion(t); err != nil {
		return nil, err
	}

	page, err := hf.ReadPage(t.RecordID.PageID)
	if err != nil {
		return nil, err
	}

	heapPage, ok := page.(*HeapPage)
	if !ok {
		return nil, fmt.Errorf("invalid page type")
	}

	if err := heapPage.DeleteTuple(t); err != nil {
		return nil, err
	}

	heapPage.MarkDirty(true, tid)
	return heapPage, nil
}

// Iterator returns a DbFileIterator for iterating over tuples in this HeapFile
func (hf *HeapFile) Iterator(tid *transaction.TransactionID) iterator.DbFileIterator {
	return NewHeapFileIterator(hf, tid)
}

// numPages is an internal method that assumes the caller holds the necessary lock
func (hf *HeapFile) numPages() (int, error) {
	if hf.file == nil {
		return 0, fmt.Errorf("file is closed")
	}

	stat, err := hf.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file stats: %v", err)
	}

	numPages := int(stat.Size() / int64(page.PageSize))
	if stat.Size()%int64(page.PageSize) != 0 {
		numPages++
	}
	return numPages, nil
}

// validateTupleForDeletion ensures tuple is valid for deletion
func (hf *HeapFile) validateTupleForDeletion(t *tuple.Tuple) error {
	if t == nil {
		return fmt.Errorf("tuple cannot be nil")
	}
	if t.RecordID == nil {
		return fmt.Errorf("tuple has no record ID")
	}
	return nil
}
