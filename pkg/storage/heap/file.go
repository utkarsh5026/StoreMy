package heap

import (
	"fmt"
	"io"
	"os"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/utils"
	"sync"
)

// HeapFile represents a collection of pages stored in a single OS file on disk.
// It implements the page.DbFile interface and manages heap pages that store tuples
// in a row-oriented format with bitmap headers.
//
// HeapFile is thread-safe and uses a read-write mutex to protect concurrent access.
// All page reads and writes go through the file system, though actual I/O is typically
// managed by the BufferPool layer above.
//
// Storage Layout:
//   - Each page is exactly page.PageSize bytes
//   - Pages are numbered sequentially starting from 0
//   - Page offsets are calculated as: pageNo * page.PageSize
type HeapFile struct {
	file      *os.File
	tupleDesc *tuple.TupleDescription // Schema definition for tuples in this file
	mutex     sync.RWMutex            // Protects concurrent file access
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

// GetID returns a unique identifier for this file based on its absolute path.
// This ID is used as the table ID throughout the system and must remain stable
// for the lifetime of the file.
//
// The ID is computed by hashing the file's absolute path, which ensures:
//   - Same file always gets the same ID
//   - Different files get different IDs (with high probability)
//
// Returns:
//   - int: A unique hash-based identifier, or 0 if the file is closed
func (hf *HeapFile) GetID() int {
	if hf.file == nil {
		return 0
	}

	return utils.HashString(hf.file.Name())
}

// GetTupleDesc returns the schema definition for tuples stored in this file.
// All tuples in a HeapFile share the same schema, which defines the number,
// types, and names of fields.
//
// Returns:
//   - *tuple.TupleDescription: The schema definition
func (hf *HeapFile) GetTupleDesc() *tuple.TupleDescription {
	return hf.tupleDesc
}

// NumPages returns the number of pages currently in this HeapFile.
// This is computed from the file size and may include partially filled pages.
//
// The page count is calculated as:
//
//	numPages = ceil(fileSize / page.PageSize)
//
// Returns:
//   - int: Number of pages in the file
//   - error: If the file is closed or stats cannot be retrieved
//
// Note: This method acquires a read lock and is safe for concurrent use.
func (hf *HeapFile) NumPages() (int, error) {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numPages()
}

// ReadPage reads the specified page from disk into memory.
// This method performs physical I/O and should typically be called through
// the BufferPool rather than directly.
//
// Parameters:
//   - pageID: The page identifier (must be a *HeapPageID)
//
// Returns:
//   - page.Page: The loaded HeapPage with tuple data
//   - error: If pageID is invalid, file is closed, or I/O fails
//
// Behavior:
//   - Returns a blank page if reading past EOF
//   - Validates that pageID matches this file's table ID
//   - Acquires a read lock during the operation
func (hf *HeapFile) ReadPage(pageID primitives.PageID) (page.Page, error) {
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

// validateAndConvertPageID validates that a PageID is appropriate for this HeapFile.
// Checks include:
//   - PageID is not nil
//   - PageID is of type *HeapPageID
//   - PageID's table ID matches this file's ID
//
// Parameters:
//   - pageID: The page identifier to validate
//
// Returns:
//   - *HeapPageID: The validated and type-cast page ID
//   - error: If validation fails
func (hf *HeapFile) validateAndConvertPageID(pageID primitives.PageID) (*HeapPageID, error) {
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

// readPageData performs the actual disk read operation for a page.
// Uses positioned I/O (ReadAt) to read exactly page.PageSize bytes from
// the calculated offset.
//
// Parameters:
//   - heapPageID: Validated page identifier
//
// Returns:
//   - page.Page: The loaded HeapPage
//   - error: If the read fails (except EOF, which returns a blank page)
//
// Note: This is an internal helper that assumes locks are already held.
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

// WritePage writes the given page to disk at its designated location.
// This method performs physical I/O and syncs the file to ensure durability.
//
// Parameters:
//   - p: The page to write (must contain a valid HeapPageID)
//
// Returns:
//   - error: If page is nil, file is closed, or I/O fails
//
// Behavior:
//   - Calculates offset from page number
//   - Writes exactly page.PageSize bytes
//   - Calls fsync to ensure data is persisted to disk
//   - Acquires a write lock during the operation
//
// Example:
//
//	modifiedPage.MarkDirty()
//	err := heapFile.WritePage(modifiedPage)
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

// writePageToDisk handles the actual disk write operation.
// Uses positioned I/O (WriteAt) to write to the exact page offset,
// followed by fsync for durability.
//
// Parameters:
//   - p: The page to write
//
// Returns:
//   - error: If the page ID is invalid or I/O fails
//
// Note: This is an internal helper that assumes locks are already held.
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

// Close closes the underlying file handle and releases resources.
// After calling Close, the HeapFile should not be used.
//
// Returns:
//   - error: If the file close operation fails
//
// Note: Calling Close multiple times is safe but only the first call
// will actually close the file.
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

// Iterator returns a DbFileIterator for iterating over all tuples in this HeapFile.
// The iterator will scan through pages sequentially and return tuples one at a time.
//
// Parameters:
//   - tid: Transaction ID for locking and concurrency control
//
// Returns:
//   - iterator.DbFileIterator: An iterator over all tuples in the file
func (hf *HeapFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return NewHeapFileIterator(hf, tid)
}

// numPages is an internal helper that computes the page count without locking.
// Callers must hold the appropriate lock before calling this method.
//
// Returns:
//   - int: Number of pages, including partial pages
//   - error: If the file is closed or stats cannot be retrieved
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
