package hash

import (
	"fmt"
	"io"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
	"sync"
)

const (
	// DefaultBuckets is the default number of hash buckets when not specified
	DefaultBuckets = 256
)

// HashFile represents a persistent hash index file that provides fast key-based lookups.
// It implements a static hash table with fixed bucket count and overflow chaining.
// Each bucket is stored as a HashPage, with additional overflow pages allocated as needed.
//
// Storage Layout:
//   - Each bucket maps to a primary page number (initially bucket i → page i)
//   - Overflow pages are allocated sequentially as buckets fill up
//   - Pages are cached in memory for performance
type HashFile struct {
	*page.BaseFile
	keyType              types.Type
	numPages, numBuckets int
	mutex                sync.RWMutex
	bucketPageID         map[int]int // Maps bucket number to primary page number
}

// NewHashFile creates or opens a hash index file at the specified path.
// If the file exists, it opens and reads the existing structure.
// If the file doesn't exist, it creates a new hash index file.
//
// Parameters:
//   - filename: Path to the hash index file
//   - keyType: Type of keys stored in the index (IntType, StringType, etc.)
//   - numBuckets: Number of hash buckets (uses DefaultBuckets if <= 0)
//
// Returns:
//   - *HashFile: The opened or created hash file
//   - error: Error if file operations fail or invalid parameters provided
func NewHashFile(filename string, keyType types.Type, numBuckets int) (*HashFile, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	if numBuckets <= 0 {
		numBuckets = DefaultBuckets
	}

	baseFile, err := page.NewBaseFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create base file: %w", err)
	}

	numPages, err := baseFile.NumPages()
	if err != nil {
		baseFile.Close()
		return nil, fmt.Errorf("failed to get page count: %w", err)
	}

	hf := &HashFile{
		BaseFile:     baseFile,
		keyType:      keyType,
		numPages:     numPages,
		numBuckets:   numBuckets,
		bucketPageID: make(map[int]int),
	}

	// Initialize bucket-to-page mapping
	// For now, bucket i is stored in page i (one bucket per page initially)
	for i := 0; i < numBuckets; i++ {
		hf.bucketPageID[i] = i
	}

	return hf, nil
}

// GetKeyType returns the type of keys stored in this index.
// All keys in the index must be of this type.
func (hf *HashFile) GetKeyType() types.Type {
	return hf.keyType
}

// NumPages returns the total number of pages in this file.
// This includes both primary bucket pages and overflow pages.
// Thread-safe with read lock.
func (hf *HashFile) NumPages() int {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numPages
}

// GetNumBuckets returns the number of hash buckets in this index.
// The bucket count is fixed at creation time and determines the hash distribution.
// Thread-safe with read lock.
func (hf *HashFile) GetNumBuckets() int {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numBuckets
}

// ReadPage reads a hash page from disk or returns it from cache.
//
// Parameters:
//   - tid: Transaction ID for concurrency control
//   - pageID: Identifier for the page to read
//
// Returns:
//   - *HashPage: The requested page (from cache or disk)
//   - error: Error if page ID is invalid or I/O fails
func (hf *HashFile) ReadPage(pageID *HashPageID) (*HashPage, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.GetTableID() != hf.GetID() {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	pageData, err := hf.ReadPageData(pageID.PageNo())
	if err != nil {
		if err == io.EOF {
			bucketNum := pageID.PageNo()
			newPage := NewHashPage(pageID, bucketNum, hf.keyType)
			return newPage, nil
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	hashPage, err := DeserializeHashPage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}

	return hashPage, nil
}

// WritePage writes a hash page to disk and updates the cache.
// This is a synchronous write operation that flushes to disk immediately.
//
// Parameters:
//   - p: The hash page to write
//
// Returns:
//   - error: Error if page is nil, write fails, or sync fails
func (hf *HashFile) WritePage(p *HashPage) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	pageID := p.pageID
	pageData := p.GetPageData()

	if err := hf.WritePageData(pageID.PageNo(), pageData); err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}

	hf.mutex.Lock()
	if pageID.PageNo() >= hf.numPages {
		hf.numPages = pageID.PageNo() + 1
	}
	hf.mutex.Unlock()

	return nil
}

// AllocatePage allocates a new overflow page for a bucket.
// Overflow pages are created when a bucket's primary page becomes full.
//
// Parameters:
//   - tid: Transaction ID that will own modifications to this page
//   - bucketNum: Bucket number this overflow page belongs to
//
// Returns:
//   - *HashPage: The newly allocated page
//   - error: Error if write operation fails
func (hf *HashFile) AllocatePage(tid *primitives.TransactionID, bucketNum int) (*HashPage, error) {
	hf.mutex.Lock()
	pageNum := hf.numPages
	hf.numPages++
	hf.mutex.Unlock()

	pageID := NewHashPageID(hf.GetID(), pageNum)
	newPage := NewHashPage(pageID, bucketNum, hf.keyType)
	newPage.MarkDirty(true, tid)

	if err := hf.WritePage(newPage); err != nil {
		return nil, fmt.Errorf("failed to write new page: %w", err)
	}

	return newPage, nil
}

// GetBucketPage returns the primary page for a given bucket number.
// This is the starting point for searching entries in a bucket.
//
// Parameters:
//   - tid: Transaction ID for concurrency control
//   - bucketNum: Bucket number (0 to numBuckets-1)
//
// Returns:
//   - *HashPage: The primary bucket page
//   - error: Error if bucket number is invalid or read fails
func (hf *HashFile) GetBucketPage(tid *primitives.TransactionID, bucketNum int) (*HashPage, error) {
	if bucketNum < 0 || bucketNum >= hf.numBuckets {
		return nil, fmt.Errorf("invalid bucket number: %d", bucketNum)
	}

	pageNum := hf.bucketPageID[bucketNum]
	pageID := NewHashPageID(hf.GetID(), pageNum)
	return hf.ReadPage(pageID)
}

// Iterator returns an iterator over all entries in the hash index.
// The iterator visits all buckets sequentially, including overflow pages.
//
// Parameters:
//   - tid: Transaction ID for the scan operation
//
// Returns:
//   - iterator.DbFileIterator: Iterator that yields all hash entries in file order
func (hf *HashFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return &HashFileIterator{
		file:          hf,
		tid:           tid,
		currentBucket: 0,
		currentPage:   nil,
		currentPos:    0,
	}
}
