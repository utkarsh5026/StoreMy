package hash

import (
	"fmt"
	"io"
	"os"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
	"storemy/pkg/utils"
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
//
// Thread Safety:
// HashFile uses a RWMutex to protect concurrent access to its internal state.
// Read operations (NumPages, GetNumBuckets) acquire read locks, while modifications
// (WritePage, AllocatePage) acquire write locks.
type HashFile struct {
	file                          *os.File
	keyType                       types.Type
	indexID, numPages, numBuckets int
	mutex                         sync.RWMutex
	pageCache                     map[int]*HashPage // Maps page number to cached HashPage
	bucketPageID                  map[int]int       // Maps bucket number to primary page number
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
//
// Example:
//
//	hf, err := NewHashFile("students.idx", types.IntType, 256)
func NewHashFile(filename string, keyType types.Type, numBuckets int) (*HashFile, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	if numBuckets <= 0 {
		numBuckets = DefaultBuckets
	}

	file, err := utils.OpenFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := int(fileInfo.Size() / int64(page.PageSize))

	hf := &HashFile{
		file:         file,
		keyType:      keyType,
		indexID:      utils.HashString(file.Name()),
		numPages:     numPages,
		numBuckets:   numBuckets,
		pageCache:    make(map[int]*HashPage),
		bucketPageID: make(map[int]int),
	}

	// Initialize bucket-to-page mapping
	// For now, bucket i is stored in page i (one bucket per page initially)
	for i := 0; i < numBuckets; i++ {
		hf.bucketPageID[i] = i
	}

	return hf, nil
}

// GetID returns the unique identifier for this index file.
// The ID is computed by hashing the file name and is used to identify
// pages belonging to this index.
func (hf *HashFile) GetID() int {
	return hf.indexID
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
// Uses double-checked locking pattern for efficient cache access.
//
// Parameters:
//   - tid: Transaction ID for concurrency control
//   - pageID: Identifier for the page to read
//
// Returns:
//   - *HashPage: The requested page (from cache or disk)
//   - error: Error if page ID is invalid or I/O fails
//
// Behavior:
//   - First checks cache with read lock for fast path
//   - Acquires write lock if page not in cache
//   - Creates new empty page if file read returns EOF
//   - Deserializes page data and adds to cache
func (hf *HashFile) ReadPage(tid *primitives.TransactionID, pageID *HashPageID) (*HashPage, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.GetTableID() != hf.indexID {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	hf.mutex.RLock()

	if cachedPage, ok := hf.pageCache[pageID.PageNo()]; ok {
		hf.mutex.RUnlock()
		return cachedPage, nil
	}
	hf.mutex.RUnlock()

	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	if cachedPage, ok := hf.pageCache[pageID.PageNo()]; ok {
		return cachedPage, nil
	}

	offset := int64(pageID.PageNo()) * int64(page.PageSize)
	pageData := make([]byte, page.PageSize)

	_, err := hf.file.ReadAt(pageData, offset)
	if err != nil {
		if err == io.EOF {
			bucketNum := pageID.PageNo()
			newPage := NewHashPage(pageID, bucketNum, hf.keyType)
			hf.pageCache[pageID.PageNo()] = newPage
			return newPage, nil
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	hashPage, err := DeserializeHashPage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}

	hf.pageCache[pageID.PageNo()] = hashPage
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
//
// Side Effects:
//   - Updates page cache with the written page
//   - Extends numPages if this is a new page beyond current file size
//   - Calls fsync to ensure durability
func (hf *HashFile) WritePage(p *HashPage) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	pageID := p.pageID
	offset := int64(pageID.PageNo()) * int64(page.PageSize)
	pageData := p.GetPageData()

	if _, err := hf.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	if err := hf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	hf.pageCache[pageID.PageNo()] = p
	if pageID.PageNo() >= hf.numPages {
		hf.numPages = pageID.PageNo() + 1
	}

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
//
// Side Effects:
//   - Increments numPages counter
//   - Writes the new page to disk immediately
//   - Marks page as dirty with the given transaction ID
func (hf *HashFile) AllocatePage(tid *primitives.TransactionID, bucketNum int) (*HashPage, error) {
	hf.mutex.Lock()
	pageNum := hf.numPages
	hf.numPages++
	hf.mutex.Unlock()

	pageID := NewHashPageID(hf.indexID, pageNum)
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
	pageID := NewHashPageID(hf.indexID, pageNum)
	return hf.ReadPage(tid, pageID)
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

// Close closes the underlying file and releases resources.
// After closing, the HashFile should not be used.
//
// Returns:
//   - error: Error if file close fails
//
// Side Effects:
//   - Closes the file handle
//   - Clears the page cache
//   - Sets file pointer to nil
func (hf *HashFile) Close() error {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	if hf.file != nil {
		err := hf.file.Close()
		hf.file = nil
		hf.pageCache = nil
		return err
	}

	return nil
}

// FlushCache writes all dirty pages to disk and clears their dirty flags.
// This ensures all in-memory modifications are persisted.
// Does not clear the cache - pages remain in memory for future access.
//
// Returns:
//   - error: Error if any page write or fsync fails
//
// Use Case:
//   - Called during transaction commit
//   - Called during checkpoint operations
//   - Called before shutdown to ensure durability
func (hf *HashFile) FlushCache() error {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()

	for _, p := range hf.pageCache {
		if p.isDirty {
			offset := int64(p.pageID.PageNo()) * int64(page.PageSize)
			pageData := p.GetPageData()

			if _, err := hf.file.WriteAt(pageData, offset); err != nil {
				return fmt.Errorf("failed to write page %v: %w", p.pageID, err)
			}

			p.isDirty = false
		}
	}

	if err := hf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// ClearCache removes all pages from the in-memory cache.
// This forces subsequent reads to fetch pages from disk.
//
// Warning: Any dirty pages not flushed will lose their modifications.
// Always call FlushCache() before ClearCache() if durability is required.
//
// Use Case:
//   - Memory pressure management
//   - Testing scenarios
//   - After FlushCache() to free memory
func (hf *HashFile) ClearCache() {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	hf.pageCache = make(map[int]*HashPage)
}
