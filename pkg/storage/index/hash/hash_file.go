package hash

import (
	"fmt"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
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

// ReadPage reads a hash page from disk and deserializes it.
// This is the DbFile interface implementation - it only handles I/O,
// no caching or page management (that's handled by PageStore).
//
// Parameters:
//   - pid: Page identifier (must be a HashPageID)
//
// Returns:
//   - page.Page: The requested page (as HashPage)
//   - error: Error if page ID is invalid or I/O fails
func (hf *HashFile) ReadPage(pid primitives.PageID) (page.Page, error) {
	pageID, ok := pid.(*HashPageID)
	if !ok {
		return nil, fmt.Errorf("invalid page ID type: expected HashPageID")
	}

	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.GetTableID() != hf.GetID() {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	pageData, err := hf.ReadPageData(pageID.PageNo())
	if err != nil {
		if err == io.EOF {
			// Return a new empty page for unallocated pages
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

// WritePage writes a hash page to disk.
// This is the DbFile interface implementation - it only handles I/O,
// no caching (that's handled by PageStore).
//
// Parameters:
//   - p: The page to write (must be a HashPage)
//
// Returns:
//   - error: Error if page is nil, write fails, or sync fails
func (hf *HashFile) WritePage(p page.Page) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	hashPage, ok := p.(*HashPage)
	if !ok {
		return fmt.Errorf("invalid page type: expected HashPage")
	}

	pageID := hashPage.pageID
	pageData := hashPage.GetPageData()

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

// AllocatePageNum allocates a new page number for overflow pages.
// This is used by HashIndex when creating new overflow pages.
// The actual page creation and management is done by PageStore.
//
// Returns:
//   - int: The newly allocated page number
//
// Thread-safe: Uses mutex to ensure atomic page number allocation.
func (hf *HashFile) AllocatePageNum() int {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	pageNum := hf.numPages
	hf.numPages++
	return pageNum
}

// GetBucketPageNum returns the page number for a given bucket.
// The actual page retrieval should go through PageStore.
//
// Parameters:
//   - bucketNum: Bucket number (0 to numBuckets-1)
//
// Returns:
//   - int: Page number for this bucket
//   - error: Error if bucket number is invalid
func (hf *HashFile) GetBucketPageNum(bucketNum int) (int, error) {
	if bucketNum < 0 || bucketNum >= hf.numBuckets {
		return -1, fmt.Errorf("invalid bucket number: %d", bucketNum)
	}

	hf.mutex.RLock()
	pageNum := hf.bucketPageID[bucketNum]
	hf.mutex.RUnlock()

	return pageNum, nil
}

func (hf *HashFile) GetTupleDesc() *tuple.TupleDescription {
	return nil
}
