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
	DefaultBuckets = 256
)

// HashFile represents a persistent hash index file
type HashFile struct {
	file                          *os.File
	keyType                       types.Type
	indexID, numPages, numBuckets int
	mutex                         sync.RWMutex
	pageCache                     map[int]*HashPage
	bucketPageID                  map[int]int // Maps bucket number to page number
}

// NewHashFile creates or opens a hash index file
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

// GetID returns the unique identifier for this index file
func (hf *HashFile) GetID() int {
	return hf.indexID
}

// GetKeyType returns the type of keys stored in this index
func (hf *HashFile) GetKeyType() types.Type {
	return hf.keyType
}

// NumPages returns the number of pages in this file
func (hf *HashFile) NumPages() int {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numPages
}

// GetNumBuckets returns the number of buckets
func (hf *HashFile) GetNumBuckets() int {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numBuckets
}

// ReadPage reads a hash page from disk
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
			// Page doesn't exist yet, create new bucket page
			bucketNum := pageID.PageNo() // Simple mapping: page number = bucket number
			newPage := NewHashPage(pageID, bucketNum, hf.keyType)
			hf.pageCache[pageID.PageNo()] = newPage
			return newPage, nil
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	// Deserialize page
	hashPage, err := DeserializeHashPage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}

	// Cache the page
	hf.pageCache[pageID.PageNo()] = hashPage

	return hashPage, nil
}

// WritePage writes a hash page to disk
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

	// Update cache
	hf.pageCache[pageID.PageNo()] = p

	// Update num pages if necessary
	if pageID.PageNo() >= hf.numPages {
		hf.numPages = pageID.PageNo() + 1
	}

	return nil
}

// AllocatePage allocates a new overflow page
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

// GetBucketPage returns the primary page for a given bucket
func (hf *HashFile) GetBucketPage(tid *primitives.TransactionID, bucketNum int) (*HashPage, error) {
	if bucketNum < 0 || bucketNum >= hf.numBuckets {
		return nil, fmt.Errorf("invalid bucket number: %d", bucketNum)
	}

	pageNum := hf.bucketPageID[bucketNum]
	pageID := NewHashPageID(hf.indexID, pageNum)
	return hf.ReadPage(tid, pageID)
}

// Iterator returns an iterator over all entries in the hash index
func (hf *HashFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return &HashFileIterator{
		file:          hf,
		tid:           tid,
		currentBucket: 0,
		currentPage:   nil,
		currentPos:    0,
	}
}

// Close closes the underlying file
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

// FlushCache writes all dirty pages to disk and clears the cache
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

// ClearCache removes all pages from the cache
func (hf *HashFile) ClearCache() {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	hf.pageCache = make(map[int]*HashPage)
}
