package btree

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

// BTreeFile represents a persistent B+Tree index file
type BTreeFile struct {
	file              *os.File
	keyType           types.Type
	indexID, numPages int
	mutex             sync.RWMutex
}

// NewBTreeFile creates or opens a B+Tree index file
func NewBTreeFile(filename string, keyType types.Type) (*BTreeFile, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	file, err := utils.OpenFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Calculate number of pages from file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := int(fileInfo.Size() / int64(page.PageSize))

	bf := &BTreeFile{
		file:     file,
		keyType:  keyType,
		indexID:  utils.HashString(file.Name()),
		numPages: numPages,
	}

	return bf, nil
}

// GetID returns the unique identifier for this index file
func (bf *BTreeFile) GetID() int {
	return bf.indexID
}

// GetKeyType returns the type of keys stored in this index
func (bf *BTreeFile) GetKeyType() types.Type {
	return bf.keyType
}

// NumPages returns the number of pages in this file
func (bf *BTreeFile) NumPages() int {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()
	return bf.numPages
}

// ReadPage reads a B+Tree page from disk
func (bf *BTreeFile) ReadPage(tid *primitives.TransactionID, pageID *BTreePageID) (*BTreePage, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.GetTableID() != bf.indexID {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	offset := int64(pageID.PageNo()) * int64(page.PageSize)
	pageData := make([]byte, page.PageSize)

	_, err := bf.file.ReadAt(pageData, offset)
	if err != nil {
		if err == io.EOF {
			newPage := NewBTreeLeafPage(pageID, bf.keyType, -1)
			return newPage, nil
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	btreePage, err := DeserializeBTreePage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}
	return btreePage, nil
}

// WritePage writes a B+Tree page to disk
func (bf *BTreeFile) WritePage(p *BTreePage) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	pageID := p.pageID
	offset := int64(pageID.PageNo()) * int64(page.PageSize)
	pageData := p.GetPageData()

	if _, err := bf.file.WriteAt(pageData, offset); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	if err := bf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Update cache

	// Update num pages if necessary
	if pageID.PageNo() >= bf.numPages {
		bf.numPages = pageID.PageNo() + 1
	}

	return nil
}

// AllocatePage allocates a new page in the file
func (bf *BTreeFile) AllocatePage(tid *primitives.TransactionID, keyType types.Type, isLeaf bool, parentPage int) (*BTreePage, error) {
	bf.mutex.Lock()
	pageNum := bf.numPages
	bf.numPages++
	bf.mutex.Unlock()

	pageID := NewBTreePageID(bf.indexID, pageNum)

	var newPage *BTreePage
	if isLeaf {
		newPage = NewBTreeLeafPage(pageID, keyType, parentPage)
	} else {
		newPage = NewBTreeInternalPage(pageID, keyType, parentPage)
	}

	newPage.MarkDirty(true, tid)

	if err := bf.WritePage(newPage); err != nil {
		return nil, fmt.Errorf("failed to write new page: %w", err)
	}

	return newPage, nil
}

// Iterator returns an iterator over all entries in the B+Tree
func (bf *BTreeFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return &BTreeFileIterator{
		file:        bf,
		tid:         tid,
		currentPage: 0,
		currentPos:  0,
	}
}

// Close closes the underlying file
func (bf *BTreeFile) Close() error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	if bf.file != nil {
		err := bf.file.Close()
		bf.file = nil
		return err
	}

	return nil
}
