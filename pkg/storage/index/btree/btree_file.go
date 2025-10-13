package btree

import (
	"fmt"
	"io"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
	"sync"
)

// BTreeFile represents a persistent B+Tree index file
type BTreeFile struct {
	*page.BaseFile
	indexID  int
	keyType  types.Type
	numPages int
	mutex    sync.RWMutex
}

// NewBTreeFile creates or opens a B+Tree index file
func NewBTreeFile(filename string, keyType types.Type) (*BTreeFile, error) {
	baseFile, err := page.NewBaseFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create base file: %w", err)
	}

	numPages, err := baseFile.NumPages()
	if err != nil {
		return nil, fmt.Errorf("failed to get num pages: %w", err)
	}

	bf := &BTreeFile{
		BaseFile: baseFile,
		indexID:  0, // Will be set by BTree
		keyType:  keyType,
		numPages: numPages,
	}

	return bf, nil
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

	pageData, err := bf.ReadPageData(pageID.PageNo())
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
	pageData := p.GetPageData()

	if err := bf.WritePageData(pageID.PageNo(), pageData); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

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
