package btree

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
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

// ReadBTreePage reads a B+Tree page from disk
func (bf *BTreeFile) ReadBTreePage(pageID *BTreePageID) (*BTreePage, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.GetTableID() != bf.indexID {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	pageData, err := bf.ReadPageData(pageID.PageNo())
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d (numPages=%d): %w", pageID.PageNo(), bf.NumPages(), err)
	}

	btreePage, err := DeserializeBTreePage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}
	return btreePage, nil
}

// ReadPage implements the DbFile interface by accepting a generic PageID
// and delegating to ReadBTreePage
func (bf *BTreeFile) ReadPage(pid primitives.PageID) (page.Page, error) {
	btreePageID, ok := pid.(*BTreePageID)
	if !ok {
		return nil, fmt.Errorf("page ID must be a BTreePageID, got %T", pid)
	}
	return bf.ReadBTreePage(btreePageID)
}

// WriteBTreePage writes a B+Tree page to disk
func (bf *BTreeFile) WriteBTreePage(p *BTreePage) error {
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

// WritePage implements the DbFile interface by accepting a generic Page
// and delegating to WriteBTreePage
func (bf *BTreeFile) WritePage(p page.Page) error {
	btreePage, ok := p.(*BTreePage)
	if !ok {
		return fmt.Errorf("page must be a BTreePage, got %T", p)
	}
	return bf.WriteBTreePage(btreePage)
}

// AllocatePage allocates a new page in the file
// Note: The page is marked dirty but NOT written to disk immediately.
// The caller is responsible for adding it to the PageStore's dirty page tracking.
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

	return newPage, nil
}

// SetIndexID sets the index ID for this BTree file
// This should be called when the file is associated with a specific index
func (bf *BTreeFile) SetIndexID(indexID int) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	bf.indexID = indexID
}

// GetIndexID returns the index ID for this BTree file
func (bf *BTreeFile) GetIndexID() int {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()
	return bf.indexID
}

// GetID implements the DbFile interface by returning the index ID
func (bf *BTreeFile) GetID() int {
	return bf.GetIndexID()
}

func (bf *BTreeFile) GetTupleDesc() *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc([]types.Type{bf.keyType}, []string{"key"})
	return td
}
