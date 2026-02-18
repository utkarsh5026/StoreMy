package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
)

const (
	// Page type constants
	pageTypeInternal byte = 0x01
	pageTypeLeaf     byte = 0x02

	// Page header size (in bytes)
	// 1 byte: page type
	// 8 bytes: parent page number (uint64)
	// 4 bytes: number of Entries
	// 8 bytes: next leaf (for leaf pages, InvalidPageNumber for internal)
	// 8 bytes: prev leaf (for leaf pages, InvalidPageNumber for internal)
	headerSize = 29

	// Maximum number of Entries per page (conservative estimate)
	// For 4KB pages: (4096 - 29) / (8 + 8 + 4 + 4) ≈ 169 Entries for int keys
	// We'll use a more conservative number to handle string keys
	MaxEntriesPerPage = 150
)

// BTreePage represents a page in a B+Tree index
// It can be either an internal node or a leaf node
type BTreePage struct {
	pageID                         *page.PageDescriptor
	pageType                       byte
	ParentPage, NextLeaf, PrevLeaf primitives.PageNumber // InvalidPageNumber if root/not linked
	keyType                        types.Type
	Entries                        []*IndexEntry    // For leaf pages
	InternalPages                  []*BTreeChildPtr // For internal pages
	isDirty                        bool
	dirtyTxn                       *primitives.TransactionID
	beforeImage                    []byte
}

// BTreeChildPtr represents a child pointer in an internal node
type BTreeChildPtr struct {
	Key      types.Field          // Separator key (minimum key in right child)
	ChildPID *page.PageDescriptor // Child page ID
}

func NewBtreeChildPtr(key types.Field, childPID *page.PageDescriptor) *BTreeChildPtr {
	return &BTreeChildPtr{Key: key, ChildPID: childPID}
}

// NewBTreeLeafPage creates a new leaf page
func NewBTreeLeafPage(pageID *page.PageDescriptor, keyType types.Type, parentPage primitives.PageNumber) *BTreePage {
	return &BTreePage{
		pageID:        pageID,
		pageType:      pageTypeLeaf,
		ParentPage:    parentPage,
		NextLeaf:      primitives.InvalidPageNumber,
		PrevLeaf:      primitives.InvalidPageNumber,
		keyType:       keyType,
		Entries:       make([]*IndexEntry, 0, MaxEntriesPerPage),
		InternalPages: nil,
		isDirty:       false,
	}
}

// NewBTreeInternalPage creates a new internal page
func NewBTreeInternalPage(pageID *page.PageDescriptor, keyType types.Type, parentPage primitives.PageNumber) *BTreePage {
	return &BTreePage{
		pageID:        pageID,
		pageType:      pageTypeInternal,
		ParentPage:    parentPage,
		NextLeaf:      primitives.InvalidPageNumber,
		PrevLeaf:      primitives.InvalidPageNumber,
		keyType:       keyType,
		Entries:       nil,
		InternalPages: make([]*BTreeChildPtr, 0, MaxEntriesPerPage+1),
		isDirty:       false,
	}
}

// GetID returns the page ID (implements page.Page interface)
func (p *BTreePage) GetID() *page.PageDescriptor {
	return p.pageID
}

// IsDirty returns whether this page has been modified
func (p *BTreePage) IsDirty() *primitives.TransactionID {
	if p.isDirty {
		return p.dirtyTxn
	}
	return nil
}

// MarkDirty marks this page as dirty
func (p *BTreePage) MarkDirty(dirty bool, tid *primitives.TransactionID) {
	if dirty && !p.isDirty && p.beforeImage == nil {
		p.beforeImage = p.GetPageData()
	}
	p.isDirty = dirty
	p.dirtyTxn = tid
}

// GetPageData serializes the page to bytes
func (p *BTreePage) GetPageData() []byte {
	buf := new(bytes.Buffer)

	buf.WriteByte(p.pageType)
	_ = binary.Write(buf, binary.BigEndian, uint64(p.ParentPage))
	_ = binary.Write(buf, binary.BigEndian, int32(p.GetNumEntries())) // #nosec G115
	_ = binary.Write(buf, binary.BigEndian, uint64(p.NextLeaf))
	_ = binary.Write(buf, binary.BigEndian, uint64(p.PrevLeaf))

	if p.IsLeafPage() {
		for _, entry := range p.Entries {
			if err := p.serializeEntry(buf, entry); err != nil {
				panic(fmt.Sprintf("failed to serialize entry: %v", err))
			}
		}
	} else {
		for i, child := range p.InternalPages {
			// For internal nodes: child[0] has no key, child[i] (i>0) has key
			if i > 0 {
				if err := p.serializeField(buf, child.Key); err != nil {
					panic(fmt.Sprintf("failed to serialize key: %v", err))
				}
			}
			_ = binary.Write(buf, binary.BigEndian, uint64(child.ChildPID.FileID()))
			_ = binary.Write(buf, binary.BigEndian, uint64(child.ChildPID.PageNo()))
		}
	}

	data := buf.Bytes()
	if len(data) < page.PageSize {
		padding := make([]byte, page.PageSize-len(data))
		data = append(data, padding...)
	}

	return data
}

func (p *BTreePage) Children() []*BTreeChildPtr {
	return p.InternalPages
}

func (p *BTreePage) Parent() primitives.PageNumber {
	return p.ParentPage
}

func (p *BTreePage) PageNo() primitives.PageNumber {
	return p.pageID.PageNo()
}

func (p *BTreePage) InsertEntry(e *IndexEntry, index int) error {
	if index < -1 || index > len(p.Entries) {
		return fmt.Errorf("invalid index %d for inserting element", index)
	}

	if index == -1 {
		index = len(p.Entries)
	}

	p.Entries = slices.Insert(p.Entries, index, e)
	return nil
}

func (p *BTreePage) RemoveEntry(index int) (*IndexEntry, error) {
	if index < -1 || index >= len(p.Entries) {
		return nil, fmt.Errorf("invalid index %d for removing element", index)
	}

	if index == -1 {
		index = len(p.Entries) - 1
	}

	removed := p.Entries[index]
	p.Entries = slices.Delete(p.Entries, index, index+1)
	return removed, nil
}

func (p *BTreePage) AddChildPtr(child *BTreeChildPtr, index int) error {
	if index < 0 || index > len(p.InternalPages) {
		return fmt.Errorf("invalid index %d for inserting child pointer", index)
	}

	p.InternalPages = slices.Insert(p.InternalPages, index, child)
	return nil
}

func (p *BTreePage) RemoveChildPtr(index int) (*BTreeChildPtr, error) {
	if index < 0 || index >= len(p.InternalPages) {
		return nil, fmt.Errorf("invalid index %d for removing child pointer", index)
	}

	removed := p.InternalPages[index]
	p.InternalPages = slices.Delete(p.InternalPages, index, index+1)
	return removed, nil
}

// GetBeforeImage returns the before-image of this page
func (p *BTreePage) GetBeforeImage() page.Page {
	if p.beforeImage == nil {
		return nil
	}

	beforePage, err := DeserializeBTreePage(p.beforeImage, p.pageID)
	if err != nil {
		return nil
	}
	return beforePage
}

// SetBeforeImage sets the before image to the current state
func (p *BTreePage) SetBeforeImage() {
	p.beforeImage = p.GetPageData()
}

func (p *BTreePage) SetParent(id primitives.PageNumber) {
	p.ParentPage = id
}

// IsLeafPage returns true if this is a leaf page
func (p *BTreePage) IsLeafPage() bool {
	return p.pageType == pageTypeLeaf
}

func (p *BTreePage) IsRoot() bool {
	return p.ParentPage == primitives.InvalidPageNumber
}

func (p *BTreePage) HasNextLeaf() bool {
	return p.NextLeaf != primitives.InvalidPageNumber
}

func (p *BTreePage) Leaves() (left, right primitives.PageNumber) {
	return p.PrevLeaf, p.NextLeaf
}

func (p *BTreePage) UpdateChildrenKey(index int, key types.Field) error {
	if index < 1 || index >= len(p.InternalPages) {
		return fmt.Errorf("invalid index %d for updating child key", index)
	}
	p.InternalPages[index].Key = key
	return nil
}

// IsInternalPage returns true if this is an internal page
func (p *BTreePage) IsInternalPage() bool {
	return p.pageType == pageTypeInternal
}

// IsFull returns true if the page cannot accept more Entries
func (p *BTreePage) IsFull() bool {
	return p.GetNumEntries() >= MaxEntriesPerPage
}

// GetNumEntries returns the number of Entries in this page
func (p *BTreePage) GetNumEntries() int {
	if p.Entries != nil {
		return len(p.Entries)
	}
	if p.InternalPages != nil {
		return len(p.InternalPages) - 1
	}
	return 0
}

func (p *BTreePage) HasMoreThanRequired() bool {
	return p.GetNumEntries() > MaxEntriesPerPage/2
}

func (p *BTreePage) HashLessThanRequired() bool {
	return p.GetNumEntries() < MaxEntriesPerPage/2
}

// serializeEntry writes an entry to the buffer
func (p *BTreePage) serializeEntry(w io.Writer, entry *IndexEntry) error {
	if err := p.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	// Write page ID type (kept for compatibility, always 0 now)
	var pageIDType byte = 0
	_ = binary.Write(w, binary.BigEndian, pageIDType)

	_ = binary.Write(w, binary.BigEndian, uint64(rid.PageID.FileID()))
	_ = binary.Write(w, binary.BigEndian, uint64(rid.PageID.PageNo()))
	_ = binary.Write(w, binary.BigEndian, uint16(rid.TupleNum))
	return nil
}

// serializeField writes a field to the buffer
func (p *BTreePage) serializeField(w io.Writer, field types.Field) error {
	if err := binary.Write(w, binary.BigEndian, byte(field.Type())); err != nil { // #nosec G115
		return err
	}
	return field.Serialize(w)
}

// DeserializeBTreePage reads a page from bytes
func DeserializeBTreePage(data []byte, pageID *page.PageDescriptor) (*BTreePage, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	pageType, _ := buf.ReadByte()

	var ParentPage, NextLeaf, PrevLeaf uint64
	var numEntries int32
	_ = binary.Read(buf, binary.BigEndian, &ParentPage)
	_ = binary.Read(buf, binary.BigEndian, &numEntries)
	_ = binary.Read(buf, binary.BigEndian, &NextLeaf)
	_ = binary.Read(buf, binary.BigEndian, &PrevLeaf)

	btreePage := &BTreePage{
		pageID:     pageID,
		pageType:   pageType,
		ParentPage: primitives.PageNumber(ParentPage),
		NextLeaf:   primitives.PageNumber(NextLeaf),
		PrevLeaf:   primitives.PageNumber(PrevLeaf),
		isDirty:    false,
	}

	// Deserialize Entries/InternalPages based on page type
	if pageType == pageTypeLeaf {
		btreePage.Entries = make([]*IndexEntry, 0, numEntries)
		for i := 0; i < int(numEntries); i++ {
			entry, err := deserializeEntry(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
			}
			btreePage.Entries = append(btreePage.Entries, entry)
			if i == 0 && entry.Key != nil {
				btreePage.keyType = entry.Key.Type()
			}
		}
	} else {
		btreePage.InternalPages = make([]*BTreeChildPtr, 0, numEntries+1)
		for i := 0; i <= int(numEntries); i++ {
			var key types.Field
			var err error

			// First child has no key
			if i > 0 {
				key, err = deserializeField(buf)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize key %d: %w", i, err)
				}
				if i == 1 && key != nil {
					btreePage.keyType = key.Type()
				}
			}

			var fileID, pageNum uint64
			_ = binary.Read(buf, binary.BigEndian, &fileID)
			_ = binary.Read(buf, binary.BigEndian, &pageNum)

			childPtr := &BTreeChildPtr{
				Key:      key,
				ChildPID: page.NewPageDescriptor(primitives.FileID(fileID), primitives.PageNumber(pageNum)),
			}
			btreePage.InternalPages = append(btreePage.InternalPages, childPtr)
		}
	}

	return btreePage, nil
}

// deserializeEntry reads an entry from the buffer
func deserializeEntry(r *bytes.Reader) (*IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	// Read page ID type (kept for compatibility but not used anymore)
	var pageIDType byte
	_ = binary.Read(r, binary.BigEndian, &pageIDType)

	var fileID, pageNum uint64
	var tupleNum uint16
	_ = binary.Read(r, binary.BigEndian, &fileID)
	_ = binary.Read(r, binary.BigEndian, &pageNum)
	_ = binary.Read(r, binary.BigEndian, &tupleNum)

	pageID := page.NewPageDescriptor(primitives.FileID(fileID), primitives.PageNumber(pageNum))

	rid := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: primitives.SlotID(tupleNum),
	}

	return &IndexEntry{
		Key: key,
		RID: rid,
	}, nil
}

// BTreeFile represents a persistent B+Tree index file
type BTreeFile struct {
	*page.BaseFile
	indexID  primitives.FileID
	keyType  types.Type
	numPages primitives.PageNumber
	mutex    sync.RWMutex
}

// NewBTreeFile creates or opens a B+Tree index file
func NewBTreeFile(filename primitives.Filepath, keyType types.Type) (*BTreeFile, error) {
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
	return int(bf.numPages) // #nosec G115
}

// ReadBTreePage reads a B+Tree page from disk
func (bf *BTreeFile) ReadBTreePage(pageID *page.PageDescriptor) (*BTreePage, error) {
	if pageID == nil {
		return nil, fmt.Errorf("page ID cannot be nil")
	}

	if pageID.FileID() != bf.indexID {
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

// ReadPage implements the DbFile interface
func (bf *BTreeFile) ReadPage(pageID *page.PageDescriptor) (page.Page, error) {
	return bf.ReadBTreePage(pageID)
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
func (bf *BTreeFile) AllocatePage(tid *primitives.TransactionID, keyType types.Type, isLeaf bool, parentPage primitives.PageNumber) (*BTreePage, error) {
	bf.mutex.Lock()
	pageNum := bf.numPages
	bf.numPages++
	bf.mutex.Unlock()

	pageID := page.NewPageDescriptor(bf.indexID, pageNum)

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
func (bf *BTreeFile) SetIndexID(indexID primitives.FileID) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	bf.indexID = indexID
}

// GetID implements the DbFile interface by returning the index ID if set,
// otherwise returns the BaseFile's ID (hash of filename).
// This allows the file to use its natural ID from BaseFile when not explicitly set.
func (bf *BTreeFile) GetID() primitives.FileID {
	bf.mutex.RLock()
	defer bf.mutex.RUnlock()
	if bf.indexID != 0 {
		return bf.indexID
	}
	return bf.BaseFile.GetID()
}

func (bf *BTreeFile) GetTupleDesc() *tuple.TupleDescription {
	td, _ := tuple.NewTupleDesc([]types.Type{bf.keyType}, []string{"key"})
	return td
}
