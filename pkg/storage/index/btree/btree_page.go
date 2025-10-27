package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/types"
)

const (
	// Page type constants
	pageTypeInternal byte = 0x01
	pageTypeLeaf     byte = 0x02
	NoPage                = -1

	// Page header size (in bytes)
	// 1 byte: page type
	// 4 bytes: parent page number
	// 4 bytes: number of Entries
	// 4 bytes: next leaf (for leaf pages, -1 for internal)
	// 4 bytes: prev leaf (for leaf pages, -1 for internal)
	headerSize = 17

	// Maximum number of Entries per page (conservative estimate)
	// For 4KB pages: (4096 - 17) / (8 + 8 + 4 + 4) ≈ 169 Entries for int keys
	// We'll use a more conservative number to handle string keys
	MaxEntriesPerPage = 150
)

// BTreePage represents a page in a B+Tree index
// It can be either an internal node or a leaf node
type BTreePage struct {
	pageID                         *page.PageDescriptor
	pageType                       byte
	ParentPage, NextLeaf, PrevLeaf int // -1 if root

	keyType       types.Type
	Entries       []*index.IndexEntry // For leaf pages
	InternalPages []*BTreeChildPtr    // For internal pages
	isDirty       bool
	dirtyTxn      *primitives.TransactionID
	beforeImage   []byte
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
func NewBTreeLeafPage(pageID *page.PageDescriptor, keyType types.Type, ParentPage int) *BTreePage {
	return &BTreePage{
		pageID:        pageID,
		pageType:      pageTypeLeaf,
		ParentPage:    ParentPage,
		NextLeaf:      NoPage,
		PrevLeaf:      NoPage,
		keyType:       keyType,
		Entries:       make([]*index.IndexEntry, 0, MaxEntriesPerPage),
		InternalPages: nil,
		isDirty:       false,
	}
}

// NewBTreeInternalPage creates a new internal page
func NewBTreeInternalPage(pageID *page.PageDescriptor, keyType types.Type, ParentPage int) *BTreePage {
	return &BTreePage{
		pageID:        pageID,
		pageType:      pageTypeInternal,
		ParentPage:    ParentPage,
		NextLeaf:      NoPage,
		PrevLeaf:      NoPage,
		keyType:       keyType,
		Entries:       nil,
		InternalPages: make([]*BTreeChildPtr, 0, MaxEntriesPerPage+1),
		isDirty:       false,
	}
}

// GetID returns the page ID as a primitives.PageID interface
func (p *BTreePage) GetID() primitives.PageID {
	return p.pageID
}

// GetBTreePageID returns the page ID as a concrete BTreePageID
func (p *BTreePage) GetBTreePageID() *page.PageDescriptor {
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
	binary.Write(buf, binary.BigEndian, int32(p.ParentPage))
	binary.Write(buf, binary.BigEndian, int32(p.GetNumEntries()))
	binary.Write(buf, binary.BigEndian, int32(p.NextLeaf))
	binary.Write(buf, binary.BigEndian, int32(p.PrevLeaf))

	if p.IsLeafPage() {
		for _, entry := range p.Entries {
			if err := p.serializeEntry(buf, entry); err != nil {
				panic(fmt.Sprintf("failed to serialize entry: %v", err))
			}
		}
	} else {
		// Write internal node InternalPages
		for i, child := range p.InternalPages {
			// For internal nodes: child[0] has no key, child[i] (i>0) has key
			if i > 0 {
				if err := p.serializeField(buf, child.Key); err != nil {
					panic(fmt.Sprintf("failed to serialize key: %v", err))
				}
			}
			binary.Write(buf, binary.BigEndian, int32(child.ChildPID.tableID))
			binary.Write(buf, binary.BigEndian, int32(child.ChildPID.pageNum))
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

func (p *BTreePage) Parent() int {
	return p.ParentPage
}

func (p *BTreePage) PageNo() int {
	return p.pageID.PageNo()
}

func (p *BTreePage) GetChildKey(index int) (types.Field, error) {
	if index < 0 || index >= len(p.InternalPages) {
		return nil, fmt.Errorf("invalid index %d for getting child key", index)
	}
	return p.InternalPages[index].Key, nil
}

func (p *BTreePage) InsertEntry(e *index.IndexEntry, index int) error {
	if index < -1 || index > len(p.Entries) {
		return fmt.Errorf("invalid index %d for inserting element", index)
	}

	if index == -1 {
		index = len(p.Entries)
	}

	p.Entries = slices.Insert(p.Entries, index, e)
	return nil
}

func (p *BTreePage) RemoveEntry(index int) (*index.IndexEntry, error) {
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

func (p *BTreePage) SetParent(id int) {
	p.ParentPage = id
}

// IsLeafPage returns true if this is a leaf page
func (p *BTreePage) IsLeafPage() bool {
	return p.pageType == pageTypeLeaf
}

func (p *BTreePage) IsRoot() bool {
	return p.ParentPage == NoPage
}

func (p *BTreePage) HasPreviousLeaf() bool {
	return p.PrevLeaf != NoPage
}

func (p *BTreePage) HasNextLeaf() bool {
	return p.NextLeaf != NoPage
}

func (p *BTreePage) Leaves() (left, right int) {
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
func (p *BTreePage) serializeEntry(w io.Writer, entry *index.IndexEntry) error {
	if err := p.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	// Write page ID type (0 for HeapPageID, 1 for BTreePageID)
	var pageIDType byte
	switch rid.PageID.(type) {
	case *BTreePageID:
		pageIDType = 1
	default: // HeapPageID or other
		pageIDType = 0
	}
	binary.Write(w, binary.BigEndian, pageIDType)

	binary.Write(w, binary.BigEndian, int32(rid.PageID.GetTableID()))
	binary.Write(w, binary.BigEndian, int32(rid.PageID.PageNo()))
	binary.Write(w, binary.BigEndian, int32(rid.TupleNum))
	return nil
}

// serializeField writes a field to the buffer
func (p *BTreePage) serializeField(w io.Writer, field types.Field) error {
	binary.Write(w, binary.BigEndian, byte(field.Type()))
	return field.Serialize(w)
}
