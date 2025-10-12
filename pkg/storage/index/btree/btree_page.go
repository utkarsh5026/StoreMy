package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

const (
	// Page type constants
	pageTypeInternal byte = 0x01
	pageTypeLeaf     byte = 0x02
	noLeaf                = -1

	// Page header size (in bytes)
	// 1 byte: page type
	// 4 bytes: parent page number
	// 4 bytes: number of entries
	// 4 bytes: next leaf (for leaf pages, -1 for internal)
	// 4 bytes: prev leaf (for leaf pages, -1 for internal)
	headerSize = 17

	// Maximum number of entries per page (conservative estimate)
	// For 4KB pages: (4096 - 17) / (8 + 8 + 4 + 4) ≈ 169 entries for int keys
	// We'll use a more conservative number to handle string keys
	maxEntriesPerPage = 150
)

// BTreePage represents a page in a B+Tree index
// It can be either an internal node or a leaf node
type BTreePage struct {
	pageID                                     *BTreePageID
	pageType                                   byte
	parentPage, numEntries, nextLeaf, prevLeaf int // -1 if root

	keyType     types.Type
	entries     []*index.IndexEntry // For leaf pages
	children    []*BTreeChildPtr    // For internal pages
	isDirty     bool
	dirtyTxn    *primitives.TransactionID
	beforeImage []byte
}

// BTreeChildPtr represents a child pointer in an internal node
type BTreeChildPtr struct {
	Key      types.Field  // Separator key (minimum key in right child)
	ChildPID *BTreePageID // Child page ID
}

// NewBTreeLeafPage creates a new leaf page
func NewBTreeLeafPage(pageID *BTreePageID, keyType types.Type, parentPage int) *BTreePage {
	return &BTreePage{
		pageID:     pageID,
		pageType:   pageTypeLeaf,
		parentPage: parentPage,
		numEntries: 0,
		nextLeaf:   noLeaf,
		prevLeaf:   noLeaf,
		keyType:    keyType,
		entries:    make([]*index.IndexEntry, 0, maxEntriesPerPage),
		children:   nil,
		isDirty:    false,
	}
}

// NewBTreeInternalPage creates a new internal page
func NewBTreeInternalPage(pageID *BTreePageID, keyType types.Type, parentPage int) *BTreePage {
	return &BTreePage{
		pageID:     pageID,
		pageType:   pageTypeInternal,
		parentPage: parentPage,
		numEntries: 0,
		nextLeaf:   noLeaf,
		prevLeaf:   noLeaf,
		keyType:    keyType,
		entries:    nil,
		children:   make([]*BTreeChildPtr, 0, maxEntriesPerPage+1),
		isDirty:    false,
	}
}

// GetID returns the page ID
func (p *BTreePage) GetID() primitives.PageID {
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
	binary.Write(buf, binary.BigEndian, int32(p.parentPage))
	binary.Write(buf, binary.BigEndian, int32(p.numEntries))
	binary.Write(buf, binary.BigEndian, int32(p.nextLeaf))
	binary.Write(buf, binary.BigEndian, int32(p.prevLeaf))

	if p.IsLeafPage() {
		for _, entry := range p.entries {
			if err := p.serializeEntry(buf, entry); err != nil {
				panic(fmt.Sprintf("failed to serialize entry: %v", err))
			}
		}
	} else {
		// Write internal node children
		for i, child := range p.children {
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

// IsLeafPage returns true if this is a leaf page
func (p *BTreePage) IsLeafPage() bool {
	return p.pageType == pageTypeLeaf
}

// IsInternalPage returns true if this is an internal page
func (p *BTreePage) IsInternalPage() bool {
	return p.pageType == pageTypeInternal
}

// IsFull returns true if the page cannot accept more entries
func (p *BTreePage) IsFull() bool {
	return p.numEntries >= maxEntriesPerPage
}

// GetNumEntries returns the number of entries in this page
func (p *BTreePage) GetNumEntries() int {
	return p.numEntries
}

// serializeEntry writes an entry to the buffer
func (p *BTreePage) serializeEntry(w io.Writer, entry *index.IndexEntry) error {
	if err := p.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	binary.Write(w, binary.BigEndian, int32(rid.PageID.GetTableID()))
	binary.Write(w, binary.BigEndian, int32(rid.PageID.PageNo()))
	binary.Write(w, binary.BigEndian, int32(rid.TupleNum))
	return nil
}

// serializeField writes a field to the buffer
func (p *BTreePage) serializeField(w io.Writer, field types.Field) error {
	// Write field type
	binary.Write(w, binary.BigEndian, byte(field.Type()))

	// Write field data
	return field.Serialize(w)
}

// DeserializeBTreePage reads a page from bytes
func DeserializeBTreePage(data []byte, pageID *BTreePageID) (*BTreePage, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	pageType, _ := buf.ReadByte()

	var parentPage, numEntries, nextLeaf, prevLeaf int32
	binary.Read(buf, binary.BigEndian, &parentPage)
	binary.Read(buf, binary.BigEndian, &numEntries)
	binary.Read(buf, binary.BigEndian, &nextLeaf)
	binary.Read(buf, binary.BigEndian, &prevLeaf)

	page := &BTreePage{
		pageID:     pageID,
		pageType:   pageType,
		parentPage: int(parentPage),
		numEntries: int(numEntries),
		nextLeaf:   int(nextLeaf),
		prevLeaf:   int(prevLeaf),
		isDirty:    false,
	}

	// Deserialize entries/children based on page type
	if pageType == pageTypeLeaf {
		page.entries = make([]*index.IndexEntry, 0, numEntries)
		for i := 0; i < int(numEntries); i++ {
			entry, err := deserializeEntry(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
			}
			page.entries = append(page.entries, entry)
			if i == 0 && entry.Key != nil {
				page.keyType = entry.Key.Type()
			}
		}
	} else {
		page.children = make([]*BTreeChildPtr, 0, numEntries+1)
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
					page.keyType = key.Type()
				}
			}

			var tableID, pageNum int32
			binary.Read(buf, binary.BigEndian, &tableID)
			binary.Read(buf, binary.BigEndian, &pageNum)

			childPtr := &BTreeChildPtr{
				Key:      key,
				ChildPID: NewBTreePageID(int(tableID), int(pageNum)),
			}
			page.children = append(page.children, childPtr)
		}
	}

	return page, nil
}

// deserializeEntry reads an entry from the buffer
func deserializeEntry(r *bytes.Reader) (*index.IndexEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	var tableID, pageNum, tupleNum int32
	binary.Read(r, binary.BigEndian, &tableID)
	binary.Read(r, binary.BigEndian, &pageNum)
	binary.Read(r, binary.BigEndian, &tupleNum)

	pageID := NewBTreePageID(int(tableID), int(pageNum))

	rid := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: int(tupleNum),
	}

	return &index.IndexEntry{
		Key: key,
		RID: rid,
	}, nil
}

// deserializeField reads a field from the buffer
// This reads the field type byte first, then delegates to types.ParseField for consistency
func deserializeField(r *bytes.Reader) (types.Field, error) {
	fieldTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fieldType := types.Type(fieldTypeByte)
	return types.ParseField(r, fieldType)
}
