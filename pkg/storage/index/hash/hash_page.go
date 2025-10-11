package hash

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
	"storemy/pkg/utils/functools"
)

const (
	// Page header size (in bytes)
	// 4 bytes: bucket number
	// 4 bytes: number of entries
	// 4 bytes: overflow page pointer (-1 if none)
	hashHeaderSize = 12

	// Maximum number of entries per hash bucket page
	// Conservative estimate for 4KB pages
	maxHashEntriesPerPage = 150
)

// HashPage represents a bucket page in a hash index
type HashPage struct {
	pageID                              *HashPageID
	bucketNum, numEntries, overflowPage int
	keyType                             types.Type
	entries                             []*HashEntry
	isDirty                             bool
	dirtyTxn                            *primitives.TransactionID
	beforeImage                         []byte
}

// HashEntry represents a key-value pair in a hash bucket
type HashEntry struct {
	Key types.Field
	RID *tuple.TupleRecordID
}

func NewHashEntry(key types.Field, rid *tuple.TupleRecordID) *HashEntry {
	return &HashEntry{
		Key: key,
		RID: rid,
	}
}

func (he *HashEntry) Equals(other *HashEntry) bool {
	return he.Key.Equals(other.Key) &&
		he.RID.PageID.Equals(other.RID.PageID) &&
		he.RID.TupleNum == other.RID.TupleNum
}

// NewHashPage creates a new hash bucket page
func NewHashPage(pageID *HashPageID, bucketNum int, keyType types.Type) *HashPage {
	return &HashPage{
		pageID:       pageID,
		bucketNum:    bucketNum,
		numEntries:   0,
		overflowPage: -1,
		keyType:      keyType,
		entries:      make([]*HashEntry, 0, maxHashEntriesPerPage),
		isDirty:      true,
	}
}

// GetID returns the page ID
func (hp *HashPage) GetID() primitives.PageID {
	return hp.pageID
}

// IsDirty returns whether this page has been modified
func (hp *HashPage) IsDirty() *primitives.TransactionID {
	if hp.isDirty {
		return hp.dirtyTxn
	}
	return nil
}

// MarkDirty marks this page as dirty
func (hp *HashPage) MarkDirty(dirty bool, tid *primitives.TransactionID) {
	if dirty && !hp.isDirty && hp.beforeImage == nil {
		hp.beforeImage = hp.GetPageData()
	}
	hp.isDirty = dirty
	hp.dirtyTxn = tid
}

// GetPageData serializes the page to bytes
func (hp *HashPage) GetPageData() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int32(hp.bucketNum))
	binary.Write(buf, binary.BigEndian, int32(hp.numEntries))
	binary.Write(buf, binary.BigEndian, int32(hp.overflowPage))

	for _, entry := range hp.entries {
		if err := hp.serializeEntry(buf, entry); err != nil {
			panic(fmt.Sprintf("failed to serialize entry: %v", err))
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
func (hp *HashPage) GetBeforeImage() page.Page {
	if hp.beforeImage == nil {
		return nil
	}

	beforePage, err := DeserializeHashPage(hp.beforeImage, hp.pageID)
	if err != nil {
		return nil
	}
	return beforePage
}

// SetBeforeImage sets the before image to the current state
func (hp *HashPage) SetBeforeImage() {
	hp.beforeImage = hp.GetPageData()
}

// IsFull returns true if the page cannot accept more entries
func (hp *HashPage) IsFull() bool {
	return hp.numEntries >= maxHashEntriesPerPage
}

// GetNumEntries returns the number of entries in this page
func (hp *HashPage) GetNumEntries() int {
	return hp.numEntries
}

// GetBucketNum returns the bucket number
func (hp *HashPage) GetBucketNum() int {
	return hp.bucketNum
}

// GetOverflowPage returns the overflow page number (-1 if none)
func (hp *HashPage) GetOverflowPage() int {
	return hp.overflowPage
}

// SetOverflowPage sets the overflow page pointer
func (hp *HashPage) SetOverflowPage(pageNum int) {
	hp.overflowPage = pageNum
}

// GetEntries returns all entries in this page
func (hp *HashPage) GetEntries() []*HashEntry {
	return slices.Clone(hp.entries)
}

// AddEntry adds a new entry to the page
func (hp *HashPage) AddEntry(entry *HashEntry) error {
	if hp.IsFull() {
		return fmt.Errorf("page is full")
	}
	hp.entries = append(hp.entries, entry)
	hp.numEntries++
	return nil
}

// RemoveEntry removes an entry from the page
func (hp *HashPage) RemoveEntry(e *HashEntry) error {
	deleteIdx := slices.IndexFunc(hp.entries, func(entry *HashEntry) bool {
		return entry.Equals(e)
	})

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	hp.entries = append(hp.entries[:deleteIdx], hp.entries[deleteIdx+1:]...)
	hp.numEntries--
	return nil
}

// FindEntries finds all entries with the given key
func (hp *HashPage) FindEntries(key types.Field) []*tuple.TupleRecordID {
	matchEntries := functools.Filter(hp.entries, func(e *HashEntry) bool {
		return e.Key.Equals(key)
	})

	return functools.Map(matchEntries, func(e *HashEntry) *tuple.TupleRecordID {
		return e.RID
	})
}

// serializeEntry writes an entry to the buffer
func (hp *HashPage) serializeEntry(w io.Writer, entry *HashEntry) error {
	if err := hp.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	binary.Write(w, binary.BigEndian, int32(rid.PageID.GetTableID()))
	binary.Write(w, binary.BigEndian, int32(rid.PageID.PageNo()))
	binary.Write(w, binary.BigEndian, int32(rid.TupleNum))

	return nil
}

// serializeField writes a field to the buffer
func (hp *HashPage) serializeField(w io.Writer, field types.Field) error {
	binary.Write(w, binary.BigEndian, byte(field.Type()))
	return field.Serialize(w)
}

// DeserializeHashPage reads a hash page from bytes
func DeserializeHashPage(data []byte, pageID *HashPageID) (*HashPage, error) {
	if len(data) < hashHeaderSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	var bucketNum, numEntries, overflowPage int32
	binary.Read(buf, binary.BigEndian, &bucketNum)
	binary.Read(buf, binary.BigEndian, &numEntries)
	binary.Read(buf, binary.BigEndian, &overflowPage)

	page := &HashPage{
		pageID:       pageID,
		bucketNum:    int(bucketNum),
		numEntries:   int(numEntries),
		overflowPage: int(overflowPage),
		entries:      make([]*HashEntry, 0, numEntries),
		isDirty:      false,
	}

	for i := 0; i < int(numEntries); i++ {
		entry, err := deserializeHashEntry(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry %d: %w", i, err)
		}
		page.entries = append(page.entries, entry)
		if i == 0 && entry.Key != nil {
			page.keyType = entry.Key.Type()
		}
	}

	return page, nil
}

// deserializeHashEntry reads an entry from the buffer
func deserializeHashEntry(r *bytes.Reader) (*HashEntry, error) {
	key, err := deserializeField(r)
	if err != nil {
		return nil, err
	}

	var tableID, pageNum, tupleNum int32
	binary.Read(r, binary.BigEndian, &tableID)
	binary.Read(r, binary.BigEndian, &pageNum)
	binary.Read(r, binary.BigEndian, &tupleNum)

	pageID := NewHashPageID(int(tableID), int(pageNum))
	rid := tuple.NewTupleRecordID(pageID, int(tupleNum))

	return &HashEntry{
		Key: key,
		RID: rid,
	}, nil
}

// deserializeField reads a field from the buffer
func deserializeField(r *bytes.Reader) (types.Field, error) {
	fieldTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fieldType := types.Type(fieldTypeByte)
	return types.ParseField(r, fieldType)
}
