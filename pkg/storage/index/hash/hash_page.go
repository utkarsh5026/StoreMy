package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"storemy/pkg/utils/functools"
)

const (
	// NoOverFlowPage indicates that a hash page has no overflow page linked
	NoOverFlowPage = -1

	// hashHeaderSize defines the size of the hash page header in bytes
	// Layout:
	//   - 4 bytes: bucket number (int32)
	//   - 4 bytes: number of entries (int32)
	//   - 4 bytes: overflow page pointer (int32, -1 if none)
	hashHeaderSize = 12

	// maxHashEntriesPerPage is the maximum number of hash entries that can fit in a single page.
	// Conservative estimate for 4KB pages to account for variable-length keys.
	// Actual capacity may be less for large string keys.
	maxHashEntriesPerPage = 150
)

// HashPage represents a single bucket page in a hash index structure.
// Each bucket stores hash entries (key-value pairs) and can link to overflow pages
// when the primary bucket becomes full.
//
// Layout:
//   - Header (12 bytes): bucket number, entry count, overflow pointer
//   - Entries: Variable-length serialized index.IndexEntry records
//   - Padding: Zeroes to fill page to PageSize
//
// Concurrency:
//   - Implements page.Page interface for transaction support
//   - Before-image captured on first modification for rollback
//   - Dirty flag tracks modifications within transaction
type HashPage struct {
	pageID       *HashPageID               // Unique identifier for this page
	bucketNum    int                       // Hash bucket number this page belongs to
	numEntries   int                       // Current number of entries stored
	overflowPage int                       // Page number of overflow page (-1 if none)
	keyType      types.Type                // Type of keys stored (IntType, StringType, etc.)
	entries      []*index.IndexEntry       // Actual hash entries in this page
	isDirty      bool                      // True if page modified since load
	dirtyTxn     *primitives.TransactionID // Transaction that dirtied this page
	beforeImage  []byte                    // Serialized page state before modifications
}

// NewHashPage creates a new hash bucket page with no entries.
// Initializes header fields and allocates space for entries.
//
// Parameters:
//   - pageID: Unique page identifier (file ID + page number)
//   - bucketNum: Hash bucket number this page represents
//   - keyType: Type of keys this page will store
//
// Returns a new HashPage marked as dirty (needs to be written).
func NewHashPage(pageID *HashPageID, bucketNum int, keyType types.Type) *HashPage {
	return &HashPage{
		pageID:       pageID,
		bucketNum:    bucketNum,
		numEntries:   0,
		overflowPage: NoOverFlowPage,
		keyType:      keyType,
		entries:      make([]*index.IndexEntry, 0, maxHashEntriesPerPage),
		isDirty:      true,
	}
}

// GetID returns the page identifier.
// Implements page.Page interface.
func (hp *HashPage) GetID() primitives.PageID {
	return hp.pageID
}

// IsDirty returns the transaction ID that modified this page, or nil if clean.
// Implements page.Page interface for transaction coordination.
//
// Returns:
//   - Transaction ID if page is dirty
//   - nil if page is clean (matches disk state)
func (hp *HashPage) IsDirty() TID {
	if hp.isDirty {
		return hp.dirtyTxn
	}
	return nil
}

// MarkDirty marks this page as modified by a transaction.
// Captures before-image on first modification for rollback support.
//
// Parameters:
//   - dirty: True to mark dirty, false to mark clean
//   - tid: Transaction ID making the modification
//
// Behavior:
//   - On first dirty mark, captures current state as before-image
//   - Before-image used for rollback if transaction aborts
func (hp *HashPage) MarkDirty(dirty bool, tid TID) {
	if dirty && !hp.isDirty && hp.beforeImage == nil {
		hp.beforeImage = hp.GetPageData()
	}
	hp.isDirty = dirty
	hp.dirtyTxn = tid
}

// GetPageData serializes the hash page to a byte array for disk storage.
// Implements page.Page interface.
//
// Format:
//  1. Header (12 bytes):
//     - Bucket number (4 bytes, big-endian int32)
//     - Entry count (4 bytes, big-endian int32)
//     - Overflow page (4 bytes, big-endian int32)
//  2. Entries (variable length):
//     - Each entry serialized with key + RID
//  3. Padding (zeroes to PageSize)
//
// Returns a byte slice of exactly PageSize (4096) bytes.
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

	// Pad to page size
	data := buf.Bytes()
	if len(data) < page.PageSize {
		padding := make([]byte, page.PageSize-len(data))
		data = append(data, padding...)
	}
	return data
}

// GetBeforeImage returns the page state before current transaction modifications.
// Used for rollback if transaction aborts.
//
// Returns:
//   - Deserialized HashPage representing pre-modification state
//   - nil if no before-image exists (page not modified)
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

// SetBeforeImage captures the current page state as the before-image.
// Called when starting a new transaction modification.
func (hp *HashPage) SetBeforeImage() {
	hp.beforeImage = hp.GetPageData()
}

// IsFull returns true if the page cannot accept more entries.
// Pages that are full require overflow pages for additional entries.
func (hp *HashPage) IsFull() bool {
	return hp.numEntries >= maxHashEntriesPerPage
}

// GetNumEntries returns the current number of entries stored in this page.
func (hp *HashPage) GetNumEntries() int {
	return hp.numEntries
}

// GetBucketNum returns the hash bucket number this page belongs to.
func (hp *HashPage) GetBucketNum() int {
	return hp.bucketNum
}

// GetOverflowPage returns the page number of the linked overflow page.
//
// Returns:
//   - Page number of overflow page (>= 0)
//   - NoOverFlowPage (-1) if no overflow page exists
func (hp *HashPage) GetOverflowPage() int {
	return hp.overflowPage
}

// SetOverflowPage links an overflow page to this hash page.
// Used when bucket becomes full and needs additional storage.
//
// Parameters:
//   - pageNum: Page number of the overflow page to link
func (hp *HashPage) SetOverflowPage(pageNum int) {
	hp.overflowPage = pageNum
}

// GetEntries returns a copy of all hash entries in this page.
// Returns a cloned slice to prevent external modification.
func (hp *HashPage) GetEntries() []*index.IndexEntry {
	return slices.Clone(hp.entries)
}

// AddEntry inserts a new hash entry into this page.
//
// Parameters:
//   - entry: index.IndexEntry to add (key-value pair)
//
// Returns error if:
//   - Page is full (numEntries >= maxHashEntriesPerPage)
//
// Behavior:
//   - Appends entry to entries slice
//   - Increments numEntries counter
//   - Does not check for duplicates
func (hp *HashPage) AddEntry(entry *index.IndexEntry) error {
	if hp.IsFull() {
		return fmt.Errorf("page is full")
	}
	hp.entries = append(hp.entries, entry)
	hp.numEntries++
	return nil
}

// RemoveEntry deletes a hash entry from this page.
// Searches for exact match on both key and RID.
//
// Parameters:
//   - e: index.IndexEntry to remove (must match exactly)
//
// Returns error if:
//   - Entry not found in this page
//
// Behavior:
//   - Finds first matching entry
//   - Removes from slice
//   - Decrements numEntries counter
func (hp *HashPage) RemoveEntry(e *index.IndexEntry) error {
	deleteIdx := slices.IndexFunc(hp.entries, func(entry *index.IndexEntry) bool {
		return entry.Equals(e)
	})

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	hp.entries = append(hp.entries[:deleteIdx], hp.entries[deleteIdx+1:]...)
	hp.numEntries--
	return nil
}

// FindEntries searches for all entries with a matching key.
// Returns all tuple locations for the given key value.
//
// Parameters:
//   - key: Field value to search for
//
// Returns:
//   - Slice of TupleRecordIDs for all matching entries
//   - Empty slice if no matches found
//
// Note: Hash indexes can contain duplicate keys with different RIDs.
func (hp *HashPage) FindEntries(key types.Field) []*tuple.TupleRecordID {
	matchEntries := functools.Filter(hp.entries, func(e *index.IndexEntry) bool {
		return e.Key.Equals(key)
	})

	return functools.Map(matchEntries, func(e *index.IndexEntry) *tuple.TupleRecordID {
		return e.RID
	})
}

// serializeEntry writes a single hash entry to the output stream.
// Internal method used by GetPageData for disk serialization.
//
// Format:
//  1. Key field (variable length based on type)
//  2. Table ID (4 bytes, big-endian int32)
//  3. Page number (4 bytes, big-endian int32)
//  4. Tuple number (4 bytes, big-endian int32)
//
// Parameters:
//   - w: Writer to serialize to
//   - entry: index.IndexEntry to serialize
//
// Returns error if serialization fails.
func (hp *HashPage) serializeEntry(w io.Writer, entry *index.IndexEntry) error {
	if err := hp.serializeField(w, entry.Key); err != nil {
		return err
	}

	rid := entry.RID
	binary.Write(w, binary.BigEndian, int32(rid.PageID.GetTableID()))
	binary.Write(w, binary.BigEndian, int32(rid.PageID.PageNo()))
	binary.Write(w, binary.BigEndian, int32(rid.TupleNum))

	return nil
}

// serializeField writes a field value to the output stream.
// Prefixes with type byte for deserialization.
//
// Format:
//  1. Type byte (1 byte)
//  2. Field data (variable length based on type)
//
// Parameters:
//   - w: Writer to serialize to
//   - field: Field to serialize
//
// Returns error if serialization fails.
func (hp *HashPage) serializeField(w io.Writer, field types.Field) error {
	binary.Write(w, binary.BigEndian, byte(field.Type()))
	return field.Serialize(w)
}

// DeserializeHashPage reconstructs a hash page from serialized bytes.
// Reads header and all entries from the byte array.
//
// Parameters:
//   - data: Serialized page data (should be PageSize bytes)
//   - pageID: Page identifier to assign to deserialized page
//
// Returns:
//   - Reconstructed HashPage with all entries
//   - Error if data is corrupted or too short
//
// Behavior:
//   - Parses header (bucket, count, overflow)
//   - Deserializes each entry
//   - Infers keyType from first entry's key
//   - Marks page as clean (not dirty)
func DeserializeHashPage(data []byte, pageID *HashPageID) (*HashPage, error) {
	if len(data) < hashHeaderSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	// Read header
	var bucketNum, numEntries, overflowPage int32
	binary.Read(buf, binary.BigEndian, &bucketNum)
	binary.Read(buf, binary.BigEndian, &numEntries)
	binary.Read(buf, binary.BigEndian, &overflowPage)

	page := &HashPage{
		pageID:       pageID,
		bucketNum:    int(bucketNum),
		numEntries:   int(numEntries),
		overflowPage: int(overflowPage),
		entries:      make([]*index.IndexEntry, 0, numEntries),
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

// deserializeHashEntry reads a single hash entry from the byte stream.
// Internal method used by DeserializeHashPage.
//
// Expects format:
//  1. Key field (type byte + data)
//  2. Table ID (4 bytes)
//  3. Page number (4 bytes)
//  4. Tuple number (4 bytes)
//
// Parameters:
//   - r: Reader to deserialize from
//
// Returns:
//   - Reconstructed index.IndexEntry
//   - Error if read fails or format is invalid
func deserializeHashEntry(r *bytes.Reader) (*index.IndexEntry, error) {
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

	return index.NewIndexEntry(key, rid), nil
}

// deserializeField reads a field value from the byte stream.
// Internal method used by deserializeHashEntry.
//
// Expects format:
//  1. Type byte (IntType, StringType, BoolType, FloatType)
//  2. Type-specific data
//
// Parameters:
//   - r: Reader to deserialize from
//
// Returns:
//   - Reconstructed Field
//   - Error if read fails or type is invalid
func deserializeField(r *bytes.Reader) (types.Field, error) {
	fieldTypeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fieldType := types.Type(fieldTypeByte)
	return types.ParseField(r, fieldType)
}
