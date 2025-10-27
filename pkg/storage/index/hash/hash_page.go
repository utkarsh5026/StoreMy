package hash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"storemy/pkg/utils/functools"
)

const (
	// NoOverFlowPage indicates that a hash page has no overflow page linked.
	// Uses math.MaxUint64 as a sentinel value since PageNumber is uint64.
	NoOverFlowPage = math.MaxUint64

	// hashHeaderSize defines the size of the hash page header in bytes
	// Layout:
	//   - 8 bytes: bucket number (uint64)
	//   - 4 bytes: number of entries (int32)
	//   - 8 bytes: overflow page pointer (uint64, MaxUint64 if none)
	hashHeaderSize = 20

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
//   - Header (20 bytes): bucket number (8 bytes), entry count (4 bytes), overflow pointer (8 bytes)
//   - Entries: Variable-length serialized index.IndexEntry records
//   - Padding: Zeroes to fill page to PageSize
//
// Concurrency:
//   - Implements page.Page interface for transaction support
//   - Before-image captured on first modification for rollback
//   - Dirty flag tracks modifications within transaction
type HashPage struct {
	pageID       *page.PageDescriptor      // Unique identifier for this page
	bucketNum    primitives.PageNumber     // Hash bucket number this page belongs to
	numEntries   int                       // Current number of entries stored
	overflowPage primitives.PageNumber     // Page number of overflow page (-1 if none)
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
func NewHashPage(pageID *page.PageDescriptor, bucketNum primitives.PageNumber, keyType types.Type) *HashPage {
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
func (hp *HashPage) GetID() *page.PageDescriptor {
	return hp.pageID
}

// IsDirty returns the transaction ID that modified this page, or nil if clean.
// Implements page.Page interface for transaction coordination.
//
// Returns:
//   - Transaction ID if page is dirty
//   - nil if page is clean (matches disk state)
func (hp *HashPage) IsDirty() *primitives.TransactionID {
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
func (hp *HashPage) MarkDirty(dirty bool, tid *primitives.TransactionID) {
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
//  1. Header (20 bytes):
//     - Bucket number (8 bytes, big-endian uint64)
//     - Entry count (4 bytes, big-endian int32)
//     - Overflow page (8 bytes, big-endian uint64, MaxUint64 if none)
//  2. Entries (variable length):
//     - Each entry serialized with key + RID
//  3. Padding (zeroes to PageSize)
//
// Returns a byte slice of exactly PageSize (4096) bytes.
func (hp *HashPage) GetPageData() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, hp.bucketNum)
	binary.Write(buf, binary.BigEndian, int32(hp.numEntries))
	binary.Write(buf, binary.BigEndian, hp.overflowPage)

	for _, entry := range hp.entries {
		if err := entry.Serialize(buf); err != nil {
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
func (hp *HashPage) GetBucketNum() primitives.PageNumber {
	return hp.bucketNum
}

// GetOverflowPage returns the page number of the linked overflow page.
//
// Returns:
//   - Page number of overflow page (>= 0)
//   - NoOverFlowPage (math.MaxUint64) if no overflow page exists
func (hp *HashPage) GetOverflowPageNum() primitives.PageNumber {
	return hp.overflowPage
}

func (hp *HashPage) HasNoOverflowPage() bool {
	return hp.overflowPage == NoOverFlowPage
}

func (hp *HashPage) GetPageNo() primitives.PageNumber {
	return hp.pageID.PageNo()
}

// SetOverflowPage links an overflow page to this hash page.
// Used when bucket becomes full and needs additional storage.
//
// Parameters:
//   - pageNum: Page number of the overflow page to link
func (hp *HashPage) SetOverflowPage(pageNum primitives.PageNumber) {
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
