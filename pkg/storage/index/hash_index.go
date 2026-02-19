package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"storemy/pkg/utils/functools"
	"sync"
)

// BucketNumber represents a logical hash bucket identifier.
// This is distinct from PageNumber - buckets are logical hash table slots,
// while pages are physical storage units.
type BucketNumber = int

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

	// DefaultBuckets is the default number of hash buckets when not specified
	DefaultBuckets BucketNumber = 256
)

// HashPage represents a single bucket page in a hash index structure.
// Each bucket stores hash entries (key-value pairs) and can link to overflow pages
// when the primary bucket becomes full.
//
// Layout:
//   - Header (20 bytes): bucket number (8 bytes), entry count (4 bytes), overflow pointer (8 bytes)
//   - Entries: Variable-length serialized index.IndexEntry records
//   - Padding: Zeroes to fill page to PageSize
type HashPage struct {
	pageID       *page.PageDescriptor      // Unique identifier for this page
	bucketNum    BucketNumber              // Hash bucket number this page belongs to (logical)
	numEntries   int                       // Current number of entries stored
	overflowPage primitives.PageNumber     // Page number of overflow page (physical)
	keyType      types.Type                // Type of keys stored (IntType, StringType, etc.)
	entries      []*IndexEntry             // Actual hash entries in this page
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
func NewHashPage(pageID *page.PageDescriptor, bucketNum BucketNumber, keyType types.Type) *HashPage {
	return &HashPage{
		pageID:       pageID,
		bucketNum:    bucketNum,
		numEntries:   0,
		overflowPage: NoOverFlowPage,
		keyType:      keyType,
		entries:      make([]*IndexEntry, 0, maxHashEntriesPerPage),
		isDirty:      true,
	}
}

// GetID returns the page identifier.
func (hp *HashPage) GetID() *page.PageDescriptor {
	return hp.pageID
}

// IsDirty returns the transaction ID that modified this page, or nil if clean.
// Implements page.Page interface for transaction coordination.
func (hp *HashPage) IsDirty() *primitives.TransactionID {
	if hp.isDirty {
		return hp.dirtyTxn
	}
	return nil
}

// MarkDirty marks this page as modified by a transaction.
// Captures before-image on first modification for rollback support.
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

	_ = binary.Write(buf, binary.BigEndian, uint64(hp.bucketNum)) // #nosec G115
	_ = binary.Write(buf, binary.BigEndian, int32(hp.numEntries)) // #nosec G115
	_ = binary.Write(buf, binary.BigEndian, hp.overflowPage)

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

	beforePage, err := deserializeHashPage(hp.beforeImage, hp.pageID)
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

// GetOverflowPage returns the page number of the linked overflow page.
func (hp *HashPage) GetOverflowPageNum() primitives.PageNumber {
	return hp.overflowPage
}

// HasNoOverflowPage returns true if this page has no overflow page linked.
func (hp *HashPage) HasNoOverflowPage() bool {
	return hp.overflowPage == NoOverFlowPage
}

// GetPageNo returns the physical page number of this hash page.
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
func (hp *HashPage) GetEntries() []*IndexEntry {
	return slices.Clone(hp.entries)
}

// AddEntry inserts a new hash entry into this page.
func (hp *HashPage) AddEntry(entry *IndexEntry) error {
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
func (hp *HashPage) RemoveEntry(e *IndexEntry) error {
	deleteIdx := slices.IndexFunc(hp.entries, func(entry *IndexEntry) bool {
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
func (hp *HashPage) FindEntries(key types.Field) []*tuple.TupleRecordID {
	matchEntries := functools.Filter(hp.entries, func(e *IndexEntry) bool {
		return e.Key.Equals(key)
	})

	return functools.Map(matchEntries, func(e *IndexEntry) *tuple.TupleRecordID {
		return e.RID
	})
}

// HashFile represents a persistent hash index file that provides fast key-based lookups.
// It implements a static hash table with fixed bucket count and overflow chaining.
// Each bucket is stored as a HashPage, with additional overflow pages allocated as needed.
//
// Storage Layout:
//   - Each bucket maps to a primary page number (initially bucket i → page i)
//   - Overflow pages are allocated sequentially as buckets fill up
//   - Pages are cached in memory for performance
type HashFile struct {
	*page.BaseFile
	keyType      types.Type
	numPages     primitives.PageNumber // Physical page count
	numBuckets   BucketNumber          // Logical bucket count
	indexID      primitives.FileID     // Override index ID (set when file is associated with an index)
	mutex        sync.RWMutex
	bucketPageID map[BucketNumber]primitives.PageNumber // Maps bucket number to primary page number
}

// NewHashFile creates or opens a hash index file at the specified path.
// If the file exists, it opens and reads the existing structure.
// If the file doesn't exist, it creates a new hash index file.
//
// Parameters:
//   - filename: Path to the hash index file
//   - keyType: Type of keys stored in the index (IntType, StringType, etc.)
//   - numBuckets: Number of hash buckets (uses DefaultBuckets if <= 0)
//
// Returns:
//   - *HashFile: The opened or created hash file
//   - error: Error if file operations fail or invalid parameters provided
func NewHashFile(filePath primitives.Filepath, keyType types.Type, numBuckets BucketNumber) (*HashFile, error) {
	if filePath == "" {
		return nil, fmt.Errorf("filePath cannot be empty")
	}

	if numBuckets <= 0 {
		numBuckets = DefaultBuckets
	}

	baseFile, err := page.NewBaseFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create base file: %w", err)
	}

	numPages, err := baseFile.NumPages()
	if err != nil {
		_ = baseFile.Close()
		return nil, fmt.Errorf("failed to get page count: %w", err)
	}

	hf := &HashFile{
		BaseFile:     baseFile,
		keyType:      keyType,
		numPages:     numPages,
		numBuckets:   numBuckets,
		bucketPageID: make(map[BucketNumber]primitives.PageNumber),
	}

	// Initialize bucket-to-page mapping
	// For now, bucket i is stored in page i (one bucket per page initially)
	for i := BucketNumber(0); i < numBuckets; i++ {
		hf.bucketPageID[i] = primitives.PageNumber(i)
	}

	return hf, nil
}

// GetKeyType returns the type of keys stored in this index.
func (hf *HashFile) GetKeyType() types.Type {
	return hf.keyType
}

// NumPages returns the total number of pages in this file.
// This includes both primary bucket pages and overflow pages.
func (hf *HashFile) NumPages() primitives.PageNumber {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numPages
}

// GetNumBuckets returns the number of hash buckets in this index.
// The bucket count is fixed at creation time and determines the hash distribution.
// Thread-safe with read lock.
func (hf *HashFile) GetNumBuckets() BucketNumber {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	return hf.numBuckets
}

// SetIndexID sets the index ID for this hash file.
// This should be called when the file is associated with a specific index.
// The indexID overrides the BaseFile's ID for page validation.
func (hf *HashFile) SetIndexID(indexID primitives.FileID) {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	hf.indexID = indexID
}

// GetID implements the DbFile interface by returning the index ID if set,
// otherwise returns the BaseFile's ID (hash of filename).
// This allows the file to be registered with a specific index ID.
func (hf *HashFile) GetID() primitives.FileID {
	hf.mutex.RLock()
	defer hf.mutex.RUnlock()
	if hf.indexID != 0 {
		return hf.indexID
	}
	return hf.BaseFile.GetID()
}

// ReadPage reads a hash page from disk and deserializes it.
// This is the DbFile interface implementation - it only handles I/O,
// no caching or page management (that's handled by PageStore).
//
// Parameters:
//   - pid: Page identifier (must be a HashPageID)
//
// Returns:
//   - page.Page: The requested page (as HashPage)
//   - error: Error if page ID is invalid or I/O fails
func (hf *HashFile) ReadPage(pageID *page.PageDescriptor) (page.Page, error) {

	if pageID.FileID() != hf.GetID() {
		return nil, fmt.Errorf("page ID index mismatch")
	}

	pageData, err := hf.ReadPageData(pageID.PageNo())
	if err != nil {
		if err == io.EOF {
			// Return a new empty page for unallocated pages
			// Use page number as bucket number for initial pages
			bucketNum := BucketNumber(pageID.PageNo()) // #nosec G115
			newPage := NewHashPage(pageID, bucketNum, hf.keyType)
			return newPage, nil
		}
		return nil, fmt.Errorf("failed to read page data: %w", err)
	}

	hashPage, err := deserializeHashPage(pageData, pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize page: %w", err)
	}

	return hashPage, nil
}

// WritePage writes a hash page to disk.
// This is the DbFile interface implementation - it only handles I/O,
// no caching (that's handled by PageStore).
//
// Parameters:
//   - p: The page to write (must be a HashPage)
//
// Returns:
//   - error: Error if page is nil, write fails, or sync fails
func (hf *HashFile) WritePage(p page.Page) error {
	if p == nil {
		return fmt.Errorf("page cannot be nil")
	}

	hashPage, ok := p.(*HashPage)
	if !ok {
		return fmt.Errorf("invalid page type: expected HashPage")
	}

	pageID := hashPage.pageID
	pageData := hashPage.GetPageData()

	if err := hf.WritePageData(pageID.PageNo(), pageData); err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}

	hf.mutex.Lock()
	if pageID.PageNo() >= hf.numPages {
		hf.numPages = pageID.PageNo() + 1
	}
	hf.mutex.Unlock()

	return nil
}

// AllocatePageNum allocates a new page number for overflow pages.
// This is used by HashIndex when creating new overflow pages.
// The actual page creation and management is done by PageStore.
//
// Returns:
//   - int: The newly allocated page number
//
// Thread-safe: Uses mutex to ensure atomic page number allocation.
func (hf *HashFile) AllocatePageNum() primitives.PageNumber {
	hf.mutex.Lock()
	defer hf.mutex.Unlock()
	pageNum := hf.numPages
	hf.numPages++
	return pageNum
}

// GetBucketPageNum returns the page number for a given bucket.
// The actual page retrieval should go through PageStore.
//
// Parameters:
//   - bucketNum: Bucket number (0 to numBuckets-1)
//
// Returns:
//   - int: Page number for this bucket
//   - error: Error if bucket number is invalid
func (hf *HashFile) GetBucketPageNum(bucketNum BucketNumber) (primitives.PageNumber, error) {
	if bucketNum < 0 || bucketNum >= hf.numBuckets {
		return 0, fmt.Errorf("invalid bucket number: %d", bucketNum)
	}

	hf.mutex.RLock()
	pageNum := hf.bucketPageID[bucketNum]
	hf.mutex.RUnlock()

	return pageNum, nil
}

func (hf *HashFile) GetTupleDesc() *tuple.TupleDescription {
	return nil
}

// deserializeHashPage reconstructs a hash page from serialized bytes.
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
//   - Parses header (bucket number as uint64, entry count as int32, overflow page as uint64)
//   - Deserializes each entry
//   - Infers keyType from first entry's key
//   - Marks page as clean (not dirty)
func deserializeHashPage(data []byte, pageID *page.PageDescriptor) (*HashPage, error) {
	if len(data) < hashHeaderSize {
		return nil, fmt.Errorf("invalid page data: too short")
	}

	buf := bytes.NewReader(data)

	var (
		bucketNumRaw uint64
		overflowPage primitives.PageNumber
		numEntries   int32
	)
	_ = binary.Read(buf, binary.BigEndian, &bucketNumRaw)
	_ = binary.Read(buf, binary.BigEndian, &numEntries)
	_ = binary.Read(buf, binary.BigEndian, &overflowPage)

	page := &HashPage{
		pageID:       pageID,
		bucketNum:    BucketNumber(bucketNumRaw),
		numEntries:   int(numEntries),
		overflowPage: overflowPage,
		entries:      make([]*IndexEntry, 0, numEntries),
		isDirty:      false,
	}

	for i := 0; i < int(numEntries); i++ {
		entry, err := DeserializeEntry(buf)
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
