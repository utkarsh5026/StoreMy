package hashindex

import (
	"errors"
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Constants for overflow chain traversal safety
const (
	maxOverflowPages = 1000 // Prevent infinite loops in corrupted chains
)

var (
	errSuccess = errors.New("success")
)

type TID = *primitives.TransactionID
type RecID = *tuple.TupleRecordID
type Field = types.Field
type HashPage = *hash.HashPage
type HashFile = *hash.HashFile

// HashIndex implements a hash-based index structure for efficient key lookups.
// It uses separate chaining with overflow pages to handle collisions.
//
// Architecture:
//   - Fixed number of hash buckets determined at creation time
//   - Each bucket is stored as a HashPage
//   - Overflow pages are linked when buckets become full
//   - Uses FNV-1a hash function for key distribution
//   - PageStore manages page lifecycle (caching, locking, WAL)
//   - HashFile handles only disk I/O
//
// Concurrency:
//   - All page access goes through PageStore which uses LockManager
//   - Transaction contexts passed to all operations for lock coordination
//   - Page-level locking with 2PL protocol via PageStore
//
// Performance Characteristics:
//   - O(1) average case for Insert/Delete/Search operations
//   - O(n) worst case when many collisions cause long overflow chains
//   - Range queries are inefficient (O(n)) - scans all buckets
type HashIndex struct {
	indexID    primitives.FileID
	numBuckets hash.BucketNumber // Logical bucket count
	tx         *transaction.TransactionContext
	keyType    types.Type
	file       *hash.HashFile    // I/O layer only
	pageStore  *memory.PageStore // Page management layer
}

// NewHashIndex creates a new hash index with the specified parameters.
//
// The indexID parameter should match the file's natural ID (from BaseFile.GetID()).
// This ensures proper coordination between the catalog, physical storage, and page management.
//
// Parameters:
//   - indexID: Unique identifier for this index (should match file.GetID())
//   - keyType: Type of keys this index will store (IntType, StringType, etc.)
//   - file: Initialized HashFile for page I/O operations
//   - pageStore: PageStore for page lifecycle management
//
// Returns a configured HashIndex ready for insert/search operations.
func NewHashIndex(indexID primitives.FileID, keyType types.Type, file *hash.HashFile, store *memory.PageStore, tx *transaction.TransactionContext) *HashIndex {
	// Set the index ID on the file to ensure page ID validation works correctly
	file.SetIndexID(indexID)
	store.RegisterDbFile(primitives.FileID(indexID), file)

	return &HashIndex{
		indexID:    indexID,
		keyType:    keyType,
		file:       file,
		pageStore:  store,
		numBuckets: file.GetNumBuckets(),
		tx:         tx,
	}
}

// createAndLinkOverflowPage allocates a new overflow page and links it to the parent.
// Updates parent page with overflow pointer and marks both as dirty.
//
// Parameters:
//   - bucketNum: Bucket number for the new overflow page
//   - parentPage: Page to link the new overflow page to
//
// Returns the newly created overflow page or error on failure.
func (hi *HashIndex) createAndLinkOverflowPage(bucketNum hash.BucketNumber, parentPage HashPage) (HashPage, error) {
	pageNum := hi.file.AllocatePageNum()
	pageID := page.NewPageDescriptor(hi.indexID, pageNum)

	overflowPage := hash.NewHashPage(pageID, bucketNum, hi.keyType)
	overflowPage.MarkDirty(true, hi.tx.ID)

	parentPage.SetOverflowPage(overflowPage.GetPageNo())
	parentPage.MarkDirty(true, hi.tx.ID)

	return overflowPage, nil
}

// readOverflowPage safely reads an overflow page with validation.
// Checks for valid overflow pointer and page bounds before reading.
// Uses PageStore to get the page with proper locking.
//
// Parameters:
//   - ctx: Transaction context for page access
//   - p: Page containing the overflow pointer
//
// Returns the overflow page or error if pointer is invalid or read fails.
func (hi *HashIndex) readOverflowPage(p HashPage) (HashPage, error) {
	overflowPageNum := p.GetOverflowPageNum()

	if p.HasNoOverflowPage() {
		return nil, fmt.Errorf("no overflow page for this hash page")
	}

	if overflowPageNum >= hi.file.NumPages() {
		return nil, fmt.Errorf("invalid overflow page number %d (max: %d)",
			overflowPageNum, hi.file.NumPages())
	}

	overflowPageID := page.NewPageDescriptor(hi.indexID, overflowPageNum)
	return hi.getPageFromStore(overflowPageID)
}

// getBucketPage retrieves the primary bucket page for a given bucket number.
// Uses PageStore for page access with proper locking and caching.
//
// Parameters:
//   - ctx: Transaction context for page access
//   - bucketNum: Bucket number (0 to numBuckets-1)
//
// Returns the bucket page or error on failure.
func (hi *HashIndex) getBucketPageByNum(bucketNum hash.BucketNumber) (HashPage, error) {
	pageNum, err := hi.file.GetBucketPageNum(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page number: %w", err)
	}

	pageID := page.NewPageDescriptor(hi.indexID, pageNum)
	return hi.getPageFromStore(pageID)
}

// GetIndexType returns the type identifier for this index implementation.
// Returns index.HashIndex constant.
func (hi *HashIndex) GetIndexType() index.IndexType {
	return index.HashIndex
}

// GetKeyType returns the type of keys this index handles.
// All keys inserted/searched must match this type.
func (hi *HashIndex) GetKeyType() types.Type {
	return hi.keyType
}

// Close releases all resources held by the index and unregister it from PageStore.
// Should be called when the index is no longer needed to prevent resource leaks.
// Returns:
//   - error: Returns error if file close fails
func (hi *HashIndex) Close() error {
	err := hi.file.Close()
	// Unregister using the indexID, matching the registration
	hi.pageStore.UnregisterDbFile(hi.indexID)
	return err
}

// hashKey computes the hash of a key and returns the bucket number.
//
// Parameters:
//   - key: Field to hash (IntType, StringType, BoolType, or FloatType)
//
// Returns bucket number in range [0, numBuckets) and error if hashing fails.
func (hi *HashIndex) hashKey(key types.Field) (hash.BucketNumber, error) {
	h, err := key.Hash()
	if err != nil {
		return 0, fmt.Errorf("failed to hash key: %w", err)
	}
	return hash.BucketNumber(int(h) % int(hi.numBuckets)), nil // #nosec G115
}

// validateKeyType checks if the provided key matches the index's key type.
// Returns error with descriptive message if types don't match.
func (hi *HashIndex) validateKeyType(key Field) error {
	if key.Type() != hi.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", hi.keyType, key.Type())
	}
	return nil
}

// getBucketPage retrieves the primary bucket page for a given key.
// Hashes the key to determine bucket number and fetches the page.
//
// Parameters:
//   - ctx: Transaction context for page access
//   - key: Key to hash for bucket selection
//
// Returns the bucket page or error on failure.
func (hi *HashIndex) getBucketPage(key Field) (HashPage, error) {
	bucketNum, err := hi.hashKey(key)
	if err != nil {
		return nil, err
	}
	return hi.getBucketPageByNum(bucketNum)
}

// traverseOverflowChain applies a function to each page in an overflow chain.
// Includes cycle detection to prevent infinite loops in corrupted chains.
//
// Parameters:
//   - ctx: Transaction context for page access
//   - start: First page in the chain (typically bucket page)
//   - f: Function to apply to each page in the chain
//
// Returns:
//   - nil if traversal completes successfully
//   - Error from function f or if page read fails
//
// Safety features:
//   - Tracks visited pages to detect cycles
//   - Limits traversal to maxOverflowPages (1000)
//   - Validates overflow pointers before following
func (hi *HashIndex) traverseOverflowChain(start HashPage, f func(HashPage) error) error {
	var err error
	currentPage := start
	visited := make(map[primitives.PageNumber]bool)

	for currentPage != nil && len(visited) < maxOverflowPages {
		pageNum := currentPage.GetPageNo()
		if visited[pageNum] {
			break
		}
		visited[pageNum] = true

		if err := f(currentPage); err != nil {
			return fmt.Errorf("failed to apply function to overflow page %d: %w", pageNum, err)
		}

		if currentPage.HasNoOverflowPage() {
			break
		}

		currentPage, err = hi.readOverflowPage(currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	return nil
}

// getPageFromStore retrieves a hash page from the PageStore with proper type validation.
// This method centralizes all page access to ensure consistent locking and caching behavior.
func (hi *HashIndex) getPageFromStore(pid *page.PageDescriptor) (HashPage, error) {
	page, err := hi.pageStore.GetPage(hi.tx, hi.file, pid, transaction.ReadWrite)
	if err != nil {
		return nil, fmt.Errorf("failed to read overflow page %s: %w", pid, err)
	}
	hashPage, ok := page.(*hash.HashPage)
	if !ok {
		return nil, fmt.Errorf("invalid page type: expected HashPage")
	}
	return hashPage, nil
}

// removeEntry removes an index entry from the hash index.
// Traverses the overflow chain starting from bucketPage until the entry is found.
// When found, removes the entry and notifies PageStore of the deletion.
//
// Parameters:
//   - entry: IndexEntry to remove (contains key and RID)
//   - bucketPage: Starting page of the bucket's overflow chain
//
// Returns:
//   - nil if entry was successfully removed
//   - error if entry not found or removal operation fails
//
// Implementation notes:
//   - Uses errSuccess as sentinel to stop traversal when entry is found
//   - Marks page as dirty via HandlePageChange callback
//   - Returns error if entry doesn't exist in any page of the chain
func (hi *HashIndex) removeEntry(entry *index.IndexEntry, bucketPage HashPage) error {
	err := hi.traverseOverflowChain(bucketPage, func(hp HashPage) error {
		if err := hp.RemoveEntry(entry); err == nil {
			if err := hi.pageStore.HandlePageChange(
				hi.tx,
				memory.DeleteOperation,
				func() ([]page.Page, error) {
					return []page.Page{hp}, nil
				}); err != nil {
				return err
			}
			return errSuccess
		}
		return nil
	})

	if errors.Is(err, errSuccess) {
		return nil
	}

	return err
}

// getMatchingEntries retrieves all tuple record IDs that match the given key.
// Searches through the entire overflow chain starting from bucketPage.
//
// Parameters:
//   - key: Field value to search for
//   - bucketPage: Starting page of the bucket's overflow chain
//
// Returns:
//   - []*tuple.TupleRecordID: Slice of all matching record IDs (may be empty if no matches)
//   - error: Non-nil if overflow chain traversal fails
//
// Performance:
//   - O(c) where c is the length of the overflow chain for this bucket
//   - All pages in the chain are examined
//   - Multiple entries with same key are supported (non-unique index)
func (hi *HashIndex) getMatchingEntries(key Field, bucketPage HashPage) ([]*tuple.TupleRecordID, error) {
	var results []*tuple.TupleRecordID
	err := hi.traverseOverflowChain(bucketPage, func(hp HashPage) error {
		entries := hp.FindEntries(key)
		results = append(results, entries...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error during overflow chain traversal: %w", err)
	}

	return results, nil
}

// getEntriesInRange retrieves all tuple record IDs with keys in the range [start, end].
// Scans through the entire overflow chain and filters entries by range comparison.
//
// Parameters:
//   - start: Lower bound of range (inclusive)
//   - end: Upper bound of range (inclusive)
//   - bucketPage: Starting page of the bucket's overflow chain
//
// Returns:
//   - []*tuple.TupleRecordID: Slice of record IDs for entries in range (may be empty)
//   - error: Non-nil if overflow chain traversal fails
//
// Performance:
//   - O(n) where n is total entries in the bucket's overflow chain
//   - Not optimal for range queries (B+ tree is better suited)
//   - Each entry's key is compared against both bounds
//
// Note: This method only scans a single bucket. For full range queries across
// all buckets, caller must invoke this on each bucket and merge results.
func (hi *HashIndex) getEntriesInRange(start, end Field, bucketPage HashPage) ([]*tuple.TupleRecordID, error) {
	var results []*tuple.TupleRecordID
	err := hi.traverseOverflowChain(bucketPage, func(hp HashPage) error {
		for _, entry := range hp.GetEntries() {
			geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, start)
			leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, end)

			if geStart && leEnd {
				results = append(results, entry.RID)
			}
		}
		return nil
	})

	return results, err
}

// findFirstEmptyPage locates the first page in the overflow chain with available space.
// If all pages are full, creates a new overflow page and links it to the chain.
//
// Parameters:
//   - bucketPage: Starting page of the bucket's overflow chain
//   - bucketNum: Bucket number (used for overflow page initialization)
//
// Returns:
//   - HashPage: Page with available space for new entry
//   - error: Non-nil if page creation or traversal fails
//
// Behavior:
//   - Traverses overflow chain until a non-full page is found
//   - If no non-full page exists, allocates new overflow page
//   - New overflow pages are automatically linked to the chain
//   - Returned page is guaranteed to have space (unless page creation fails)
//
// Used by: Insert operation to find where to place new index entries
func (hi *HashIndex) findFirstEmptyPage(bucketPage HashPage, bucketNum hash.BucketNumber) (HashPage, error) {
	var err error
	currentPage := bucketPage
	for currentPage.IsFull() {
		if currentPage.HasNoOverflowPage() {
			currentPage, err = hi.createAndLinkOverflowPage(bucketNum, currentPage)
			if err != nil {
				return nil, fmt.Errorf("failed to create and link overflow page: %w", err)
			}
			break
		}
		currentPage, err = hi.readOverflowPage(currentPage)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page: %w", err)
		}
	}
	return currentPage, nil
}
