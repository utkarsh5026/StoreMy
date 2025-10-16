package hashindex

import (
	"errors"
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/hash"
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
	indexID, numBuckets int
	tx                  *transaction.TransactionContext
	keyType             types.Type
	file                *hash.HashFile    // I/O layer only
	pageStore           *memory.PageStore // Page management layer
}

// NewHashIndex creates a new hash index with the specified parameters.
//
// Parameters:
//   - indexID: Unique identifier for this index
//   - keyType: Type of keys this index will store (IntType, StringType, etc.)
//   - file: Initialized HashFile for page I/O operations
//   - pageStore: PageStore for page lifecycle management
//
// Returns a configured HashIndex ready for insert/search operations.
func NewHashIndex(indexID int, keyType types.Type, file *hash.HashFile, pageStore *memory.PageStore, tx *transaction.TransactionContext) *HashIndex {
	return &HashIndex{
		indexID:    indexID,
		keyType:    keyType,
		file:       file,
		pageStore:  pageStore,
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
func (hi *HashIndex) createAndLinkOverflowPage(bucketNum int, parentPage HashPage) (HashPage, error) {
	pageNum := hi.file.AllocatePageNum()
	pageID := hash.NewHashPageID(hi.file.GetID(), pageNum)

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

	overflowPageID := hash.NewHashPageID(hi.file.GetID(), overflowPageNum)
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
func (hi *HashIndex) getBucketPageByNum(bucketNum int) (HashPage, error) {
	pageNum, err := hi.file.GetBucketPageNum(bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page number: %w", err)
	}

	pageID := hash.NewHashPageID(hi.file.GetID(), pageNum)
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

// hashKey computes the hash of a key and returns the bucket number.
//
// Parameters:
//   - key: Field to hash (IntType, StringType, BoolType, or FloatType)
//
// Returns bucket number in range [0, numBuckets).
func (hi *HashIndex) hashKey(key types.Field) int {
	h, _ := key.Hash()
	return int(h) % hi.numBuckets
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
	bucketNum := hi.hashKey(key)
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
	visited := make(map[int]bool)

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

func (hi *HashIndex) getPageFromStore(pid *hash.HashPageID) (HashPage, error) {
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
