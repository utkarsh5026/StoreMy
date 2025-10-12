package hash

import (
	"errors"
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
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

// HashIndex implements a hash-based index structure for efficient key lookups.
// It uses separate chaining with overflow pages to handle collisions.
//
// Architecture:
//   - Fixed number of hash buckets determined at creation time
//   - Each bucket is stored as a HashPage
//   - Overflow pages are linked when buckets become full
//   - Uses FNV-1a hash function for key distribution
//
// Concurrency:
//   - All page access goes through HashFile which uses LockManager
//   - Transaction IDs (tid) passed to all operations for lock coordination
//   - Page-level locking inherited from storage layer
//
// Performance Characteristics:
//   - O(1) average case for Insert/Delete/Search operations
//   - O(n) worst case when many collisions cause long overflow chains
//   - Range queries are inefficient (O(n)) - scans all buckets
type HashIndex struct {
	indexID, numBuckets int
	keyType             types.Type
	file                *HashFile
}

// NewHashIndex creates a new hash index with the specified parameters.
//
// Parameters:
//   - indexID: Unique identifier for this index
//   - keyType: Type of keys this index will store (IntType, StringType, etc.)
//   - file: Initialized HashFile for page storage and retrieval
//
// Returns a configured HashIndex ready for insert/search operations.
func NewHashIndex(indexID int, keyType types.Type, file *HashFile) *HashIndex {
	return &HashIndex{
		indexID:    indexID,
		keyType:    keyType,
		file:       file,
		numBuckets: file.GetNumBuckets(),
	}
}

// Insert adds a key-value pair to the hash index.
// If the target bucket is full, creates or traverses overflow pages.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - key: Index key (must match keyType)
//   - rid: Tuple record ID pointing to actual data
//
// Returns error if:
//   - Key type doesn't match index keyType
//   - Page allocation fails
//   - Write operation fails
//
// Behavior:
//   - Hashes key to determine target bucket
//   - Traverses overflow chain if bucket is full
//   - Creates new overflow page if needed
//   - Marks pages dirty and writes to disk
func (hi *HashIndex) Insert(tid TID, key Field, rid RecID) error {
	if err := hi.validateKeyType(key); err != nil {
		return err
	}

	bucketNum := hi.hashKey(key)
	bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)

	currentPage := bucketPage
	for currentPage.IsFull() {
		if currentPage.GetOverflowPage() == NoOverFlowPage {
			currentPage, err = hi.createAndLinkOverflowPage(tid, bucketNum, currentPage)
			if err != nil {
				return fmt.Errorf("failed to create and link overflow page: %w", err)
			}
			break
		}
		currentPage, err = hi.readOverflowPage(tid, currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	if err := currentPage.AddEntry(entry); err != nil {
		return fmt.Errorf("failed to add entry: %w", err)
	}

	return hi.markAndWritePage(currentPage, tid)
}

// createAndLinkOverflowPage allocates a new overflow page and links it to the parent.
// Updates parent page with overflow pointer and writes both pages to disk.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - bucketNum: Bucket number for the new overflow page
//   - parentPage: Page to link the new overflow page to
//
// Returns the newly created overflow page or error on failure.
func (hi *HashIndex) createAndLinkOverflowPage(tid TID, bucketNum int, parentPage *HashPage) (*HashPage, error) {
	overflowPage, err := hi.file.AllocatePage(tid, bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate overflow page: %w", err)
	}

	parentPage.SetOverflowPage(overflowPage.pageID.PageNo())
	if err := hi.markAndWritePage(parentPage, tid); err != nil {
		return nil, fmt.Errorf("failed to write parent page with overflow link: %w", err)
	}

	return overflowPage, nil
}

// readOverflowPage safely reads an overflow page with validation.
// Checks for valid overflow pointer and page bounds before reading.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - parentPage: Page containing the overflow pointer
//
// Returns the overflow page or error if pointer is invalid or read fails.
func (hi *HashIndex) readOverflowPage(tid TID, parentPage *HashPage) (*HashPage, error) {
	overflowPageNum := parentPage.GetOverflowPage()

	if overflowPageNum == NoOverFlowPage {
		return nil, fmt.Errorf("no overflow page for this hash page")
	}

	// Validate overflow page number
	if overflowPageNum >= hi.file.NumPages() {
		return nil, fmt.Errorf("invalid overflow page number %d (max: %d)",
			overflowPageNum, hi.file.NumPages())
	}

	overflowPageID := NewHashPageID(hi.file.GetID(), overflowPageNum)
	page, err := hi.file.ReadPage(tid, overflowPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read overflow page %d: %w", overflowPageNum, err)
	}

	return page, nil
}

// markAndWritePage marks a page as dirty and writes it to disk.
// Required for transaction recovery and durability.
//
// Parameters:
//   - page: HashPage to mark and write
//   - tid: Transaction ID for tracking modifications
//
// Returns error if write operation fails.
func (hi *HashIndex) markAndWritePage(page *HashPage, tid TID) error {
	page.MarkDirty(true, tid)
	if err := hi.file.WritePage(page); err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}
	return nil
}

// Delete removes a key-value pair from the hash index.
// Traverses overflow chain to find and remove the matching entry.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - key: Index key to delete
//   - rid: Tuple record ID to delete (must match exactly)
//
// Returns error if:
//   - Key type doesn't match index keyType
//   - Entry not found in bucket or overflow chain
//   - Write operation fails
//
// Note: Only removes the first matching entry if duplicates exist.
func (hi *HashIndex) Delete(tid TID, key Field, rid RecID) error {
	if err := hi.validateKeyType(key); err != nil {
		return err
	}

	bucketPage, err := hi.getBucketPage(tid, key)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)
	err = hi.traverseOverflowChain(tid, bucketPage, func(hp *HashPage) error {
		if err := hp.RemoveEntry(entry); err == nil {
			hp.MarkDirty(true, tid)

			if writeErr := hi.file.WritePage(hp); writeErr != nil {
				return writeErr
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

// Search finds all tuple locations for a given key.
// Traverses the entire overflow chain to find all matching entries.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - key: Index key to search for
//
// Returns:
//   - Slice of TupleRecordIDs pointing to matching tuples
//   - Error if key type is invalid or page read fails
//
// Performance: O(1) average case, O(k) where k is overflow chain length.
func (hi *HashIndex) Search(tid TID, key Field) ([]RecID, error) {
	if err := hi.validateKeyType(key); err != nil {
		return nil, err
	}

	bucketPage, err := hi.getBucketPage(tid, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page: %w", err)
	}

	var results []*tuple.TupleRecordID

	err = hi.traverseOverflowChain(tid, bucketPage, func(hp *HashPage) error {
		entries := hp.FindEntries(key)
		results = append(results, entries...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error during overflow chain traversal: %w", err)
	}

	return results, nil
}

// RangeSearch finds all tuples where key is in [startKey, endKey].
//
// WARNING: Hash indexes do not efficiently support range queries.
// This implementation scans ALL buckets and overflow chains.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - startKey: Lower bound (inclusive)
//   - endKey: Upper bound (inclusive)
//
// Returns:
//   - Slice of TupleRecordIDs for tuples in range
//   - Error if key types don't match or page reads fail
//
// Performance: O(n) where n is total number of index entries.
// Consider using B-Tree index for efficient range queries.
func (hi *HashIndex) RangeSearch(tid TID, startKey, endKey Field) ([]RecID, error) {
	if startKey.Type() != hi.keyType || endKey.Type() != hi.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	var results []RecID
	for bucketNum := 0; bucketNum < hi.numBuckets; bucketNum++ {
		if bucketNum >= hi.file.NumPages() {
			continue
		}

		bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get bucket page %d: %w", bucketNum, err)
		}

		if bucketPage.GetNumEntries() == 0 {
			continue
		}

		err = hi.traverseOverflowChain(tid, bucketPage, func(hp *HashPage) error {
			for _, entry := range hp.GetEntries() {
				geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, startKey)
				leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, endKey)

				if geStart && leEnd {
					results = append(results, entry.RID)
				}
			}
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("error during overflow chain traversal: %w", err)
		}
	}

	return results, nil
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

// Close releases resources held by the index.
// Delegates to underlying HashFile for cleanup and page flushing.
//
// Returns error if file close operation fails.
func (hi *HashIndex) Close() error {
	return hi.file.Close()
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
//   - tid: Transaction ID for lock coordination
//   - key: Key to hash for bucket selection
//
// Returns the bucket page or error on failure.
func (hi *HashIndex) getBucketPage(tid TID, key Field) (*HashPage, error) {
	bucketNum := hi.hashKey(key)
	bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page: %w", err)
	}
	return bucketPage, nil
}

// traverseOverflowChain applies a function to each page in an overflow chain.
// Includes cycle detection to prevent infinite loops in corrupted chains.
//
// Parameters:
//   - tid: Transaction ID for lock coordination
//   - startPage: First page in the chain (typically bucket page)
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
func (hi *HashIndex) traverseOverflowChain(tid TID, startPage *HashPage, f func(*HashPage) error) error {
	var err error
	currentPage := startPage
	visited := make(map[int]bool)

	for currentPage != nil && len(visited) < maxOverflowPages {
		pageNum := currentPage.pageID.PageNo()
		if visited[pageNum] {
			break
		}
		visited[pageNum] = true

		if err := f(currentPage); err != nil {
			return fmt.Errorf("failed to apply function to overflow page %d: %w", pageNum, err)
		}

		if currentPage.GetOverflowPage() == NoOverFlowPage {
			break
		}

		currentPage, err = hi.readOverflowPage(tid, currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	return nil
}
