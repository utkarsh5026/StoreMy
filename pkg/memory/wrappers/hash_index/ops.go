package hashindex

import (
	"errors"
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// Insert adds a key-value pair to the hash index.
// If the target bucket is full, creates or traverses overflow pages.
//
// Parameters:
//   - ctx: Transaction context for lock coordination and page tracking
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
//   - Marks pages dirty (PageStore handles actual writes on commit)
func (hi *HashIndex) Insert(ctx *transaction.TransactionContext, key Field, rid RecID) error {
	if err := hi.validateKeyType(key); err != nil {
		return err
	}

	bucketNum := hi.hashKey(key)
	bucketPage, err := hi.getBucketPage(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)

	currentPage := bucketPage
	for currentPage.IsFull() {
		if currentPage.HasNoOverflowPage() {
			currentPage, err = hi.createAndLinkOverflowPage(ctx, bucketNum, currentPage)
			if err != nil {
				return fmt.Errorf("failed to create and link overflow page: %w", err)
			}
			break
		}
		currentPage, err = hi.readOverflowPage(ctx, currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	if err := currentPage.AddEntry(entry); err != nil {
		return fmt.Errorf("failed to add entry: %w", err)
	}

	return hi.pageStore.HandlePageChange(
		ctx,
		memory.InsertOperation,
		func() ([]page.Page, error) {
			return []page.Page{currentPage}, nil
		})
}

// Delete removes a key-value pair from the hash index.
// Traverses overflow chain to find and remove the matching entry.
//
// Parameters:
//   - ctx: Transaction context for page access
//   - key: Index key to delete
//   - rid: Tuple record ID to delete (must match exactly)
//
// Returns error if:
//   - Key type doesn't match index keyType
//   - Entry not found in bucket or overflow chain
//   - Write operation fails
//
// Note: Only removes the first matching entry if duplicates exist.
func (hi *HashIndex) Delete(ctx *transaction.TransactionContext, key Field, rid RecID) error {
	if err := hi.validateKeyType(key); err != nil {
		return err
	}

	bucketPage, err := hi.getBucketPage(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)
	err = hi.traverseOverflowChain(ctx, bucketPage, func(hp HashPage) error {
		if err := hp.RemoveEntry(entry); err == nil {
			// Mark dirty - PageStore handles flush on commit
			hp.MarkDirty(true, ctx.ID)
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
//   - ctx: Transaction context for page access
//   - key: Index key to search for
//
// Returns:
//   - Slice of TupleRecordIDs pointing to matching tuples
//   - Error if key type is invalid or page read fails
//
// Performance: O(1) average case, O(k) where k is overflow chain length.
func (hi *HashIndex) Search(ctx *transaction.TransactionContext, key Field) ([]RecID, error) {
	if err := hi.validateKeyType(key); err != nil {
		return nil, err
	}

	bucketPage, err := hi.getBucketPage(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page: %w", err)
	}

	var results []*tuple.TupleRecordID

	err = hi.traverseOverflowChain(ctx, bucketPage, func(hp HashPage) error {
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
//   - ctx: Transaction context for page access
//   - startKey: Lower bound (inclusive)
//   - endKey: Upper bound (inclusive)
//
// Returns:
//   - Slice of TupleRecordIDs for tuples in range
//   - Error if key types don't match or page reads fail
//
// Performance: O(n) where n is total number of index entries.
// Consider using B-Tree index for efficient range queries.
func (hi *HashIndex) RangeSearch(ctx *transaction.TransactionContext, startKey, endKey Field) ([]RecID, error) {
	if startKey.Type() != hi.keyType || endKey.Type() != hi.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	var results []RecID
	for bucketNum := 0; bucketNum < hi.numBuckets; bucketNum++ {
		if bucketNum >= hi.file.NumPages() {
			continue
		}

		bucketPage, err := hi.getBucketPageByNum(ctx, bucketNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get bucket page %d: %w", bucketNum, err)
		}

		if bucketPage.GetNumEntries() == 0 {
			continue
		}

		err = hi.traverseOverflowChain(ctx, bucketPage, func(hp HashPage) error {
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
