package btreeindex

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Insert adds a key-value pair to the B+Tree, maintaining sorted order and balance.
// This is the main entry point for all insert operations in the index.
//
// The insertion process:
// 1. Validates key type matches index configuration
// 2. Finds the appropriate leaf page by traversing from root
// 3. If leaf has space: directly inserts maintaining sort order
// 4. If leaf is full: triggers split operation that may propagate upward
//
// Key behaviors:
// - Duplicate (key, RID) pairs are rejected with error
// - Empty tree gets first entry inserted in root leaf
// - Splits may cascade up to root, potentially increasing tree height
//
// Parameters:
//   - key: The index key to insert (must match index keyType)
//   - rid: Tuple record identifier pointing to actual row data
//
// Returns:
//   - error: Returns error if key type mismatch, page access fails, or duplicate exists
func (bt *BTree) Insert(key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	entry := index.NewIndexEntry(key, rid)
	rootPage, err := bt.getRootPage(transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 && rootPage.IsLeafPage() {
		return bt.insertIntoLeaf(rootPage, entry)
	}

	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	if leafPage.IsFull() {
		return bt.insertAndSplitLeaf(leafPage, key, rid)
	}

	return bt.insertIntoLeaf(leafPage, entry)
}

// Delete removes a key-value pair from the B+Tree, maintaining balance through merging.
// Finds the appropriate leaf page and removes the specified entry if it exists.
//
// The deletion process:
// 1. Validates key type matches index configuration
// 2. Traverses tree to find leaf page containing the key
// 3. Removes the entry from the leaf if found
// 4. May trigger merge/redistribution if page becomes full
//
// Note: The tid parameter is unused as transaction context is stored in bt.tx.
// This parameter exists for interface compatibility.
//
// Parameters:
//   - tid: Transaction ID (unused - bt.tx provides context)
//   - key: The index key to delete
//   - rid: The specific record ID to delete (allows multiple entries per key)
//
// Returns:
//   - error: Returns error if key type mismatch, page access fails, or entry not found
func (bt *BTree) Delete(key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	entry := index.NewIndexEntry(key, rid)
	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	return bt.deleteFromLeaf(leafPage, entry)
}

// Search finds all tuple locations (RIDs) for a given key using exact match.
// This is a point lookup operation that returns all rows with the specified key.
//
// The search process:
// 1. Validates key type matches index configuration
// 2. Traverses tree from root to appropriate leaf page
// 3. Scans leaf entries for all matches (handles duplicate keys)
// 4. Returns empty slice if key not found (not an error)
//
// B+Tree guarantees:
// - O(log N) traversal time where N is number of keys
// - All matching entries are in a single leaf page
// - Results maintain insertion order for duplicate keys
//
// Parameters:
//   - key: The key to search for (must match index keyType)
//
// Returns:
//   - []*tuple.TupleRecordID: Slice of all matching record IDs (empty if none found)
//   - error: Returns error only if key type mismatch or page access fails
func (bt *BTree) Search(key types.Field) ([]*tuple.TupleRecordID, error) {
	if key.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	leafPage, err := bt.findLeafPage(rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf page: %w", err)
	}

	var results []*tuple.TupleRecordID
	for _, entry := range leafPage.Entries {
		if entry.Key.Equals(key) {
			results = append(results, entry.RID)
		}
	}
	return results, nil
}

// RangeSearch finds all tuples where key is in the inclusive range [startKey, endKey].
// Leverages the B+Tree's sorted leaf chain to efficiently scan a range of keys.
//
// The range scan process:
// 1. Validates both keys match index type
// 2. Finds leaf page containing startKey
// 3. Scans forward through leaf chain collecting matching entries
// 4. Stops when endKey is exceeded or leaf chain ends
//
// Efficiency characteristics:
// - O(log N) to find starting leaf
// - O(M) to scan M matching entries across leaf pages
// - Uses doubly-linked leaf chain (NextLeaf pointers)
// - More efficient than repeated point lookups for ranges
//
// Note: The tid parameter is unused as transaction context is stored in bt.tx.
// This parameter exists for interface compatibility.
//
// Parameters:
//   - tid: Transaction ID (unused - bt.tx provides context)
//   - startKey: Lower bound of range (inclusive)
//   - endKey: Upper bound of range (inclusive)
//
// Returns:
//   - []*tuple.TupleRecordID: All record IDs with keys in [startKey, endKey]
//   - error: Returns error if key types mismatch or page access fails
func (bt *BTree) RangeSearch(startKey, endKey types.Field) ([]*tuple.TupleRecordID, error) {
	if startKey.Type() != bt.keyType || endKey.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	rootPage, err := bt.getRootPage(transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	leafPage, err := bt.findLeafPage(rootPage, startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find start leaf page: %w", err)
	}

	var results []*tuple.TupleRecordID

	for leafPage != nil {
		for _, entry := range leafPage.Entries {
			geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, startKey)
			leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, endKey)

			if geStart && leEnd {
				results = append(results, entry.RID)
			} else if !leEnd {
				return results, nil
			}
		}

		if !leafPage.HasNextLeaf() {
			break
		}

		_, nextLeaf := leafPage.Leaves()
		nextPageID := btree.NewBTreePageID(bt.indexID, nextLeaf)
		leafPage, err = bt.getPage(nextPageID, transaction.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf page: %w", err)
		}
	}

	return results, nil
}
