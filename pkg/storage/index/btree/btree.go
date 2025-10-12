package btree

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// BTree implements the Index interface for B+Tree indexes
type BTree struct {
	indexID    int
	keyType    types.Type
	file       *BTreeFile
	rootPageID *BTreePageID
}

// NewBTree creates a new B+Tree index
func NewBTree(indexID int, keyType types.Type, file *BTreeFile) *BTree {
	return &BTree{
		indexID: indexID,
		keyType: keyType,
		file:    file,
	}
}

// Insert adds a key-value pair to the B+Tree
func (bt *BTree) Insert(tid *primitives.TransactionID, key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	// Get or create root page
	rootPage, err := bt.getRootPage(tid)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	// If root is empty, insert directly
	if rootPage.GetNumEntries() == 0 && rootPage.IsLeafPage() {
		return bt.insertIntoLeaf(tid, rootPage, key, rid)
	}

	// Find the appropriate leaf page
	leafPage, err := bt.findLeafPage(tid, rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	// Check if leaf is full - if so, split it
	if leafPage.IsFull() {
		return bt.insertAndSplit(tid, leafPage, key, rid)
	}

	// Insert into leaf
	return bt.insertIntoLeaf(tid, leafPage, key, rid)
}

// Delete removes a key-value pair from the B+Tree
func (bt *BTree) Delete(tid *primitives.TransactionID, key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(tid)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	// Find the leaf page containing the key
	leafPage, err := bt.findLeafPage(tid, rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	// Find and remove the entry
	return bt.deleteFromLeaf(tid, leafPage, key, rid)
}

// Search finds all tuple locations for a given key
func (bt *BTree) Search(tid *primitives.TransactionID, key types.Field) ([]*tuple.TupleRecordID, error) {
	if key.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	rootPage, err := bt.getRootPage(tid)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	// Find the leaf page
	leafPage, err := bt.findLeafPage(tid, rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf page: %w", err)
	}

	// Search within the leaf
	var results []*tuple.TupleRecordID
	for _, entry := range leafPage.entries {
		if entry.Key.Equals(key) {
			results = append(results, entry.RID)
		}
	}

	return results, nil
}

// RangeSearch finds all tuples where key is in [startKey, endKey]
func (bt *BTree) RangeSearch(tid *primitives.TransactionID, startKey, endKey types.Field) ([]*tuple.TupleRecordID, error) {
	if startKey.Type() != bt.keyType || endKey.Type() != bt.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	rootPage, err := bt.getRootPage(tid)
	if err != nil {
		return nil, fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 {
		return []*tuple.TupleRecordID{}, nil
	}

	// Find the leftmost leaf page containing startKey
	leafPage, err := bt.findLeafPage(tid, rootPage, startKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find start leaf page: %w", err)
	}

	var results []*tuple.TupleRecordID

	// Scan through leaf pages until we exceed endKey
	for leafPage != nil {
		for _, entry := range leafPage.entries {
			// Check if key >= startKey
			geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, startKey)
			// Check if key <= endKey
			leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, endKey)

			if geStart && leEnd {
				results = append(results, entry.RID)
			} else if !leEnd {
				// We've passed endKey, stop scanning
				return results, nil
			}
		}

		// Move to next leaf page
		if leafPage.nextLeaf == -1 {
			break
		}

		nextPageID := NewBTreePageID(bt.indexID, leafPage.nextLeaf)
		leafPage, err = bt.file.ReadPage(tid, nextPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf page: %w", err)
		}
	}

	return results, nil
}

// GetIndexType returns BTreeIndex
func (bt *BTree) GetIndexType() index.IndexType {
	return index.BTreeIndex
}

// GetKeyType returns the type of keys this index handles
func (bt *BTree) GetKeyType() types.Type {
	return bt.keyType
}

// Close releases resources held by the index
func (bt *BTree) Close() error {
	return bt.file.Close()
}

// getRootPage retrieves the root page of the B+Tree
func (bt *BTree) getRootPage(tid *primitives.TransactionID) (*BTreePage, error) {
	if bt.rootPageID == nil {
		// Create initial root page (leaf)
		bt.rootPageID = NewBTreePageID(bt.indexID, 0)
		rootPage := NewBTreeLeafPage(bt.rootPageID, bt.keyType, -1)
		rootPage.MarkDirty(true, tid)
		bt.file.WritePage(rootPage)
		return rootPage, nil
	}

	return bt.file.ReadPage(tid, bt.rootPageID)
}

// findLeafPage navigates from root to the leaf page that should contain the key
func (bt *BTree) findLeafPage(tid *primitives.TransactionID, currentPage *BTreePage, key types.Field) (*BTreePage, error) {
	// If we're at a leaf, we're done
	if currentPage.IsLeafPage() {
		return currentPage, nil
	}

	// Find the appropriate child pointer
	childPID := bt.findChildPointer(currentPage, key)
	if childPID == nil {
		return nil, fmt.Errorf("failed to find child pointer for key")
	}

	// Read the child page
	childPage, err := bt.file.ReadPage(tid, childPID)
	if err != nil {
		return nil, fmt.Errorf("failed to read child page: %w", err)
	}

	// Recursively search
	return bt.findLeafPage(tid, childPage, key)
}

// findChildPointer finds the appropriate child pointer for a given key in an internal node
func (bt *BTree) findChildPointer(internalPage *BTreePage, key types.Field) *BTreePageID {
	if !internalPage.IsInternalPage() || len(internalPage.children) == 0 {
		return nil
	}

	// In B+Tree internal nodes:
	// children[0] contains keys < children[1].Key
	// children[i] contains keys >= children[i].Key and < children[i+1].Key
	for i := len(internalPage.children) - 1; i >= 1; i-- {
		childPtr := internalPage.children[i]
		// If key >= childPtr.Key, go to this child
		if ge, _ := key.Compare(primitives.GreaterThanOrEqual, childPtr.Key); ge {
			return childPtr.ChildPID
		}
	}

	// Key is less than all separator keys, go to first child
	return internalPage.children[0].ChildPID
}

// insertIntoLeaf inserts a key-value pair into a leaf page (assumes space is available)
func (bt *BTree) insertIntoLeaf(tid *primitives.TransactionID, leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	insertPos := 0
	for i, entry := range leafPage.entries {
		if lt, _ := key.Compare(primitives.LessThan, entry.Key); lt {
			insertPos = i
			break
		}
		insertPos = i + 1
	}

	// Create new entry
	newEntry := &BTreeEntry{
		Key: key,
		RID: rid,
	}

	// Insert at position
	leafPage.entries = append(leafPage.entries[:insertPos],
		append([]*BTreeEntry{newEntry}, leafPage.entries[insertPos:]...)...)
	leafPage.numEntries++
	leafPage.MarkDirty(true, tid)

	// Write back to file
	return bt.file.WritePage(leafPage)
}

// deleteFromLeaf removes a key-value pair from a leaf page
func (bt *BTree) deleteFromLeaf(tid *primitives.TransactionID, leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	// Find the entry to delete
	deleteIdx := -1
	for i, entry := range leafPage.entries {
		if entry.Key.Equals(key) && entry.RID.PageID.Equals(rid.PageID) && entry.RID.TupleNum == rid.TupleNum {
			deleteIdx = i
			break
		}
	}

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	// Remove the entry
	leafPage.entries = append(leafPage.entries[:deleteIdx], leafPage.entries[deleteIdx+1:]...)
	leafPage.numEntries--
	leafPage.MarkDirty(true, tid)

	// Write back to file
	return bt.file.WritePage(leafPage)
}

// insertAndSplit handles insertion when the leaf page is full
// This method delegates to insertAndSplitLeaf in btree_split.go
func (bt *BTree) insertAndSplit(tid *primitives.TransactionID, leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	return bt.insertAndSplitLeaf(tid, leafPage, key, rid)
}

// compareKeys compares two keys and returns -1, 0, or 1
func compareKeys(k1, k2 types.Field) int {
	if k1.Equals(k2) {
		return 0
	}
	if lt, _ := k1.Compare(primitives.LessThan, k2); lt {
		return -1
	}
	return 1
}
