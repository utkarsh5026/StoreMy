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
	bt := &BTree{
		indexID: indexID,
		keyType: keyType,
		file:    file,
	}

	// Set the file's index ID to match this BTree's index ID
	file.indexID = indexID

	if file.NumPages() > 0 {
		bt.rootPageID = NewBTreePageID(bt.indexID, 0)
	}

	return bt
}

// Insert adds a key-value pair to the B+Tree
func (bt *BTree) Insert(tid *primitives.TransactionID, key types.Field, rid *tuple.TupleRecordID) error {
	if key.Type() != bt.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", bt.keyType, key.Type())
	}

	entry := index.NewIndexEntry(key, rid)
	rootPage, err := bt.getRootPage(tid)
	if err != nil {
		return fmt.Errorf("failed to get root page: %w", err)
	}

	if rootPage.GetNumEntries() == 0 && rootPage.IsLeafPage() {
		return bt.insertIntoLeaf(tid, rootPage, entry)
	}

	leafPage, err := bt.findLeafPage(tid, rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	if leafPage.IsFull() {
		return bt.insertAndSplit(tid, leafPage, key, rid)
	}

	return bt.insertIntoLeaf(tid, leafPage, entry)
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

	entry := index.NewIndexEntry(key, rid)
	leafPage, err := bt.findLeafPage(tid, rootPage, key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	return bt.deleteFromLeaf(tid, leafPage, entry)
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
	if bt.rootPageID != nil {
		return bt.file.ReadPage(tid, bt.rootPageID)
	}

	bt.rootPageID = NewBTreePageID(bt.indexID, 0)
	root := NewBTreeLeafPage(bt.rootPageID, bt.keyType, -1)
	root.MarkDirty(true, tid)
	bt.file.WritePage(root)
	return root, nil
}

// findLeafPage navigates from root to the leaf page that should contain the key
func (bt *BTree) findLeafPage(tid *primitives.TransactionID, currentPage *BTreePage, key types.Field) (*BTreePage, error) {
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
		if ge, _ := key.Compare(primitives.GreaterThanOrEqual, childPtr.Key); ge {
			return childPtr.ChildPID
		}
	}

	// Key is less than all separator keys, go to first child
	return internalPage.children[0].ChildPID
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

// updateParentKey updates the separator key in the parent after first key changes
func (bt *BTree) updateParentKey(tid *primitives.TransactionID, childPage *BTreePage, newKey types.Field) error {
	if childPage.parentPage == -1 {
		return nil // Root has no parent
	}

	parentPageID := NewBTreePageID(bt.indexID, childPage.parentPage)
	parentPage, err := bt.file.ReadPage(tid, parentPageID)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	// Find the child pointer and update its key
	// Note: children[0] has no key in B+tree, so we start from index 1
	for i := 1; i < len(parentPage.children); i++ {
		if parentPage.children[i].ChildPID.Equals(childPage.pageID) {
			parentPage.children[i].Key = newKey
			parentPage.MarkDirty(true, tid)
			return bt.file.WritePage(parentPage)
		}
	}

	// If we get here, the child might be at index 0, which doesn't have a separator key
	// Check if it's the first child
	if len(parentPage.children) > 0 && parentPage.children[0].ChildPID.Equals(childPage.pageID) {
		// First child doesn't have a separator key, so nothing to update
		return nil
	}

	return fmt.Errorf("child not found in parent")
}
