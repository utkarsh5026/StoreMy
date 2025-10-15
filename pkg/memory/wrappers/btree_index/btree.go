package btreeindex

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type BTreeFile = btree.BTreeFile
type BTreePage = btree.BTreePage
type BTreePageID = btree.BTreePageID

// BTree implements the Index interface for B+Tree indexes
type BTree struct {
	indexID    int
	keyType    types.Type
	file       *BTreeFile
	rootPageID *BTreePageID
	tx         *transaction.TransactionContext
	store      *memory.PageStore
}

// NewBTree creates a new B+Tree index
func NewBTree(indexID int, keyType types.Type, file *BTreeFile, tx *transaction.TransactionContext, store *memory.PageStore) *BTree {
	bt := &BTree{
		indexID: indexID,
		keyType: keyType,
		file:    file,
		tx:      tx,
		store:   store,
	}

	// Set the file's index ID to match this BTree's index ID
	// file.indexID = indexID

	if file.NumPages() > 0 {
		bt.rootPageID = btree.NewBTreePageID(bt.indexID, 0)
	}

	return bt
}

// Insert adds a key-value pair to the B+Tree
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
		return bt.insertAndSplit(leafPage, key, rid)
	}

	return bt.insertIntoLeaf(leafPage, entry)
}

// Delete removes a key-value pair from the B+Tree
func (bt *BTree) Delete(tid *primitives.TransactionID, key types.Field, rid *tuple.TupleRecordID) error {
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

// Search finds all tuple locations for a given key
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
	for _, entry := range leafPage.Entries() {
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
		for _, entry := range leafPage.Entries() {
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
func (bt *BTree) getRootPage(perm transaction.Permissions) (*BTreePage, error) {
	if bt.rootPageID != nil {
		return bt.getPage(bt.rootPageID, perm)
	}

	bt.rootPageID = btree.NewBTreePageID(bt.indexID, 0)
	root := btree.NewBTreeLeafPage(bt.rootPageID, bt.keyType, -1)
	return root, nil
}

// findLeafPage navigates from root to the leaf page that should contain the key
func (bt *BTree) findLeafPage(currentPage *BTreePage, key types.Field) (*BTreePage, error) {
	if currentPage.IsLeafPage() {
		return currentPage, nil
	}

	childPID := bt.findChildPointer(currentPage, key)
	if childPID == nil {
		return nil, fmt.Errorf("failed to find child pointer for key")
	}

	childPage, err := bt.getPage(childPID, transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to read child page: %w", err)
	}

	return bt.findLeafPage(childPage, key)
}

// findChildPointer finds the appropriate child pointer for a given key in an internal node
func (bt *BTree) findChildPointer(internalPage *BTreePage, key types.Field) *BTreePageID {
	if !internalPage.IsInternalPage() || len(internalPage.Children()) == 0 {
		return nil
	}

	children := internalPage.Children()

	// In B+Tree internal nodes:
	// children[0] contains keys < children[1].Key
	// children[i] contains keys >= children[i].Key and < children[i+1].Key
	for i := len(children) - 1; i >= 1; i-- {
		childPtr := children[i]
		if ge, _ := key.Compare(primitives.GreaterThanOrEqual, childPtr.Key); ge {
			return childPtr.ChildPID
		}
	}

	return children[0].ChildPID
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
func (bt *BTree) updateParentKey(child *BTreePage, newKey types.Field) error {
	if child.IsRoot() {
		return nil
	}

	parID := btree.NewBTreePageID(bt.indexID, child.Parent())
	parent, err := bt.getPage(parID, transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	children := parent.Children()

	// Find the child pointer and update its key
	// Note: children[0] has no key in B+tree, so we start from index 1
	for i := 1; i < len(children); i++ {
		if children[i].ChildPID.Equals(child.GetID()) {
			parent.UpdateChildrenKey(i, newKey)
			return bt.addDirtyPage(parent, memory.UpdateOperation)
		}
	}

	// If we get here, the child might be at index 0, which doesn't have a separator key
	// Check if it's the first child
	if len(children) > 0 && children[0].ChildPID.Equals(child.GetID()) {

		return nil
	}

	return fmt.Errorf("child not found in parent")
}

func (bt *BTree) getPage(pageID *BTreePageID, perm transaction.Permissions) (*BTreePage, error) {
	p, err := bt.store.GetPage(bt.tx, bt.file, pageID, perm)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %w", err)
	}

	btreePage, ok := p.(*BTreePage)
	if !ok {
		return nil, fmt.Errorf("page is not a BTreePage")
	}

	return btreePage, nil
}

func (bt *BTree) addDirtyPage(p *BTreePage, op memory.OperationType) error {
	return bt.store.HandlePageChange(bt.tx, op, func() ([]page.Page, error) {
		return []page.Page{p}, nil
	})
}
