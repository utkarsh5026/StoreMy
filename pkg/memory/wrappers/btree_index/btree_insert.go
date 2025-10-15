package btreeindex

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// insertIntoLeaf inserts a key-value pair into a leaf page (assumes space is available)
func (bt *BTree) insertIntoLeaf(leaf *BTreePage, e *index.IndexEntry) error {
	insertPos := 0
	wasFirstKey := false

	for i, entry := range leaf.Entries {
		if entry.Equals(e) {
			return fmt.Errorf("duplicate entry: key %v, rid %v already exists", e.Key, e.RID)
		}

		if lt, _ := e.Key.Compare(primitives.LessThan, entry.Key); lt {
			insertPos = i
			if i == 0 {
				wasFirstKey = true
			}
			break
		}
		insertPos = i + 1
	}

	leaf.InsertEntry(e, insertPos)

	if err := bt.addDirtyPage(leaf, memory.InsertOperation); err != nil {
		return err
	}

	// If we inserted at position 0 and this isn't root, update parent separator key
	if wasFirstKey && !leaf.IsRoot() {
		return bt.updateParentKey(leaf, e.Key)
	}

	return nil
}

// insertAndSplitLeaf handles insertion into a full leaf page by splitting it
func (bt *BTree) insertAndSplitLeaf(leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	if !leafPage.IsLeafPage() {
		return fmt.Errorf("page is not a leaf")
	}

	allEntries := mergeEntryIntoSorted(leafPage.Entries, index.NewIndexEntry(key, rid))

	midPoint := len(allEntries) / 2
	leftEntries := allEntries[:midPoint]
	rightEntries := allEntries[midPoint:]

	// Update current page (left side)
	leafPage.Entries = leftEntries

	// Create new right page
	rightPage, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, true, leafPage.ParentPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new leaf page: %w", err)
	}

	rightPage.Entries = rightEntries
	rightPage.PrevLeaf = leafPage.PageNo()
	rightPage.NextLeaf = leafPage.NextLeaf

	bt.addDirtyPage(rightPage, memory.UpdateOperation)

	// Update leaf page links
	if leafPage.NextLeaf != btree.NoPage {
		nextPageID := btree.NewBTreePageID(bt.indexID, leafPage.NextLeaf)
		nextPage, err := bt.getPage(nextPageID, transaction.ReadWrite)
		if err == nil {
			nextPage.PrevLeaf = rightPage.PageNo()
			bt.addDirtyPage(nextPage, memory.UpdateOperation)
		}
	}

	leafPage.NextLeaf = rightPage.PageNo()

	// Get the first key of the right page (separator key)
	separatorKey := rightEntries[0].Key

	// Write both pages
	if err := bt.file.WritePage(leafPage); err != nil {
		return fmt.Errorf("failed to write left leaf: %w", err)
	}
	if err := bt.file.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right leaf: %w", err)
	}

	// Insert separator into parent
	return bt.insertIntoParent(separatorKey, leafPage, rightPage)
}

// mergeEntryIntoSorted inserts a new entry into a sorted slice
func mergeEntryIntoSorted(entries []*index.IndexEntry, newEntry *index.IndexEntry) []*index.IndexEntry {
	allEntries := make([]*index.IndexEntry, 0, len(entries)+1)
	inserted := false

	for _, entry := range entries {
		if !inserted && compareKeys(newEntry.Key, entry.Key) < 0 {
			allEntries = append(allEntries, newEntry)
			inserted = true
		}
		allEntries = append(allEntries, entry)
	}

	if !inserted {
		allEntries = append(allEntries, newEntry)
	}

	return allEntries
}

// insertIntoParent inserts a separator key into the parent after a split
func (bt *BTree) insertIntoParent(separatorKey types.Field, left, right *BTreePage) error {
	if left.IsRoot() {
		return bt.createNewRoot(left, separatorKey, right)
	}

	parentPageID := btree.NewBTreePageID(bt.indexID, left.ParentPage)
	parentPage, err := bt.getPage(parentPageID, transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	if parentPage.IsFull() {
		return bt.insertAndSplitInternal(parentPage, separatorKey, right.GetBTreePageID())
	}

	return bt.insertIntoInternal(parentPage, separatorKey, right.GetBTreePageID())
}

// insertIntoInternal inserts a key-pointer pair into an internal node (assumes space available)
func (bt *BTree) insertIntoInternal(internalPage *BTreePage, key types.Field, childPID *BTreePageID) error {
	if !internalPage.IsInternalPage() {
		return fmt.Errorf("page is not internal")
	}

	// Find insertion position
	insertPos := len(internalPage.InternalPages)
	for i := 1; i < len(internalPage.InternalPages); i++ {
		if compareKeys(key, internalPage.InternalPages[i].Key) < 0 {
			insertPos = i
			break
		}
	}

	// Create new child pointer
	newChildPtr := btree.NewBtreeChildPtr(key, childPID)

	// Insert at position
	internalPage.AddChildPtr(newChildPtr, insertPos)
	bt.addDirtyPage(internalPage, memory.InsertOperation)

	// Update child's parent pointer
	childPage, err := bt.getPage(childPID, transaction.ReadWrite)
	if err == nil {
		childPage.ParentPage = internalPage.PageNo()
		bt.addDirtyPage(childPage, memory.UpdateOperation)
	}

	return bt.file.WritePage(internalPage)
}

// insertAndSplitInternal handles insertion into a full internal page by splitting it
func (bt *BTree) insertAndSplitInternal(internalPage *BTreePage, key types.Field, childPID *BTreePageID) error {
	if !internalPage.IsInternalPage() {
		return fmt.Errorf("page is not internal")
	}

	allChildren := mergeChildPtrIntoSorted(internalPage.InternalPages, key, childPID)
	leftChildren, middleKey, rightChildren := splitInternalChildren(allChildren)

	// Update current page (left side)
	internalPage.InternalPages = leftChildren
	bt.addDirtyPage(internalPage, memory.UpdateOperation)

	// Create new right page
	rightPage, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, false, internalPage.ParentPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new internal page: %w", err)
	}

	rightPage.InternalPages = rightChildren
	bt.addDirtyPage(rightPage, memory.UpdateOperation)

	// Update all children's parent pointers
	for _, child := range leftChildren {
		childPage, err := bt.getPage(child.ChildPID, transaction.ReadWrite)
		if err == nil {
			childPage.ParentPage = internalPage.PageNo()
			bt.addDirtyPage(childPage, memory.UpdateOperation)
		}
	}

	for _, child := range rightChildren {
		childPage, err := bt.getPage(child.ChildPID, transaction.ReadWrite)
		if err == nil {
			childPage.ParentPage = rightPage.PageNo()
			bt.addDirtyPage(childPage, memory.UpdateOperation)
		}
	}

	return bt.insertIntoParent(middleKey, internalPage, rightPage)
}

// mergeChildPtrIntoSorted inserts a new child pointer into sorted children
func mergeChildPtrIntoSorted(children []*btree.BTreeChildPtr, key types.Field, childPID *BTreePageID) []*btree.BTreeChildPtr {
	allChildren := make([]*btree.BTreeChildPtr, 0, len(children)+1)
	inserted := false

	// First child has no key
	allChildren = append(allChildren, children[0])

	for i := 1; i < len(children); i++ {
		child := children[i]
		if !inserted && compareKeys(key, child.Key) < 0 {
			allChildren = append(allChildren, btree.NewBtreeChildPtr(key, childPID))
			inserted = true
		}
		allChildren = append(allChildren, child)
	}

	if !inserted {
		allChildren = append(allChildren, btree.NewBtreeChildPtr(key, childPID))
	}

	return allChildren
}

func splitInternalChildren(children []*btree.BTreeChildPtr) (left []*btree.BTreeChildPtr, middleKey types.Field, right []*btree.BTreeChildPtr) {
	midPoint := len(children) / 2

	left = children[:midPoint]
	middleKey = children[midPoint].Key
	right = children[midPoint:]

	// Right side's first child shouldn't have a key
	right[0].Key = nil

	return
}

// createNewRoot creates a new root page after splitting the old root
func (bt *BTree) createNewRoot(left *BTreePage, separatorKey types.Field, right *BTreePage) error {
	// Allocate new root page
	newRoot, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, false, -1)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	// Set up new root's children
	// First child (no key)
	newRoot.InternalPages = []*btree.BTreeChildPtr{
		{Key: nil, ChildPID: left.GetBTreePageID()},
		{Key: separatorKey, ChildPID: right.GetBTreePageID()},
	}

	// Update children's parent pointers
	left.ParentPage = newRoot.PageNo()
	right.ParentPage = newRoot.PageNo()

	// Update root pointer
	bt.rootPageID = newRoot.GetBTreePageID()

	bt.addDirtyPage(left, memory.UpdateOperation)
	bt.addDirtyPage(right, memory.UpdateOperation)
	bt.addDirtyPage(newRoot, memory.UpdateOperation)

	return nil
}
