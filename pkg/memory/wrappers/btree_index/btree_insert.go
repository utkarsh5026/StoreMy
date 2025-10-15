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

// insertAndSplit handles insertion when the leaf page is full
// This method delegates to insertAndSplitLeaf in btree_split.go
func (bt *BTree) insertAndSplit(leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	return bt.insertAndSplitLeaf(leafPage, key, rid)
}

// insertAndSplitLeaf handles insertion into a full leaf page by splitting it
func (bt *BTree) insertAndSplitLeaf(leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	if !leafPage.IsLeafPage() {
		return fmt.Errorf("page is not a leaf")
	}

	// Create a temporary slice with all entries including the new one
	allEntries := make([]*index.IndexEntry, 0, len(leafPage.Entries)+1)
	inserted := false

	for _, entry := range leafPage.Entries {
		if !inserted && (compareKeys(key, entry.Key) < 0) {
			allEntries = append(allEntries, index.NewIndexEntry(key, rid))
			inserted = true
		}
		allEntries = append(allEntries, entry)
	}

	if !inserted {
		allEntries = append(allEntries, index.NewIndexEntry(key, rid))
	}

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

// insertIntoParent inserts a separator key into the parent after a split
func (bt *BTree) insertIntoParent(separatorKey types.Field, left, right *BTreePage) error {
	// If left page is root, create a new root
	if left.IsRoot() {
		return bt.createNewRoot(left, separatorKey, right)
	}

	// Get parent page
	parentPageID := btree.NewBTreePageID(bt.indexID, left.ParentPage)
	parentPage, err := bt.getPage(parentPageID, transaction.ReadWrite)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	// Check if parent is full
	if parentPage.IsFull() {
		return bt.insertAndSplitInternal(parentPage, separatorKey, right.GetBTreePageID())
	}

	// Insert into parent (not full)
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

	// Create temporary slice with all InternalPages including new one
	allChildren := make([]*btree.BTreeChildPtr, 0, len(internalPage.InternalPages)+1)
	inserted := false

	// First child has no key
	allChildren = append(allChildren, internalPage.InternalPages[0])

	for i := 1; i < len(internalPage.InternalPages); i++ {
		child := internalPage.InternalPages[i]
		if !inserted && compareKeys(key, child.Key) < 0 {
			allChildren = append(allChildren, btree.NewBtreeChildPtr(key, childPID))
			inserted = true
		}
		allChildren = append(allChildren, child)
	}

	if !inserted {
		allChildren = append(allChildren, btree.NewBtreeChildPtr(key, childPID))
	}

	// Split point: divide in half
	// For internal nodes, we push up the middle key
	midPoint := len(allChildren) / 2
	leftChildren := allChildren[:midPoint]
	middleKey := allChildren[midPoint].Key
	rightChildren := allChildren[midPoint:]

	// Fix: right children's first child shouldn't have a key
	rightChildren[0].Key = nil

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
