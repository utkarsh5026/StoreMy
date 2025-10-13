package btree

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// insertIntoLeaf inserts a key-value pair into a leaf page (assumes space is available)
func (bt *BTree) insertIntoLeaf(tid *primitives.TransactionID, leaf *BTreePage, e *index.IndexEntry) error {
	insertPos := 0
	wasFirstKey := false

	for i, entry := range leaf.entries {
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

	leaf.entries = append(leaf.entries[:insertPos],
		append([]*index.IndexEntry{e}, leaf.entries[insertPos:]...)...)
	leaf.numEntries++
	leaf.MarkDirty(true, tid)

	if err := bt.file.WritePage(leaf); err != nil {
		return err
	}

	// If we inserted at position 0 and this isn't root, update parent separator key
	if wasFirstKey && leaf.parentPage != -1 {
		return bt.updateParentKey(tid, leaf, e.Key)
	}

	return nil
}

// insertAndSplit handles insertion when the leaf page is full
// This method delegates to insertAndSplitLeaf in btree_split.go
func (bt *BTree) insertAndSplit(tid *primitives.TransactionID, leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	return bt.insertAndSplitLeaf(tid, leafPage, key, rid)
}

// insertAndSplitLeaf handles insertion into a full leaf page by splitting it
func (bt *BTree) insertAndSplitLeaf(tid *primitives.TransactionID, leafPage *BTreePage, key types.Field, rid *tuple.TupleRecordID) error {
	if !leafPage.IsLeafPage() {
		return fmt.Errorf("page is not a leaf")
	}

	// Create a temporary slice with all entries including the new one
	allEntries := make([]*index.IndexEntry, 0, len(leafPage.entries)+1)
	inserted := false

	for _, entry := range leafPage.entries {
		if !inserted && (compareKeys(key, entry.Key) < 0) {
			allEntries = append(allEntries, &index.IndexEntry{Key: key, RID: rid})
			inserted = true
		}
		allEntries = append(allEntries, entry)
	}

	if !inserted {
		allEntries = append(allEntries, &index.IndexEntry{Key: key, RID: rid})
	}

	// Split point: divide entries in half
	midPoint := len(allEntries) / 2
	leftEntries := allEntries[:midPoint]
	rightEntries := allEntries[midPoint:]

	// Update current page (left side)
	leafPage.entries = leftEntries
	leafPage.numEntries = len(leftEntries)
	leafPage.MarkDirty(true, tid)

	// Create new right page
	rightPage, err := bt.file.AllocatePage(tid, bt.keyType, true, leafPage.parentPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new leaf page: %w", err)
	}

	rightPage.entries = rightEntries
	rightPage.numEntries = len(rightEntries)
	rightPage.prevLeaf = leafPage.pageID.PageNo()
	rightPage.nextLeaf = leafPage.nextLeaf
	rightPage.MarkDirty(true, tid)

	// Update leaf page links
	if leafPage.nextLeaf != -1 {
		nextPageID := NewBTreePageID(bt.indexID, leafPage.nextLeaf)
		nextPage, err := bt.file.ReadPage(tid, nextPageID)
		if err == nil {
			nextPage.prevLeaf = rightPage.pageID.PageNo()
			nextPage.MarkDirty(true, tid)
			bt.file.WritePage(nextPage)
		}
	}

	leafPage.nextLeaf = rightPage.pageID.PageNo()

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
	return bt.insertIntoParent(tid, leafPage, separatorKey, rightPage)
}

// insertIntoParent inserts a separator key into the parent after a split
func (bt *BTree) insertIntoParent(tid *primitives.TransactionID, leftPage *BTreePage, separatorKey types.Field, rightPage *BTreePage) error {
	// If left page is root, create a new root
	if leftPage.parentPage == -1 {
		return bt.createNewRoot(tid, leftPage, separatorKey, rightPage)
	}

	// Get parent page
	parentPageID := NewBTreePageID(bt.indexID, leftPage.parentPage)
	parentPage, err := bt.file.ReadPage(tid, parentPageID)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	// Check if parent is full
	if parentPage.IsFull() {
		return bt.insertAndSplitInternal(tid, parentPage, separatorKey, rightPage.pageID)
	}

	// Insert into parent (not full)
	return bt.insertIntoInternal(tid, parentPage, separatorKey, rightPage.pageID)
}

// insertIntoInternal inserts a key-pointer pair into an internal node (assumes space available)
func (bt *BTree) insertIntoInternal(tid *primitives.TransactionID, internalPage *BTreePage, key types.Field, childPID *BTreePageID) error {
	if !internalPage.IsInternalPage() {
		return fmt.Errorf("page is not internal")
	}

	// Find insertion position
	insertPos := len(internalPage.children)
	for i := 1; i < len(internalPage.children); i++ {
		if compareKeys(key, internalPage.children[i].Key) < 0 {
			insertPos = i
			break
		}
	}

	// Create new child pointer
	newChildPtr := &BTreeChildPtr{
		Key:      key,
		ChildPID: childPID,
	}

	// Insert at position
	internalPage.children = append(
		internalPage.children[:insertPos],
		append([]*BTreeChildPtr{newChildPtr}, internalPage.children[insertPos:]...)...,
	)
	internalPage.numEntries++
	internalPage.MarkDirty(true, tid)

	// Update child's parent pointer
	childPage, err := bt.file.ReadPage(tid, childPID)
	if err == nil {
		childPage.parentPage = internalPage.pageID.PageNo()
		childPage.MarkDirty(true, tid)
		bt.file.WritePage(childPage)
	}

	return bt.file.WritePage(internalPage)
}

// insertAndSplitInternal handles insertion into a full internal page by splitting it
func (bt *BTree) insertAndSplitInternal(tid *primitives.TransactionID, internalPage *BTreePage, key types.Field, childPID *BTreePageID) error {
	if !internalPage.IsInternalPage() {
		return fmt.Errorf("page is not internal")
	}

	// Create temporary slice with all children including new one
	allChildren := make([]*BTreeChildPtr, 0, len(internalPage.children)+1)
	inserted := false

	// First child has no key
	allChildren = append(allChildren, internalPage.children[0])

	for i := 1; i < len(internalPage.children); i++ {
		child := internalPage.children[i]
		if !inserted && compareKeys(key, child.Key) < 0 {
			allChildren = append(allChildren, &BTreeChildPtr{Key: key, ChildPID: childPID})
			inserted = true
		}
		allChildren = append(allChildren, child)
	}

	if !inserted {
		allChildren = append(allChildren, &BTreeChildPtr{Key: key, ChildPID: childPID})
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
	internalPage.children = leftChildren
	internalPage.numEntries = len(leftChildren) - 1
	internalPage.MarkDirty(true, tid)

	// Create new right page
	rightPage, err := bt.file.AllocatePage(tid, bt.keyType, false, internalPage.parentPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new internal page: %w", err)
	}

	rightPage.children = rightChildren
	rightPage.numEntries = len(rightChildren) - 1
	rightPage.MarkDirty(true, tid)

	// Update all children's parent pointers
	for _, child := range leftChildren {
		childPage, err := bt.file.ReadPage(tid, child.ChildPID)
		if err == nil {
			childPage.parentPage = internalPage.pageID.PageNo()
			childPage.MarkDirty(true, tid)
			bt.file.WritePage(childPage)
		}
	}

	for _, child := range rightChildren {
		childPage, err := bt.file.ReadPage(tid, child.ChildPID)
		if err == nil {
			childPage.parentPage = rightPage.pageID.PageNo()
			childPage.MarkDirty(true, tid)
			bt.file.WritePage(childPage)
		}
	}

	// Write both pages
	if err := bt.file.WritePage(internalPage); err != nil {
		return fmt.Errorf("failed to write left internal: %w", err)
	}
	if err := bt.file.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right internal: %w", err)
	}

	// Insert middle key into parent
	return bt.insertIntoParent(tid, internalPage, middleKey, rightPage)
}

// createNewRoot creates a new root page after splitting the old root
func (bt *BTree) createNewRoot(tid *primitives.TransactionID, leftPage *BTreePage, separatorKey types.Field, rightPage *BTreePage) error {
	// Allocate new root page
	newRoot, err := bt.file.AllocatePage(tid, bt.keyType, false, -1)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	// Set up new root's children
	// First child (no key)
	newRoot.children = []*BTreeChildPtr{
		{Key: nil, ChildPID: leftPage.pageID},
		{Key: separatorKey, ChildPID: rightPage.pageID},
	}
	newRoot.numEntries = 1
	newRoot.MarkDirty(true, tid)

	// Update children's parent pointers
	leftPage.parentPage = newRoot.pageID.PageNo()
	leftPage.MarkDirty(true, tid)
	rightPage.parentPage = newRoot.pageID.PageNo()
	rightPage.MarkDirty(true, tid)

	// Update root pointer
	bt.rootPageID = newRoot.pageID

	// Write all pages
	if err := bt.file.WritePage(newRoot); err != nil {
		return fmt.Errorf("failed to write new root: %w", err)
	}
	if err := bt.file.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write left page: %w", err)
	}
	if err := bt.file.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right page: %w", err)
	}

	return nil
}
