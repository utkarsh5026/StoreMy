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

	rightPage, err := bt.createRightLeafSibling(leafPage, rightEntries)
	if err != nil {
		return err
	}

	leafPage.Entries = leftEntries
	if err := bt.addDirtyPage(leafPage, memory.UpdateOperation); err != nil {
		return fmt.Errorf("failed to mark left leaf as dirty: %w", err)
	}

	separatorKey := rightEntries[0].Key
	return bt.insertIntoParent(separatorKey, leafPage, rightPage)
}

func (bt *BTree) createRightLeafSibling(left *BTreePage, entries []*index.IndexEntry) (*BTreePage, error) {
	right, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, true, left.ParentPage)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new leaf page: %w", err)
	}

	right.Entries = entries
	right.PrevLeaf = left.PageNo()
	right.NextLeaf = left.NextLeaf

	if left.NextLeaf != btree.NoPage {
		if err := bt.updateLeafPrevPointer(left.NextLeaf, right.PageNo()); err != nil {
			return nil, fmt.Errorf("error setting the pointers of the right")
		}
	}

	left.NextLeaf = right.PageNo()
	return right, bt.addDirtyPage(right, memory.InsertOperation)
}

func (bt *BTree) updateLeafPrevPointer(pageNo, newPrev int) error {
	pageID := btree.NewBTreePageID(bt.indexID, pageNo)
	page, err := bt.getPage(pageID, transaction.ReadWrite)
	if err != nil {
		return err
	}

	page.PrevLeaf = newPrev
	return bt.addDirtyPage(page, memory.UpdateOperation)
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
	left, middleKey, right := splitInternalChildren(allChildren)

	internalPage.InternalPages = left
	bt.addDirtyPage(internalPage, memory.UpdateOperation)

	rightPage, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, false, internalPage.ParentPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new internal page: %w", err)
	}

	rightPage.InternalPages = right
	bt.addDirtyPage(rightPage, memory.UpdateOperation)

	if err := bt.updateChildrenParentPointers(left, internalPage.PageNo()); err != nil {
		return err
	}
	if err := bt.updateChildrenParentPointers(right, rightPage.PageNo()); err != nil {
		return err
	}

	return bt.insertIntoParent(middleKey, internalPage, rightPage)
}
func (bt *BTree) updateChildrenParentPointers(children []*btree.BTreeChildPtr, parentPageNo int) error {
	for _, child := range children {
		if err := bt.updateChildParentPointer(child.ChildPID, parentPageNo); err != nil {
			return fmt.Errorf("failed to update child parent pointer: %w", err)
		}
	}
	return nil
}

func (bt *BTree) updateChildParentPointer(childPID *BTreePageID, parentPageNo int) error {
	childPage, err := bt.getPage(childPID, transaction.ReadWrite)
	if err != nil {
		return err
	}

	childPage.ParentPage = parentPageNo
	return bt.addDirtyPage(childPage, memory.UpdateOperation)
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
	newRoot, err := bt.file.AllocatePage(bt.tx.ID, bt.keyType, false, btree.NoPage)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	newRoot.InternalPages = []*btree.BTreeChildPtr{
		{Key: nil, ChildPID: left.GetBTreePageID()},
		{Key: separatorKey, ChildPID: right.GetBTreePageID()},
	}

	left.ParentPage = newRoot.PageNo()
	right.ParentPage = newRoot.PageNo()

	bt.rootPageID = newRoot.GetBTreePageID()

	bt.addDirtyPage(left, memory.UpdateOperation)
	bt.addDirtyPage(right, memory.UpdateOperation)
	bt.addDirtyPage(newRoot, memory.UpdateOperation)

	return nil
}
