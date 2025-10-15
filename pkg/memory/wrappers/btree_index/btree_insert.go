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

// insertIntoLeaf inserts a key-value pair into a leaf page that has available space.
// It maintains sorted order of entries and handles duplicate key detection.
//
// The function performs the following steps:
// 1. Finds the correct insertion position to maintain sorted order
// 2. Checks for duplicate entries (same key AND RID)
// 3. Inserts the entry at the calculated position
// 4. Marks the page as dirty for transaction tracking
// 5. If inserted at position 0 (new minimum key), updates parent separator key
//
// Parameters:
//   - leaf: The leaf page where the entry will be inserted (must have space)
//   - e: The index entry containing the key and tuple record ID
//
// Returns:
//   - error: Returns error if duplicate found, page marking fails, or parent update fails
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

// insertAndSplitLeaf handles insertion into a full leaf page by splitting it into two pages.
// This is a core operation in B-tree growth, triggered when a leaf page has no space.
//
// The split process:
// 1. Merges the new entry into the existing entries (maintaining sort order)
// 2. Splits the merged entries at the midpoint
// 3. Keeps left half in the original page, creates new right sibling for right half
// 4. Updates leaf chain pointers (prev/next) to maintain sequential scan capability
// 5. Promotes the first key of the right page as separator to parent
//
// Parameters:
//   - leafPage: The full leaf page requiring split
//   - key: The key to be inserted
//   - rid: The tuple record ID associated with the key
//
// Returns:
//   - error: Returns error if allocation fails, page updates fail, or parent insertion fails
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

// createRightLeafSibling creates a new leaf page to serve as the right sibling after a split.
// It properly maintains the doubly-linked list structure of leaf pages for range scans.
//
// The function:
// 1. Allocates a new leaf page with the same parent as the left page
// 2. Assigns the provided entries to the new page
// 3. Updates the leaf chain: left <- new -> old_right
// 4. Fixes the prev pointer of the old right sibling (if exists)
// 5. Updates the next pointer of the left page to point to new page
//
// Parameters:
//   - left: The original leaf page being split
//   - entries: The entries that will belong to the new right sibling
//
// Returns:
//   - *BTreePage: The newly created right sibling page
//   - error: Returns error if allocation fails or pointer updates fail
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

// updateLeafPrevPointer updates the previous pointer of a leaf page in the leaf chain.
// Used during leaf splits to maintain the doubly-linked list integrity.
//
// Parameters:
//   - pageNo: The page number of the leaf whose prev pointer needs updating
//   - newPrev: The page number to set as the new previous sibling
//
// Returns:
//   - error: Returns error if page fetch fails or dirty marking fails
func (bt *BTree) updateLeafPrevPointer(pageNo, newPrev int) error {
	pageID := btree.NewBTreePageID(bt.indexID, pageNo)
	page, err := bt.getPage(pageID, transaction.ReadWrite)
	if err != nil {
		return err
	}

	page.PrevLeaf = newPrev
	return bt.addDirtyPage(page, memory.UpdateOperation)
}

// mergeEntryIntoSorted inserts a new entry into a sorted slice while maintaining sort order.
// This helper function is used during leaf splits to combine existing entries with the new entry.
//
// Algorithm:
// - Iterates through existing entries
// - Inserts new entry when first larger key is found
// - Appends remaining entries
// - If new entry is largest, appends at end
//
// Parameters:
//   - entries: The existing sorted entries
//   - newEntry: The new entry to insert
//
// Returns:
//   - []*index.IndexEntry: A new slice with all entries in sorted order
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

// insertIntoParent inserts a separator key into the parent node after a child page split.
// This is a critical function in maintaining B-tree structure during splits.
//
// The function handles three scenarios:
// 1. If left child is root: Creates a new root with two children
// 2. If parent has space: Directly inserts the separator and right child pointer
// 3. If parent is full: Recursively splits the parent (propagates split upward)
//
// Parameters:
//   - separatorKey: The key that separates left and right children (first key of right child)
//   - left: The left child page after split (could be original or newly split)
//   - right: The newly created right child page
//
// Returns:
//   - error: Returns error if root creation, parent read, or insertion fails
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

// insertIntoInternal inserts a key-pointer pair into an internal (non-leaf) node.
// Assumes the internal page has available space (checked by caller).
//
// Internal node structure: [P0, K1, P1, K2, P2, ..., Kn, Pn]
// Where Pi are child pointers and Ki are separator keys (all keys in Pi < Ki < all keys in Pi+1)
//
// The function:
// 1. Finds correct position based on key comparison
// 2. Inserts the child pointer with separator key
// 3. Updates the child's parent pointer to maintain tree structure
// 4. Marks both pages as dirty for transaction consistency
//
// Parameters:
//   - internalPage: The internal node where insertion occurs
//   - key: The separator key for the new child
//   - childPID: The page ID of the child being added
//
// Returns:
//   - error: Returns error if page is not internal, dirty marking fails, or write fails
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

// insertAndSplitInternal handles insertion into a full internal page by splitting it.
// This implements recursive split propagation up the tree, potentially increasing tree height.
//
// Split process for internal nodes:
// 1. Merges new key-pointer into existing children (maintaining sort order)
// 2. Splits at midpoint, extracting middle key as new separator
// 3. Left half stays in original page, right half goes to new sibling
// 4. Updates all affected children's parent pointers
// 5. Recursively inserts middle key into parent (may cause further splits)
//
// Note: Unlike leaf splits, internal splits "push up" the middle key rather than copying it.
//
// Parameters:
//   - internalPage: The full internal page requiring split
//   - key: The separator key to be inserted
//   - childPID: The child page pointer to be inserted
//
// Returns:
//   - error: Returns error if allocation fails, pointer updates fail, or parent insertion fails
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

// updateChildrenParentPointers updates the parent pointer for all children in a slice.
// Used after internal node splits to ensure all child pages point to the correct parent.
//
// Parameters:
//   - children: Slice of child pointers whose pages need parent updates
//   - parentPageNo: The page number to set as parent for all children
//
// Returns:
//   - error: Returns error if any child pointer update fails
func (bt *BTree) updateChildrenParentPointers(children []*btree.BTreeChildPtr, parentPageNo int) error {
	for _, child := range children {
		if err := bt.updateChildParentPointer(child.ChildPID, parentPageNo); err != nil {
			return fmt.Errorf("failed to update child parent pointer: %w", err)
		}
	}
	return nil
}

// updateChildParentPointer updates a single child page's parent pointer.
// Acquires the child page in read-write mode and marks it dirty.
//
// Parameters:
//   - childPID: The page ID of the child to update
//   - parentPageNo: The new parent page number
//
// Returns:
//   - error: Returns error if page fetch fails or dirty marking fails
func (bt *BTree) updateChildParentPointer(childPID *BTreePageID, parentPageNo int) error {
	childPage, err := bt.getPage(childPID, transaction.ReadWrite)
	if err != nil {
		return err
	}

	childPage.ParentPage = parentPageNo
	return bt.addDirtyPage(childPage, memory.UpdateOperation)
}

// mergeChildPtrIntoSorted inserts a new child pointer into a sorted slice of child pointers.
// Used during internal node splits to merge the new child into existing children.
//
// Important: The first child pointer (index 0) has no key (it's the leftmost pointer).
// Keys start from index 1 onwards.
//
// Parameters:
//   - children: The existing sorted child pointers
//   - key: The separator key for the new child
//   - childPID: The page ID of the new child
//
// Returns:
//   - []*btree.BTreeChildPtr: New slice with all child pointers in sorted order
func mergeChildPtrIntoSorted(children []*btree.BTreeChildPtr, key types.Field, childPID *BTreePageID) []*btree.BTreeChildPtr {
	allChildren := make([]*btree.BTreeChildPtr, 0, len(children)+1)
	inserted := false

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

// splitInternalChildren splits a slice of child pointers at the midpoint for internal node splits.
// Returns left half, middle key (which gets pushed up), and right half.
//
// Key behavior:
// - Middle child's key becomes the separator in the parent
// - Left side keeps keys as-is
// - Right side's first child has its key removed (becomes leftmost pointer of right sibling)
//
// Parameters:
//   - children: The merged child pointers to split
//
// Returns:
//   - left: Child pointers for the left page
//   - middleKey: The separator key to push up to parent
//   - right: Child pointers for the right page (first child has nil key)
func splitInternalChildren(children []*btree.BTreeChildPtr) (left []*btree.BTreeChildPtr, middleKey types.Field, right []*btree.BTreeChildPtr) {
	midPoint := len(children) / 2

	left = children[:midPoint]
	middleKey = children[midPoint].Key
	right = children[midPoint:]

	// Right side's first child shouldn't have a key
	right[0].Key = nil

	return
}

// createNewRoot creates a new root page when the old root splits.
// This is the only operation that increases the height of the B-tree.
//
// The new root structure:
// - Has exactly two children: the split left and right pages
// - Left child pointer has no key (nil)
// - Right child pointer has the separator key
// - Both old pages now have this new root as their parent
//
// After this operation:
// - Tree height increases by 1
// - Root page ID is updated to the new root
// - All three pages (new root, left, right) are marked dirty
//
// Parameters:
//   - left: The left page from the root split (old root)
//   - separatorKey: The key separating left and right subtrees
//   - right: The right page from the root split (newly created)
//
// Returns:
//   - error: Returns error if allocation fails or dirty marking fails
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
