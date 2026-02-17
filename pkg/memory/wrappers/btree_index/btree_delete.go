package btreeindex

import (
	"fmt"
	"slices"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/page"
)

// deleteFromLeaf removes a key-value pair from a leaf page and handles rebalancing.
// It performs the following steps:
// 1. Locates and removes the target entry from the leaf
// 2. Updates parent keys if the first key was deleted
// 3. Handles underflow by redistributing or merging with siblings
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - leaf: The leaf page containing the entry to delete
//   - ie: The index entry to remove
//
// Returns an error if the entry is not found or if write operations fail.
func (bt *BTree) deleteFromLeaf(leaf *BTreePage, ie *index.IndexEntry) error {
	deleteIdx := slices.IndexFunc(leaf.Entries, func(e *index.IndexEntry) bool {
		return e.Equals(ie)
	})

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	wasFirstKey := (deleteIdx == 0 && leaf.GetNumEntries() > 1)
	if _, err := leaf.RemoveEntry(deleteIdx); err != nil {
		return fmt.Errorf("failed to remove entry: %w", err)
	}

	if err := bt.addDirtyPage(leaf, memory.DeleteOperation); err != nil {
		return err
	}

	if wasFirstKey && !leaf.IsRoot() {
		newFirstKey := leaf.Entries[0].Key
		if err := bt.updateParentKey(leaf, newFirstKey); err != nil {
			return err
		}
	}

	minEntries := btree.MaxEntriesPerPage / 2
	if leaf.GetNumEntries() < minEntries && !leaf.IsRoot() {
		return bt.handleUnderflow(leaf)
	}

	return nil
}

// handleUnderflow restores B-tree balance after a page falls below minimum occupancy.
// It follows the standard B-tree rebalancing algorithm:
// 1. If root has one child, promote that child to root
// 2. Try borrowing from left sibling (if it has spare entries)
// 3. Try borrowing from right sibling (if it has spare entries)
// 4. Merge with left sibling if borrowing fails
// 5. Merge with right sibling as last resort
//
// This recursively propagates underflow up the tree if merging causes parent underflow.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - page: The page with insufficient entries (< maxEntriesPerPage/2)
//
// Returns an error if page operations fail.
func (bt *BTree) handleUnderflow(underflowPage *BTreePage) error {
	if underflowPage.IsRoot() && underflowPage.IsInternalPage() && underflowPage.GetNumEntries() == 0 && len(underflowPage.Children()) == 1 {
		childPID := underflowPage.Children()[0].ChildPID
		childPage, err := bt.getPage(childPID, transaction.ReadWrite)
		if err != nil {
			return err
		}
		childPage.SetParent(primitives.InvalidPageNumber)
		bt.rootPageID = childPID
		return bt.addDirtyPage(childPage, memory.UpdateOperation)
	}

	parentPageID := page.NewPageDescriptor(bt.indexID, underflowPage.Parent())
	parent, err := bt.getPage(parentPageID, transaction.ReadWrite)
	if err != nil {
		return err
	}

	pageID := underflowPage.GetID()
	childIdx := slices.IndexFunc(parent.Children(), func(pp *btree.BTreeChildPtr) bool {
		return pp.ChildPID.Equals(pageID)
	})

	if childIdx == -1 {
		return fmt.Errorf("child not found in parent")
	}

	// Try to borrow from left sibling
	var left *BTreePage
	var lerr error
	if childIdx > 0 {
		left, lerr = bt.getSiblingPage(parent, childIdx, -1)
		if lerr == nil && left.HasMoreThanRequired() {
			return bt.redistributeFromLeft(left, underflowPage, parent, childIdx)
		}
	}

	var right *BTreePage
	var rerr error
	if childIdx < len(parent.Children())-1 {
		right, rerr = bt.getSiblingPage(parent, childIdx, 1)
		if rerr == nil && right.HasMoreThanRequired() {
			return bt.redistributeFromRight(underflowPage, right, parent, childIdx)
		}
	}

	if childIdx > 0 && lerr == nil {
		return bt.mergeWithLeft(left, underflowPage, parent, childIdx)
	}

	if childIdx < len(parent.Children())-1 && rerr == nil {
		return bt.mergeWithRight(underflowPage, right, parent, childIdx)
	}

	return nil
}

// redistributeFromLeft borrows the rightmost entry from the left sibling.
// For leaf pages: moves the last entry from left to the beginning of current
// For internal pages: rotates entries through the parent separator key
//
// This operation maintains B-tree ordering by updating the parent's separator key.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - left: Left sibling with spare entries
//   - current: Underflow page receiving the entry
//   - parent: Parent page containing separator keys
//   - pageIdx: Index of current page in parent's children array
//
// Returns an error if write operations fail.
func (bt *BTree) redistributeFromLeft(left, current, parent *BTreePage, pageIdx int) error {
	if current.IsLeafPage() {
		deleted, err := left.RemoveEntry(-1)
		if err != nil {
			return fmt.Errorf("failed to delete entry from left sibling: %w", err)
		}

		if err := current.InsertEntry(deleted, 0); err != nil {
			return fmt.Errorf("failed to insert entry into current page: %w", err)
		}

		if err := parent.UpdateChildrenKey(pageIdx, deleted.Key); err != nil {
			return fmt.Errorf("failed to update parent key: %w", err)
		}
		return nil
	}

	moved, err := left.RemoveChildPtr(-1)
	if err != nil {
		return fmt.Errorf("failed to remove entry from left sibling: %w", err)
	}

	ch := btree.NewBtreeChildPtr(nil, moved.ChildPID)
	if len(current.Children()) > 0 {
		if err := current.UpdateChildrenKey(0, parent.Children()[pageIdx].Key); err != nil {
			return fmt.Errorf("failed to update current key: %w", err)
		}
	}

	if err := current.AddChildPtr(ch, -1); err != nil {
		return fmt.Errorf("failed to add child pointer to current page: %w", err)
	}

	if err := parent.UpdateChildrenKey(pageIdx, moved.Key); err != nil {
		return fmt.Errorf("failed to update parent key: %w", err)
	}

	return nil
}

// redistributeFromRight borrows the leftmost entry from the right sibling.
// For leaf pages: moves the first entry from right to the end of current
// For internal pages: rotates entries through the parent separator key
//
// This operation maintains B-tree ordering by updating the parent's separator key.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - current: Underflow page receiving the entry
//   - right: Right sibling with spare entries
//   - parent: Parent page containing separator keys
//   - pageIdx: Index of current page in parent's children array
//
// Returns an error if write operations fail.
func (bt *BTree) redistributeFromRight(current, right, parent *BTreePage, pageIdx int) error {
	rightStart := 0

	if current.IsLeafPage() {
		deleted, err := right.RemoveEntry(rightStart)
		if err != nil {
			return fmt.Errorf("failed to remove entry from right sibling: %w", err)
		}

		if err := current.InsertEntry(deleted, -1); err != nil {
			return fmt.Errorf("failed to insert entry into current page: %w", err)
		}

		if err := parent.UpdateChildrenKey(pageIdx+1, right.Entries[rightStart].Key); err != nil {
			return fmt.Errorf("failed to update parent key: %w", err)
		}
		return nil
	}

	deleted, err := right.RemoveChildPtr(0)
	if err != nil {
		return fmt.Errorf("failed to remove child pointer from right sibling: %w", err)
	}

	ch := btree.NewBtreeChildPtr(parent.Children()[pageIdx+1].Key, deleted.ChildPID)
	if err := current.AddChildPtr(ch, -1); err != nil {
		return fmt.Errorf("failed to add child pointer to current page: %w", err)
	}

	if len(right.Children()) > 0 {
		if err := parent.UpdateChildrenKey(pageIdx+1, right.Children()[0].Key); err != nil {
			return fmt.Errorf("failed to update parent key: %w", err)
		}
		if err := right.UpdateChildrenKey(0, nil); err != nil {
			return fmt.Errorf("failed to clear right child key: %w", err)
		}
	}

	return nil
}

// mergeWithLeft merges the current page into its left sibling.
// This is a convenience wrapper around mergePages that handles left-side merging.
func (bt *BTree) mergeWithLeft(leftSibling, current, parent *BTreePage, pageIdx int) error {
	return bt.mergePages(leftSibling, current, parent, pageIdx, pageIdx)
}

// mergeWithRight merges the right sibling into the current page.
// This is a convenience wrapper around mergePages that handles right-side merging.
func (bt *BTree) mergeWithRight(page, rightSibling, parentPage *BTreePage, pageIdx int) error {
	return bt.mergePages(page, rightSibling, parentPage, pageIdx+1, pageIdx+1)
}

// mergePages combines two sibling pages into one, removing a child pointer from parent.
// For leaf pages: concatenates entries and updates doubly-linked leaf chain
// For internal pages: includes parent's separator key in the merge
//
// After merging, the right page is effectively deallocated and the parent's
// child pointer is removed. If this causes parent underflow, recursively rebalance.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - left: Page receiving all merged entries
//   - right: Page being merged (entries moved to left)
//   - parent: Parent page losing a child pointer
//   - childIdxToDelete: Index of child pointer to remove from parent
//   - separatorIdx: Index of separator key in parent (for internal nodes)
//
// Returns an error if page operations fail.
func (bt *BTree) mergePages(left, right, parent *BTreePage, childIdxToDelete, separatorIdx int) error {
	if left.IsLeafPage() {
		for _, e := range right.Entries {
			if err := left.InsertEntry(e, -1); err != nil {
				return fmt.Errorf("failed to insert entry during merge: %w", err)
			}
		}
		left.NextLeaf = right.NextLeaf
		if err := bt.addDirtyPage(left, memory.UpdateOperation); err != nil {
			return fmt.Errorf("failed to mark page dirty: %w", err)
		}

		if right.NextLeaf != primitives.InvalidPageNumber {
			nextPageID := page.NewPageDescriptor(bt.indexID, right.NextLeaf)
			nextPage, err := bt.getPage(nextPageID, transaction.ReadWrite)
			if err == nil {
				nextPage.PrevLeaf = left.PageNo()
				_ = bt.addDirtyPage(nextPage, memory.UpdateOperation)
			}

		}
	} else {
		separatorKey := parent.Children()[separatorIdx].Key
		if len(right.Children()) > 0 {
			if err := right.UpdateChildrenKey(0, separatorKey); err != nil {
				return fmt.Errorf("failed to update children key: %w", err)
			}
		}

		for _, ch := range right.Children() {
			if err := left.AddChildPtr(ch, -1); err != nil {
				return fmt.Errorf("failed to add child pointer during merge: %w", err)
			}
		}

		if err := bt.addDirtyPage(left, memory.InsertOperation); err != nil {
			return fmt.Errorf("failed to mark left page dirty: %w", err)
		}

		// Update all children from right page to point to left page as their parent
		for _, child := range right.Children() {
			childPage, err := bt.getPage(child.ChildPID, transaction.ReadWrite)
			if err == nil {
				childPage.ParentPage = left.PageNo()
				_ = bt.addDirtyPage(childPage, memory.UpdateOperation)
			}
		}
	}

	if _, err := parent.RemoveChildPtr(childIdxToDelete); err != nil {
		return fmt.Errorf("failed to remove child ptr from parent: %w", err)
	}
	if err := bt.addDirtyPage(parent, memory.DeleteOperation); err != nil {
		return fmt.Errorf("failed to mark parent dirty: %w", err)
	}

	if parent.HashLessThanRequired() && !parent.IsRoot() {
		return bt.handleUnderflow(parent)
	}
	return nil
}
