package btree

import (
	"fmt"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
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
func (bt *BTree) deleteFromLeaf(tid *primitives.TransactionID, leaf *BTreePage, ie *index.IndexEntry) error {
	deleteIdx := slices.IndexFunc(leaf.entries, func(e *index.IndexEntry) bool {
		return e.Equals(ie)
	})

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	wasFirstKey := (deleteIdx == 0 && leaf.numEntries > 1)
	deleteFromPage(leaf, deleteIdx, leaf.entries, tid)

	if err := bt.file.WritePage(leaf); err != nil {
		return err
	}

	if wasFirstKey && leaf.parentPage != -1 {
		newFirstKey := leaf.entries[0].Key
		if err := bt.updateParentKey(tid, leaf, newFirstKey); err != nil {
			return err
		}
	}

	minEntries := maxEntriesPerPage / 2
	if leaf.numEntries < minEntries && leaf.parentPage != -1 {
		return bt.handleUnderflow(tid, leaf)
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
func (bt *BTree) handleUnderflow(tid *primitives.TransactionID, page *BTreePage) error {
	if page.parentPage == -1 {
		if page.IsInternalPage() && page.numEntries == 0 && len(page.children) == 1 {
			childPID := page.children[0].ChildPID
			childPage, err := bt.file.ReadPage(tid, childPID)
			if err != nil {
				return err
			}
			childPage.parentPage = -1
			childPage.MarkDirty(true, tid)
			bt.file.WritePage(childPage)
			bt.rootPageID = childPID
		}
		return nil
	}

	// Get parent and find siblings
	parentPageID := NewBTreePageID(bt.indexID, page.parentPage)
	parentPage, err := bt.file.ReadPage(tid, parentPageID)
	if err != nil {
		return err
	}

	childIdx := slices.IndexFunc(parentPage.children, func(pp *BTreeChildPtr) bool {
		return pp.ChildPID.Equals(page.pageID)
	})

	if childIdx == -1 {
		return fmt.Errorf("child not found in parent")
	}

	if childIdx > 0 {
		leftSiblingPID := parentPage.children[childIdx-1].ChildPID
		leftSibling, err := bt.file.ReadPage(tid, leftSiblingPID)
		if err == nil && leftSibling.numEntries > maxEntriesPerPage/2 {
			return bt.redistributeFromLeft(tid, leftSibling, page, parentPage, childIdx)
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parentPage.children)-1 {
		rightSiblingPID := parentPage.children[childIdx+1].ChildPID
		rightSibling, err := bt.file.ReadPage(tid, rightSiblingPID)
		if err == nil && rightSibling.numEntries > maxEntriesPerPage/2 {
			return bt.redistributeFromRight(tid, page, rightSibling, parentPage, childIdx)
		}
	}

	// Cannot borrow - must merge
	if childIdx > 0 {
		// Merge with left sibling
		leftSiblingPID := parentPage.children[childIdx-1].ChildPID
		leftSibling, err := bt.file.ReadPage(tid, leftSiblingPID)
		if err == nil {
			return bt.mergeWithLeft(tid, leftSibling, page, parentPage, childIdx)
		}
	}

	// Merge with right sibling
	if childIdx < len(parentPage.children)-1 {
		rightSiblingPID := parentPage.children[childIdx+1].ChildPID
		rightSibling, err := bt.file.ReadPage(tid, rightSiblingPID)
		if err == nil {
			return bt.mergeWithRight(tid, page, rightSibling, parentPage, childIdx)
		}
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
func (bt *BTree) redistributeFromLeft(tid *primitives.TransactionID, left, current, parent *BTreePage, pageIdx int) error {
	if current.IsLeafPage() {
		leftLastIdx := left.numEntries - 1
		deleted := deleteFromPage(left, leftLastIdx, left.entries, tid)

		insertAtBegin(current, current.entries, *deleted, tid)

		parent.children[pageIdx].Key = (*deleted).Key
		parent.MarkDirty(true, tid)

		return bt.writePages(left, current, parent)
	}

	moved := deleteFromPage(left, left.numEntries, left.children, tid)

	ch := newBtreeChildPtr(nil, (*moved).ChildPID)
	if len(current.children) > 0 {
		current.children[0].Key = parent.children[pageIdx].Key
	}
	insertAtEnd(current, current.children, ch, tid)

	parent.children[pageIdx].Key = (*moved).Key
	parent.MarkDirty(true, tid)

	return bt.writePages(left, current, parent)
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
func (bt *BTree) redistributeFromRight(tid *primitives.TransactionID, current, right, parent *BTreePage, pageIdx int) error {
	rightStart := 0

	if current.IsLeafPage() {
		deleted := deleteFromPage(right, rightStart, right.entries, tid)
		insertAtEnd(current, current.entries, *deleted, tid)

		parent.children[pageIdx+1].Key = right.entries[rightStart].Key
		parent.MarkDirty(true, tid)
		return bt.writePages(current, right, parent)
	}

	deleted := deleteFromPage(right, rightStart, right.children, tid)

	ch := newBtreeChildPtr(parent.children[pageIdx+1].Key, (*deleted).ChildPID)
	insertAtEnd(current, current.children, ch, tid)

	if len(right.children) > 0 {
		parent.children[pageIdx+1].Key = right.children[0].Key
		right.children[0].Key = nil
	}
	parent.MarkDirty(true, tid)
	return bt.writePages(current, right, parent)
}

// mergeWithLeft merges the current page into its left sibling.
// This is a convenience wrapper around mergePages that handles left-side merging.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - leftSibling: Left sibling receiving merged entries
//   - current: Current page being merged (will be deallocated)
//   - parent: Parent page whose child pointer will be removed
//   - pageIdx: Index of current page in parent's children array
//
// Returns an error if merge operations fail.
func (bt *BTree) mergeWithLeft(tid *primitives.TransactionID, leftSibling, current, parent *BTreePage, pageIdx int) error {
	return bt.mergePages(tid, leftSibling, current, parent, pageIdx, pageIdx)
}

// mergeWithRight merges the right sibling into the current page.
// This is a convenience wrapper around mergePages that handles right-side merging.
//
// Parameters:
//   - tid: Transaction ID for lock management
//   - page: Current page receiving merged entries
//   - rightSibling: Right sibling being merged (will be deallocated)
//   - parentPage: Parent page whose child pointer will be removed
//   - pageIdx: Index of current page in parent's children array
//
// Returns an error if merge operations fail.
func (bt *BTree) mergeWithRight(tid *primitives.TransactionID, page, rightSibling, parentPage *BTreePage, pageIdx int) error {
	return bt.mergePages(tid, page, rightSibling, parentPage, pageIdx+1, pageIdx+1)
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
func (bt *BTree) mergePages(tid *primitives.TransactionID, left, right, parent *BTreePage, childIdxToDelete int, separatorIdx int) error {
	if left.IsLeafPage() {
		left.entries = append(left.entries, right.entries...)
		left.numEntries += right.numEntries
		left.nextLeaf = right.nextLeaf
		left.MarkDirty(true, tid)

		if right.nextLeaf != -1 {
			nextPageID := NewBTreePageID(bt.indexID, right.nextLeaf)
			nextPage, err := bt.file.ReadPage(tid, nextPageID)
			if err == nil {
				nextPage.prevLeaf = left.pageID.PageNo()
				nextPage.MarkDirty(true, tid)
				bt.file.WritePage(nextPage)
			}
		}

		bt.file.WritePage(left)
	} else {
		separatorKey := parent.children[separatorIdx].Key
		if len(right.children) > 0 {
			right.children[0].Key = separatorKey
		}
		left.children = append(left.children, right.children...)
		left.numEntries += right.numEntries
		left.MarkDirty(true, tid)

		// Update all children from right page to point to left page as their parent
		for _, child := range right.children {
			childPage, err := bt.file.ReadPage(tid, child.ChildPID)
			if err == nil {
				childPage.parentPage = left.pageID.PageNo()
				childPage.MarkDirty(true, tid)
				bt.file.WritePage(childPage)
			}
		}

		bt.file.WritePage(left)
	}

	deleteFromPage(parent, childIdxToDelete, parent.children, tid)
	bt.file.WritePage(parent)

	minEntries := maxEntriesPerPage / 2
	if parent.numEntries < minEntries && parent.parentPage != -1 {
		return bt.handleUnderflow(tid, parent)
	}
	return nil
}

// writePages writes multiple pages to disk in sequence.
// This is a utility function to reduce code duplication when multiple pages
// need to be persisted after a rebalancing operation.
//
// Parameters:
//   - pages: Variable number of pages to write
//
// Returns the first error encountered during writing, or nil if all succeed.
func (bt *BTree) writePages(pages ...*BTreePage) error {
	for _, page := range pages {
		if err := bt.file.WritePage(page); err != nil {
			return err
		}
	}
	return nil
}

// deleteFromPage removes an element at the specified index from a page's slice.
// This generic helper function works with both entries ([]*IndexEntry) and
// children ([]*BTreeChildPtr) slices. It updates the page's internal slice,
// decrements numEntries, and marks the page dirty.
//
// Parameters:
//   - p: Page containing the slice
//   - idx: Index of element to delete
//   - s: Slice containing the element (entries or children)
//   - tid: Transaction ID for marking page dirty
//
// Returns a pointer to the deleted element, or nil if index is invalid.
func deleteFromPage[T any](p *BTreePage, idx int, s []T, tid *primitives.TransactionID) *T {
	if idx < 0 || idx >= len(s) {
		return nil
	}

	deleted := s[idx]
	newSlice := slices.Delete(s, idx, idx+1)

	switch any(newSlice).(type) {
	case []*index.IndexEntry:
		p.entries = any(newSlice).([]*index.IndexEntry)
	case []*BTreeChildPtr:
		p.children = any(newSlice).([]*BTreeChildPtr)
	}

	p.numEntries--
	p.MarkDirty(true, tid)

	return &deleted
}

// insertIntoPage inserts an element at the specified index in a page's slice.
// This generic helper function works with both entries ([]*IndexEntry) and
// children ([]*BTreeChildPtr) slices. It updates the page's internal slice,
// increments numEntries, and marks the page dirty.
//
// Parameters:
//   - p: Page containing the slice
//   - idx: Index where element should be inserted (0 = beginning, len(s) = end)
//   - s: Slice to insert into (entries or children)
//   - elem: Element to insert
//   - tid: Transaction ID for marking page dirty
//
// Returns an error if the index is out of bounds.
func insertIntoPage[T any](p *BTreePage, idx int, s []T, elem T, tid *primitives.TransactionID) error {
	if idx < 0 || idx > len(s) {
		return fmt.Errorf("invalid index %d for inserting element", idx)
	}

	newSlice := slices.Insert(s, idx, elem)

	switch any(newSlice).(type) {
	case []*index.IndexEntry:
		p.entries = any(newSlice).([]*index.IndexEntry)
	case []*BTreeChildPtr:
		p.children = any(newSlice).([]*BTreeChildPtr)
	}

	p.numEntries++
	p.MarkDirty(true, tid)

	return nil
}

// insertAtBegin inserts an element at the beginning of a page's slice.
// This is a convenience wrapper around insertIntoPage.
//
// Parameters:
//   - p: Page containing the slice
//   - s: Slice to insert into (entries or children)
//   - elem: Element to insert at position 0
//   - tid: Transaction ID for marking page dirty
//
// Returns an error if the insertion fails.
func insertAtBegin[T any](p *BTreePage, s []T, elem T, tid *primitives.TransactionID) error {
	return insertIntoPage(p, 0, s, elem, tid)
}

// insertAtEnd appends an element to the end of a page's slice.
// This is a convenience wrapper around insertIntoPage.
//
// Parameters:
//   - p: Page containing the slice
//   - s: Slice to insert into (entries or children)
//   - elem: Element to append at the end
//   - tid: Transaction ID for marking page dirty
//
// Returns an error if the insertion fails.
func insertAtEnd[T any](p *BTreePage, s []T, elem T, tid *primitives.TransactionID) error {
	return insertIntoPage(p, len(s), s, elem, tid)
}
