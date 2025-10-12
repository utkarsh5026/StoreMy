package btree

import (
	"fmt"
	"slices"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
)

// deleteFromLeaf removes a key-value pair from a leaf page
func (bt *BTree) deleteFromLeaf(tid *primitives.TransactionID, leaf *BTreePage, ie *index.IndexEntry) error {
	deleteIdx := slices.IndexFunc(leaf.entries, func(e *index.IndexEntry) bool {
		return e.Equals(ie)
	})

	if deleteIdx == -1 {
		return fmt.Errorf("entry not found")
	}

	wasFirstKey := (deleteIdx == 0 && leaf.numEntries > 1)

	leaf.entries = append(leaf.entries[:deleteIdx], leaf.entries[deleteIdx+1:]...)
	leaf.numEntries--
	leaf.MarkDirty(true, tid)

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

// handleUnderflow handles page underflow after deletion
func (bt *BTree) handleUnderflow(tid *primitives.TransactionID, page *BTreePage) error {
	if page.parentPage == -1 {
		// Root page - only collapse if it's internal with one child
		if page.IsInternalPage() && page.numEntries == 0 && len(page.children) == 1 {
			// Make the only child the new root
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

	// Find this page's position in parent
	childIdx := -1
	for i, child := range parentPage.children {
		if child.ChildPID.Equals(page.pageID) {
			childIdx = i
			break
		}
	}

	if childIdx == -1 {
		return fmt.Errorf("page not found in parent")
	}

	// Try to borrow from left sibling
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

// redistributeFromLeft borrows an entry from left sibling
func (bt *BTree) redistributeFromLeft(tid *primitives.TransactionID, left, current, parent *BTreePage, pageIdx int) error {
	if current.IsLeafPage() {
		leftLastIdx := left.numEntries - 1
		movedEntry := left.entries[leftLastIdx]
		left.entries = left.entries[:leftLastIdx]
		left.numEntries--
		left.MarkDirty(true, tid)

		current.entries = append([]*index.IndexEntry{movedEntry}, current.entries...)
		current.numEntries++
		current.MarkDirty(true, tid)

		parent.children[pageIdx].Key = movedEntry.Key
		parent.MarkDirty(true, tid)

		bt.file.WritePage(left)
		bt.file.WritePage(current)
		return bt.file.WritePage(parent)
	}

	movedChild := left.children[left.numEntries]
	left.children = left.children[:left.numEntries]
	left.numEntries--
	left.MarkDirty(true, tid)

	newFirstChild := &BTreeChildPtr{Key: nil, ChildPID: movedChild.ChildPID}
	if len(current.children) > 0 {
		current.children[0].Key = parent.children[pageIdx].Key
	}
	current.children = append([]*BTreeChildPtr{newFirstChild}, current.children...)
	current.numEntries++
	current.MarkDirty(true, tid)

	parent.children[pageIdx].Key = movedChild.Key
	parent.MarkDirty(true, tid)

	bt.file.WritePage(left)
	bt.file.WritePage(current)
	return bt.file.WritePage(parent)
}
