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
		return fmt.Errorf("page not found in parent")
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

// redistributeFromLeft borrows an entry from left sibling
func (bt *BTree) redistributeFromLeft(tid *primitives.TransactionID, left, current, parent *BTreePage, pageIdx int) error {
	if current.IsLeafPage() {
		return bt.redistributeLeafFromLeft(tid, left, current, parent, pageIdx)
	}

	return bt.redistributeInternalFromLeft(tid, left, current, parent, pageIdx)
}

func (bt *BTree) redistributeLeafFromLeft(tid *primitives.TransactionID, left, current, parent *BTreePage, pageIdx int) error {
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

	return bt.writePages(left, current, parent)
}

func (bt *BTree) redistributeInternalFromLeft(tid *primitives.TransactionID, left, current, parent *BTreePage, pageIdx int) error {
	last := left.numEntries
	moved := left.children[last]
	left.children = left.children[:last]
	left.numEntries--
	left.MarkDirty(true, tid)

	ch := newBtreeChildPtr(nil, moved.ChildPID)
	if len(current.children) > 0 {
		current.children[0].Key = parent.children[pageIdx].Key
	}
	current.children = append([]*BTreeChildPtr{ch}, current.children...)
	current.numEntries++
	current.MarkDirty(true, tid)

	parent.children[pageIdx].Key = moved.Key
	parent.MarkDirty(true, tid)

	return bt.writePages(left, current, parent)
}

// redistributeFromRight borrows an entry from right sibling
func (bt *BTree) redistributeFromRight(tid *primitives.TransactionID, current, right, parent *BTreePage, pageIdx int) error {
	rightStart := 0

	if current.IsLeafPage() {
		movedEntry := right.entries[rightStart]
		right.entries = right.entries[rightStart+1:]
		right.numEntries--
		right.MarkDirty(true, tid)

		current.entries = append(current.entries, movedEntry)
		current.numEntries++
		current.MarkDirty(true, tid)

		parent.children[pageIdx+1].Key = right.entries[rightStart].Key
		parent.MarkDirty(true, tid)

		bt.file.WritePage(current)
		bt.file.WritePage(right)
		return bt.file.WritePage(parent)
	}

	movedChild := right.children[rightStart]
	right.children = right.children[rightStart+1:]
	right.numEntries--
	right.MarkDirty(true, tid)

	newChild := newBtreeChildPtr(parent.children[pageIdx+1].Key, movedChild.ChildPID)
	current.children = append(current.children, newChild)
	current.numEntries++
	current.MarkDirty(true, tid)

	if len(right.children) > 0 {
		parent.children[pageIdx+1].Key = right.children[0].Key
		right.children[0].Key = nil
	}
	parent.MarkDirty(true, tid)

	bt.file.WritePage(current)
	bt.file.WritePage(right)
	return bt.file.WritePage(parent)
}

// mergeWithLeft merges current page with left sibling
func (bt *BTree) mergeWithLeft(tid *primitives.TransactionID, leftSibling, page, parentPage *BTreePage, pageIdx int) error {
	if page.IsLeafPage() {
		leftSibling.entries = append(leftSibling.entries, page.entries...)
		leftSibling.numEntries += page.numEntries
		leftSibling.nextLeaf = page.nextLeaf
		leftSibling.MarkDirty(true, tid)

		// Update next leaf's prev pointer
		if page.nextLeaf != -1 {
			nextPageID := NewBTreePageID(bt.indexID, page.nextLeaf)
			nextPage, err := bt.file.ReadPage(tid, nextPageID)
			if err == nil {
				nextPage.prevLeaf = leftSibling.pageID.PageNo()
				nextPage.MarkDirty(true, tid)
				bt.file.WritePage(nextPage)
			}
		}

		bt.file.WritePage(leftSibling)
	} else {
		separatorKey := parentPage.children[pageIdx].Key
		if len(page.children) > 0 {
			page.children[0].Key = separatorKey
		}
		leftSibling.children = append(leftSibling.children, page.children...)
		leftSibling.numEntries += page.numEntries
		leftSibling.MarkDirty(true, tid)
		bt.file.WritePage(leftSibling)
	}

	// Remove pointer to merged page from parent
	parentPage.children = append(parentPage.children[:pageIdx], parentPage.children[pageIdx+1:]...)
	parentPage.numEntries--
	parentPage.MarkDirty(true, tid)
	bt.file.WritePage(parentPage)

	// Check parent for underflow
	minEntries := maxEntriesPerPage / 2
	if parentPage.numEntries < minEntries && parentPage.parentPage != -1 {
		return bt.handleUnderflow(tid, parentPage)
	}

	return nil
}

func (bt *BTree) mergeWithRight(tid *primitives.TransactionID, page, rightSibling, parentPage *BTreePage, pageIdx int) error {
	if page.IsLeafPage() {
		page.entries = append(page.entries, rightSibling.entries...)
		page.numEntries += rightSibling.numEntries
		page.nextLeaf = rightSibling.nextLeaf
		page.MarkDirty(true, tid)

		if rightSibling.nextLeaf != -1 {
			nextPageID := NewBTreePageID(bt.indexID, rightSibling.nextLeaf)
			nextPage, err := bt.file.ReadPage(tid, nextPageID)
			if err == nil {
				nextPage.prevLeaf = page.pageID.PageNo()
				nextPage.MarkDirty(true, tid)
				bt.file.WritePage(nextPage)
			}
		}

		bt.file.WritePage(page)
	} else {
		// Internal page merge - include separator from parent
		separatorKey := parentPage.children[pageIdx+1].Key
		if len(rightSibling.children) > 0 {
			rightSibling.children[0].Key = separatorKey
		}
		page.children = append(page.children, rightSibling.children...)
		page.numEntries += rightSibling.numEntries
		page.MarkDirty(true, tid)
		bt.file.WritePage(page)
	}

	// Remove pointer to merged page from parent
	parentPage.children = append(parentPage.children[:pageIdx+1], parentPage.children[pageIdx+2:]...)
	parentPage.numEntries--
	parentPage.MarkDirty(true, tid)
	bt.file.WritePage(parentPage)

	// Check parent for underflow
	minEntries := maxEntriesPerPage / 2
	if parentPage.numEntries < minEntries && parentPage.parentPage != -1 {
		return bt.handleUnderflow(tid, parentPage)
	}

	return nil
}

func (bt *BTree) writePages(pages ...*BTreePage) error {
	for _, page := range pages {
		if err := bt.file.WritePage(page); err != nil {
			return err
		}
	}
	return nil
}
