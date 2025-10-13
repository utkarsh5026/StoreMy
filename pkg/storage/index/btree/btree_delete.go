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
		leftLastIdx := left.numEntries - 1
		deleted := deleteFromPage(left, leftLastIdx, left.entries, tid)

		insertAtBegin(current, current.entries, *deleted, tid)

		parent.children[pageIdx].Key = (*deleted).Key
		parent.MarkDirty(true, tid)

		return bt.writePages(left, current, parent)
	}

	last := left.numEntries
	moved := deleteFromPage(left, last, left.children, tid)

	ch := newBtreeChildPtr(nil, (*moved).ChildPID)
	if len(current.children) > 0 {
		current.children[0].Key = parent.children[pageIdx].Key
	}
	insertAtEnd(current, current.children, ch, tid)

	parent.children[pageIdx].Key = (*moved).Key
	parent.MarkDirty(true, tid)

	return bt.writePages(left, current, parent)
}

// redistributeFromRight borrows an entry from right sibling
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
	return bt.writePages(parent, current, parent)
}

// mergeWithLeft merges current page with left sibling
func (bt *BTree) mergeWithLeft(tid *primitives.TransactionID, leftSibling, current, parent *BTreePage, pageIdx int) error {
	return bt.mergePages(tid, leftSibling, current, parent, pageIdx, pageIdx)
}

// mergeWithRight merges current page with right sibling
func (bt *BTree) mergeWithRight(tid *primitives.TransactionID, page, rightSibling, parentPage *BTreePage, pageIdx int) error {
	return bt.mergePages(tid, page, rightSibling, parentPage, pageIdx, pageIdx+1)
}

// mergePages merges right page into left page
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

func (bt *BTree) writePages(pages ...*BTreePage) error {
	for _, page := range pages {
		if err := bt.file.WritePage(page); err != nil {
			return err
		}
	}
	return nil
}

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

// insertIntoPage inserts an element at the specified index (0 = beginning, len = end)
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

func insertAtBegin[T any](p *BTreePage, s []T, elem T, tid *primitives.TransactionID) error {
	return insertIntoPage(p, 0, s, elem, tid)
}

func insertAtEnd[T any](p *BTreePage, s []T, elem T, tid *primitives.TransactionID) error {
	return insertIntoPage(p, len(s), s, elem, tid)
}
