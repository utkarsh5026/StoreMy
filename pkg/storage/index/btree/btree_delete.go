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
