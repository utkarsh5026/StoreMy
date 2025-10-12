package btree

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// BTreeFileIterator iterates over all entries in a B+Tree file
type BTreeFileIterator struct {
	file        *BTreeFile
	tid         *primitives.TransactionID
	currentPage int
	currentPos  int
	currentLeaf *BTreePage
}

// Open initializes the iterator
func (it *BTreeFileIterator) Open() error {
	// Find the first leaf page (leftmost)
	// For now, assume page 0 is the root
	if it.file.NumPages() == 0 {
		return nil
	}

	rootPageID := NewBTreePageID(it.file.GetID(), 0)
	rootPage, err := it.file.ReadPage(it.tid, rootPageID)
	if err != nil {
		return fmt.Errorf("failed to read root page: %w", err)
	}

	// Navigate to leftmost leaf
	currentPage := rootPage
	for !currentPage.IsLeafPage() {
		if len(currentPage.children) == 0 {
			return fmt.Errorf("internal node has no children")
		}
		childPID := currentPage.children[0].ChildPID
		currentPage, err = it.file.ReadPage(it.tid, childPID)
		if err != nil {
			return fmt.Errorf("failed to read child page: %w", err)
		}
	}

	it.currentLeaf = currentPage
	it.currentPos = 0

	return nil
}

// HasNext checks if there are more entries
func (it *BTreeFileIterator) HasNext() (bool, error) {
	if it.currentLeaf == nil {
		return false, nil
	}

	if it.currentPos < len(it.currentLeaf.entries) {
		return true, nil
	}

	// Check if there's a next leaf
	return it.currentLeaf.nextLeaf != -1, nil
}

// Next returns the next entry (as a tuple containing the key)
func (it *BTreeFileIterator) Next() (*tuple.Tuple, error) {
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, fmt.Errorf("no more entries")
	}

	// If we've exhausted current leaf, move to next
	if it.currentPos >= len(it.currentLeaf.entries) {
		if it.currentLeaf.nextLeaf == -1 {
			return nil, fmt.Errorf("no more entries")
		}

		nextPageID := NewBTreePageID(it.file.GetID(), it.currentLeaf.nextLeaf)
		nextLeaf, err := it.file.ReadPage(it.tid, nextPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf: %w", err)
		}

		it.currentLeaf = nextLeaf
		it.currentPos = 0
	}

	entry := it.currentLeaf.entries[it.currentPos]
	it.currentPos++

	// Create a tuple with just the key (for index scans)
	// In a real implementation, you might want to return the full tuple from the heap file
	td, _ := tuple.NewTupleDesc(
		[]types.Type{entry.Key.Type()},
		[]string{"key"},
	)
	tup := tuple.NewTuple(td)
	tup.SetField(0, entry.Key)
	tup.RecordID = entry.RID

	return tup, nil
}

// Close closes the iterator
func (it *BTreeFileIterator) Close() error {
	it.currentLeaf = nil
	return nil
}

// Rewind resets the iterator to the beginning
func (it *BTreeFileIterator) Rewind() error {
	it.currentPos = 0
	return it.Open()
}
