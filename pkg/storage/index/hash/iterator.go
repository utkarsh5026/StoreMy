package hash

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// HashFileIterator iterates over all entries in a hash index file
type HashFileIterator struct {
	file                      *HashFile
	tid                       *primitives.TransactionID
	currentBucket, currentPos int
	currentPage               *HashPage
}

// Open initializes the iterator
func (it *HashFileIterator) Open() error {
	if it.file.NumPages() == 0 {
		return nil
	}

	it.currentBucket = 0
	page, err := it.file.GetBucketPage(it.tid, 0)
	if err != nil {
		return fmt.Errorf("failed to read first bucket page: %w", err)
	}

	it.currentPage = page
	it.currentPos = 0
	return nil
}

// HasNext checks if there are more entries
func (it *HashFileIterator) HasNext() (bool, error) {
	if it.currentPage == nil {
		return false, nil
	}

	// Check current page
	if it.currentPos < len(it.currentPage.entries) {
		return true, nil
	}

	// Check overflow pages
	if it.currentPage.overflowPage != -1 {
		return true, nil
	}

	// Check if there are more buckets to explore
	if it.currentBucket+1 < it.file.numBuckets {
		// Check if next buckets have entries
		return true, nil
	}

	return false, nil
}

// Next returns the next entry
func (it *HashFileIterator) Next() (*tuple.Tuple, error) {
	hasNext, err := it.HasNext()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return nil, fmt.Errorf("no more entries")
	}

	for it.currentPos >= len(it.currentPage.entries) {
		if it.currentPage.overflowPage != -1 {
			pageID := NewHashPageID(it.file.indexID, it.currentPage.overflowPage)
			nextPage, err := it.file.ReadPage(it.tid, pageID)
			if err != nil {
				return nil, fmt.Errorf("failed to read overflow page: %w", err)
			}
			it.currentPage = nextPage
			it.currentPos = 0
		} else {
			it.currentBucket++
			if it.currentBucket >= it.file.numBuckets {
				return nil, fmt.Errorf("no more entries")
			}
			nextPage, err := it.file.GetBucketPage(it.tid, it.currentBucket)
			if err != nil {
				return nil, fmt.Errorf("failed to read next bucket: %w", err)
			}
			it.currentPage = nextPage
			it.currentPos = 0
		}
	}

	if it.currentPos >= len(it.currentPage.entries) {
		return nil, fmt.Errorf("no more entries in page")
	}

	entry := it.currentPage.entries[it.currentPos]
	it.currentPos++

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
func (it *HashFileIterator) Close() error {
	it.currentPage = nil
	return nil
}

// Rewind resets the iterator to the beginning
func (it *HashFileIterator) Rewind() error {
	it.currentPos = 0
	it.currentBucket = 0
	return it.Open()
}
