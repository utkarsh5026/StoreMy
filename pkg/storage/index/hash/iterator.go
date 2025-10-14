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
		it.currentPage = nil
		return nil
	}

	it.currentBucket = 0
	it.currentPos = 0

	for it.currentBucket < it.file.numBuckets {
		page, err := it.file.GetBucketPage(it.tid, it.currentBucket)
		if err != nil {
			return fmt.Errorf("failed to read bucket page: %w", err)
		}

		if len(page.entries) > 0 {
			it.currentPage = page
			return nil
		}

		it.currentBucket++
	}

	it.currentPage = nil
	return nil
}

// HasNext checks if there are more entries
func (it *HashFileIterator) HasNext() (bool, error) {
	if it.currentPage == nil {
		return false, nil
	}

	if it.currentPos < len(it.currentPage.entries) {
		return true, nil
	}

	return it.advanceToNextEntry()
}

// advanceToNextEntry moves the iterator to the next entry if possible
// Returns true if a next entry exists, false otherwise
func (it *HashFileIterator) advanceToNextEntry() (bool, error) {
	visitedPages := make(map[int]bool)
	maxPages := it.file.NumPages() + it.file.numBuckets + 10 // Safety limit
	pagesChecked := 0

	for {
		pagesChecked++
		if pagesChecked > maxPages {
			return false, fmt.Errorf("iterator safety limit exceeded")
		}
		// Try overflow page first
		if it.currentPage.overflowPage != -1 {
			overflowPageNum := it.currentPage.overflowPage

			// Check for circular references
			if visitedPages[overflowPageNum] {
				// Circular reference detected, move to next bucket instead
				it.currentBucket++
				if it.currentBucket >= it.file.numBuckets {
					it.currentPage = nil
					return false, nil
				}

				nextPage, err := it.file.GetBucketPage(it.tid, it.currentBucket)
				if err != nil {
					return false, fmt.Errorf("failed to read next bucket: %w", err)
				}
				it.currentPage = nextPage
				it.currentPos = 0
				visitedPages = make(map[int]bool) // Reset visited pages for new bucket

				if len(it.currentPage.entries) > 0 {
					return true, nil
				}
				continue
			}
			visitedPages[overflowPageNum] = true

			pageID := NewHashPageID(it.file.GetID(), overflowPageNum)
			nextPage, err := it.file.ReadPage(pageID)
			if err != nil {
				return false, fmt.Errorf("failed to read overflow page: %w", err)
			}
			it.currentPage = nextPage
			it.currentPos = 0

			if len(it.currentPage.entries) > 0 {
				return true, nil
			}
		} else {
			it.currentBucket++
			if it.currentBucket >= it.file.numBuckets {
				it.currentPage = nil
				return false, nil
			}

			nextPage, err := it.file.GetBucketPage(it.tid, it.currentBucket)
			if err != nil {
				return false, fmt.Errorf("failed to read next bucket: %w", err)
			}
			it.currentPage = nextPage
			it.currentPos = 0
			visitedPages = make(map[int]bool)

			if len(it.currentPage.entries) > 0 {
				return true, nil
			}
		}
	}
}

// Next returns the next entry
func (it *HashFileIterator) Next() (*tuple.Tuple, error) {
	if it.currentPage == nil {
		return nil, fmt.Errorf("no more entries")
	}

	if it.currentPos >= len(it.currentPage.entries) {
		hasNext, err := it.advanceToNextEntry()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return nil, fmt.Errorf("no more entries")
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
