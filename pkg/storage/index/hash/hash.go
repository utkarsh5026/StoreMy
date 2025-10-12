package hash

import (
	"fmt"
	"hash/fnv"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type TID = *primitives.TransactionID
type RecID = *tuple.TupleRecordID
type Field = types.Field

// HashIndex implements the Index interface for hash-based indexes
type HashIndex struct {
	indexID, numBuckets int
	keyType             types.Type
	file                *HashFile
}

// NewHashIndex creates a new hash index
func NewHashIndex(indexID int, keyType types.Type, file *HashFile) *HashIndex {
	return &HashIndex{
		indexID:    indexID,
		keyType:    keyType,
		file:       file,
		numBuckets: file.GetNumBuckets(),
	}
}

// Insert adds a key-value pair to the hash index
func (hi *HashIndex) Insert(tid TID, key Field, rid RecID) error {
	if err := hi.validateKeyType(key); err != nil {
		return err
	}

	bucketNum := hi.hashKey(key)
	bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	entry := NewHashEntry(key, rid)

	currentPage := bucketPage
	for currentPage.IsFull() {
		if currentPage.GetOverflowPage() == -1 {
			currentPage, err = hi.createAndLinkOverflowPage(tid, bucketNum, currentPage)
			if err != nil {
				return fmt.Errorf("failed to create and link overflow page: %w", err)
			}
			break
		}
		currentPage, err = hi.readOverflowPage(tid, currentPage)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	if err := currentPage.AddEntry(entry); err != nil {
		return fmt.Errorf("failed to add entry: %w", err)
	}

	return hi.markAndWritePage(currentPage, tid)
}

func (hi *HashIndex) createAndLinkOverflowPage(tid TID, bucketNum int, parentPage *HashPage) (*HashPage, error) {
	overflowPage, err := hi.file.AllocatePage(tid, bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate overflow page: %w", err)
	}

	// Link parent to new overflow
	parentPage.SetOverflowPage(overflowPage.pageID.PageNo())
	if err := hi.markAndWritePage(parentPage, tid); err != nil {
		return nil, fmt.Errorf("failed to write parent page with overflow link: %w", err)
	}

	return overflowPage, nil
}

// readOverflowPage safely reads an overflow page with validation.
func (hi *HashIndex) readOverflowPage(tid TID, parentPage *HashPage) (*HashPage, error) {
	overflowPageNum := parentPage.GetOverflowPage()

	// Validate overflow page number
	if overflowPageNum >= hi.file.NumPages() {
		return nil, fmt.Errorf("invalid overflow page number %d (max: %d)",
			overflowPageNum, hi.file.NumPages())
	}

	overflowPageID := NewHashPageID(hi.file.GetID(), overflowPageNum)
	page, err := hi.file.ReadPage(tid, overflowPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read overflow page %d: %w", overflowPageNum, err)
	}

	return page, nil
}

// markAndWritePage marks a page as dirty and writes it to disk.
func (hi *HashIndex) markAndWritePage(page *HashPage, tid TID) error {
	page.MarkDirty(true, tid)
	if err := hi.file.WritePage(page); err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}
	return nil
}

// Delete removes a key-value pair from the hash index
func (hi *HashIndex) Delete(tid TID, key Field, rid RecID) error {
	if key.Type() != hi.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", hi.keyType, key.Type())
	}

	bucketNum := hi.hashKey(key)

	bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
	if err != nil {
		return fmt.Errorf("failed to get bucket page: %w", err)
	}

	currentPage := bucketPage
	visitedPages := make(map[int]bool)
	maxPages := 1000

	for currentPage != nil && len(visitedPages) < maxPages {
		pageNum := currentPage.pageID.PageNo()
		if visitedPages[pageNum] {
			break
		}
		visitedPages[pageNum] = true

		err := currentPage.RemoveEntry(NewHashEntry(key, rid))
		if err == nil {
			currentPage.MarkDirty(true, tid)
			return hi.file.WritePage(currentPage)
		}

		if currentPage.GetOverflowPage() == -1 {
			break
		}

		overflowPageNum := currentPage.GetOverflowPage()
		if overflowPageNum >= hi.file.NumPages() {
			break
		}

		overflowPageID := NewHashPageID(hi.file.GetID(), overflowPageNum)
		currentPage, err = hi.file.ReadPage(tid, overflowPageID)
		if err != nil {
			return fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	return fmt.Errorf("entry not found")
}

// Search finds all tuple locations for a given key
func (hi *HashIndex) Search(tid TID, key Field) ([]RecID, error) {
	if key.Type() != hi.keyType {
		return nil, fmt.Errorf("key type mismatch: expected %v, got %v", hi.keyType, key.Type())
	}

	// Hash the key to get bucket number
	bucketNum := hi.hashKey(key)

	// Get the bucket page
	bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket page: %w", err)
	}

	var results []*tuple.TupleRecordID

	// Search through bucket and overflow pages
	currentPage := bucketPage
	visitedPages := make(map[int]bool)
	maxPages := 1000 // Safety limit to prevent infinite loops

	for currentPage != nil && len(visitedPages) < maxPages {
		pageNum := currentPage.pageID.PageNo()

		// Detect infinite loop
		if visitedPages[pageNum] {
			break
		}
		visitedPages[pageNum] = true

		// Find entries in current page
		entries := currentPage.FindEntries(key)
		results = append(results, entries...)

		// Check overflow page
		if currentPage.GetOverflowPage() == -1 {
			break
		}

		overflowPageNum := currentPage.GetOverflowPage()
		// Check if overflow page exists in file
		if overflowPageNum >= hi.file.NumPages() {
			break
		}

		overflowPageID := NewHashPageID(hi.file.GetID(), overflowPageNum)
		currentPage, err = hi.file.ReadPage(tid, overflowPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to read overflow page: %w", err)
		}
	}

	return results, nil
}

// RangeSearch finds all tuples where key is in [startKey, endKey]
// Note: Hash indexes do not efficiently support range queries
// This implementation scans all buckets (inefficient)
func (hi *HashIndex) RangeSearch(tid TID, startKey, endKey Field) ([]RecID, error) {
	if startKey.Type() != hi.keyType || endKey.Type() != hi.keyType {
		return nil, fmt.Errorf("key type mismatch")
	}

	var results []RecID
	for bucketNum := 0; bucketNum < hi.numBuckets; bucketNum++ {
		if bucketNum >= hi.file.NumPages() {
			continue
		}

		bucketPage, err := hi.file.GetBucketPage(tid, bucketNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get bucket page %d: %w", bucketNum, err)
		}

		if bucketPage.GetNumEntries() == 0 {
			continue
		}

		currentPage := bucketPage
		visitedPages := make(map[int]bool)
		maxPages := 1000

		for currentPage != nil && len(visitedPages) < maxPages {
			pageNum := currentPage.pageID.PageNo()
			if visitedPages[pageNum] {
				break
			}
			visitedPages[pageNum] = true

			for _, entry := range currentPage.GetEntries() {
				geStart, _ := entry.Key.Compare(primitives.GreaterThanOrEqual, startKey)
				leEnd, _ := entry.Key.Compare(primitives.LessThanOrEqual, endKey)

				if geStart && leEnd {
					results = append(results, entry.RID)
				}
			}

			if currentPage.GetOverflowPage() == -1 {
				break
			}

			overflowPageNum := currentPage.GetOverflowPage()
			if overflowPageNum >= hi.file.NumPages() {
				break
			}

			overflowPageID := NewHashPageID(hi.file.GetID(), overflowPageNum)
			currentPage, err = hi.file.ReadPage(tid, overflowPageID)
			if err != nil {
				return nil, fmt.Errorf("failed to read overflow page: %w", err)
			}
		}
	}

	return results, nil
}

// GetIndexType returns HashIndex
func (hi *HashIndex) GetIndexType() index.IndexType {
	return index.HashIndex
}

// GetKeyType returns the type of keys this index handles
func (hi *HashIndex) GetKeyType() types.Type {
	return hi.keyType
}

// Close releases resources held by the index
func (hi *HashIndex) Close() error {
	return hi.file.Close()
}

// hashKey computes the hash of a key and returns the bucket number
func (hi *HashIndex) hashKey(key types.Field) int {
	h := fnv.New32a()

	switch key.Type() {
	case types.IntType:
		intField := key.(*types.IntField)
		value := intField.Value
		bytes := make([]byte, 8)
		for i := range 8 {
			bytes[i] = byte(value >> (8 * i))
		}
		h.Write(bytes)

	case types.StringType:
		strField := key.(*types.StringField)
		h.Write([]byte(strField.Value))

	case types.BoolType:
		boolField := key.(*types.BoolField)
		if boolField.Value {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}

	case types.FloatType:
		floatField := key.(*types.Float64Field)
		value := floatField.Value
		bytes := make([]byte, 8)
		bits := uint64(value)
		for i := range 8 {
			bytes[i] = byte(bits >> (8 * i))
		}
		h.Write(bytes)
	}

	return int(h.Sum32()) % hi.numBuckets
}

func (hi *HashIndex) validateKeyType(key Field) error {
	if key.Type() != hi.keyType {
		return fmt.Errorf("key type mismatch: expected %v, got %v", hi.keyType, key.Type())
	}
	return nil
}
