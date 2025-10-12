package btree

import (
	"os"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func setupTestBTree(t *testing.T) (*BTreeIndexFile, func()) {
	tmpDir, err := os.MkdirTemp("", "btree_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	metadata := &index.IndexMetadata{
		IndexID:    1,
		IndexName:  "test_index",
		TableID:    1,
		FieldIndex: 0,
		FieldName:  "id",
		KeyType:    types.IntType,
		IndexType:  index.BTreeIndex,
		IsUnique:   true,
		IsPrimary:  true,
		FilePath:   filepath.Join(tmpDir, "test_index.dat"),
	}

	btreeFile, err := NewBTreeIndexFile(metadata)
	if err != nil {
		t.Fatalf("failed to create B+Tree: %v", err)
	}

	cleanup := func() {
		btreeFile.Close()
		os.RemoveAll(tmpDir)
	}

	return btreeFile, cleanup
}

func TestBTreeBasicOperations(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	key := types.NewIntField(42)
	rid := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestBTreeInsertMultiple(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert multiple entries in order
	testData := []int{10, 20, 5, 15, 25, 30, 1, 12}

	for i, val := range testData {
		key := types.NewIntField(int64(val))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, 0),
			TupleNum: i,
		}

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", val, err)
		}
	}

	// Verify all entries can be found
	for i, val := range testData {
		key := types.NewIntField(int64(val))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("failed to search key %d: %v", val, err)
		}

		if len(results) != 1 {
			t.Errorf("expected 1 result for key %d, got %d", val, len(results))
			continue
		}

		if results[0].TupleNum != i {
			t.Errorf("wrong tuple number for key %d: expected %d, got %d", val, i, results[0].TupleNum)
		}
	}
}

func TestBTreeDuplicateInsert(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	key := types.NewIntField(10)
	rid := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	// First insert should succeed
	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// Duplicate insert should fail
	err = btree.Insert(tid, key, rid)
	if err == nil {
		t.Fatal("expected error for duplicate insert, got nil")
	}
}

func TestBTreeInsertSameKeyDifferentRID(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	key := types.NewIntField(10)

	// Insert multiple entries with same key but different RIDs
	rids := []*tuple.TupleRecordID{
		{PageID: heap.NewHeapPageID(1, 0), TupleNum: 0},
		{PageID: heap.NewHeapPageID(1, 0), TupleNum: 1},
		{PageID: heap.NewHeapPageID(1, 0), TupleNum: 2},
	}

	for _, rid := range rids {
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search should return all entries
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != len(rids) {
		t.Errorf("expected %d results, got %d", len(rids), len(results))
	}
}

func TestBTreeDelete(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert entries
	key := types.NewIntField(10)
	rid := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Verify inserted
	results, err := btree.Search(tid, key)
	if err != nil || len(results) != 1 {
		t.Fatalf("search after insert failed")
	}

	// Delete entry
	err = btree.Delete(tid, key, rid)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deleted
	results, err = btree.Search(tid, key)
	if err != nil {
		t.Fatalf("search after delete failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

func TestBTreeDeleteNonExistent(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	key := types.NewIntField(10)
	rid := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}

	// Try to delete non-existent entry
	err := btree.Delete(tid, key, rid)
	if err == nil {
		t.Fatal("expected error for deleting non-existent entry, got nil")
	}
}

func TestBTreeLargeInsert(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert many entries to trigger splits
	numEntries := 200

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Verify all entries can be found
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("failed to search key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert entries
	numEntries := 50
	rids := make([]*tuple.TupleRecordID, numEntries)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}
		rids[i] = rid

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Delete entries sequentially
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		err := btree.Delete(tid, key, rids[i])
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", i, err)
		}

		// Verify deleted
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("search failed after deleting key %d: %v", i, err)
		}
		if len(results) != 0 {
			t.Errorf("key %d still found after deletion", i)
		}
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert entries
	numEntries := 50
	rids := make([]*tuple.TupleRecordID, numEntries)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}
		rids[i] = rid

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Delete in reverse order
	for i := numEntries - 1; i >= 0; i-- {
		key := types.NewIntField(int64(i))
		err := btree.Delete(tid, key, rids[i])
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", i, err)
		}

		// Verify remaining keys still exist
		for j := 0; j < i; j++ {
			searchKey := types.NewIntField(int64(j))
			results, err := btree.Search(tid, searchKey)
			if err != nil || len(results) == 0 {
				t.Errorf("key %d not found after deleting key %d", j, i)
			}
		}
	}
}

func TestBTreeRandomDelete(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert entries
	numEntries := 100
	rids := make([]*tuple.TupleRecordID, numEntries)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}
		rids[i] = rid

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Delete every other entry
	for i := 0; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		err := btree.Delete(tid, key, rids[i])
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted entries are gone
	for i := 0; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("search failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("key %d still found after deletion", i)
		}
	}

	// Verify remaining entries still exist
	for i := 1; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil || len(results) == 0 {
			t.Errorf("key %d not found but should exist", i)
		}
	}
}

func TestBTreeRangeSearch(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert entries
	for i := 0; i < 100; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Test range search [20, 30]
	startKey := types.NewIntField(20)
	endKey := types.NewIntField(30)

	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	expectedCount := 11 // 20 to 30 inclusive
	if len(results) != expectedCount {
		t.Errorf("expected %d results, got %d", expectedCount, len(results))
	}
}

func TestBTreeInsertAfterDelete(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert, delete, and re-insert
	key := types.NewIntField(10)
	rid1 := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 0,
	}
	rid2 := &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, 0),
		TupleNum: 1,
	}

	// Insert
	err := btree.Insert(tid, key, rid1)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	// Delete
	err = btree.Delete(tid, key, rid1)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Re-insert with different RID
	err = btree.Insert(tid, key, rid2)
	if err != nil {
		t.Fatalf("second insert failed: %v", err)
	}

	// Verify
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].TupleNum != rid2.TupleNum {
		t.Errorf("expected tuple number %d, got %d", rid2.TupleNum, results[0].TupleNum)
	}
}

func TestBTreeMixedOperations(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Insert some entries
	for i := 0; i < 50; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Delete some
	for i := 10; i < 20; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}

		err := btree.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to delete key %d: %v", i, err)
		}
	}

	// Insert more
	for i := 50; i < 75; i++ {
		key := types.NewIntField(int64(i))
		rid := &tuple.TupleRecordID{
			PageID:   heap.NewHeapPageID(1, i/100),
			TupleNum: i % 100,
		}

		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("failed to insert key %d: %v", i, err)
		}
	}

	// Verify entries 0-9 exist
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil || len(results) == 0 {
			t.Errorf("key %d not found but should exist", i)
		}
	}

	// Verify entries 10-19 don't exist
	for i := 10; i < 20; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("search failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("key %d found but should be deleted", i)
		}
	}

	// Verify entries 20-74 exist
	for i := 20; i < 75; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil || len(results) == 0 {
			t.Errorf("key %d not found but should exist", i)
		}
	}
}

func TestBTreeEmptyTree(t *testing.T) {
	btree, cleanup := setupTestBTree(t)
	defer cleanup()

	tid := &primitives.TransactionID{}

	// Search in empty tree
	key := types.NewIntField(10)
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("search in empty tree failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results in empty tree, got %d", len(results))
	}

	// Range search in empty tree
	startKey := types.NewIntField(10)
	endKey := types.NewIntField(20)
	results, err = btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("range search in empty tree failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results in empty tree, got %d", len(results))
	}
}
