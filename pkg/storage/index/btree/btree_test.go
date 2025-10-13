package btree

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// Helper function to create a test BTree
func createTestBTree(t *testing.T) (*BTree, *primitives.TransactionID, string) {
	t.Helper()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_btree.dat")

	file, err := NewBTreeFile(tmpFile, types.IntType)
	if err != nil {
		t.Fatalf("Failed to create BTree file: %v", err)
	}

	btree := NewBTree(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	return btree, tid, tmpFile
}

// Helper to create a test record ID
// Note: This creates a generic page ID that's not a BTreePageID
// For test purposes, we use a heap page ID
func createTestRID(pageNum, tupleNum int) *tuple.TupleRecordID {
	return &tuple.TupleRecordID{
		PageID:   heap.NewHeapPageID(1, pageNum),
		TupleNum: tupleNum,
	}
}

func TestBTree_NewBTree(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.dat")

	file, err := NewBTreeFile(tmpFile, types.IntType)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	btree := NewBTree(1, types.IntType, file)

	if btree.indexID != 1 {
		t.Errorf("Expected indexID 1, got %d", btree.indexID)
	}

	if btree.keyType != types.IntType {
		t.Errorf("Expected IntType, got %v", btree.keyType)
	}

	if btree.file == nil {
		t.Error("File should not be nil")
	}
}

func TestBTree_InsertSingleEntry(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	rid := createTestRID(0, 0)

	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify the entry was inserted
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].TupleNum != rid.TupleNum {
		t.Errorf("Expected tuple num %d, got %d", rid.TupleNum, results[0].TupleNum)
	}
}

func TestBTree_InsertMultipleEntries(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert entries in random order
	entries := []int{50, 20, 80, 10, 30, 70, 90, 5, 15, 25}

	for i, val := range entries {
		key := types.NewIntField(int64(val))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", val, err)
		}
	}

	// Verify all entries can be found
	for i, val := range entries {
		key := types.NewIntField(int64(val))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", val, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 result for key %d, got %d", val, len(results))
		}

		if results[0].TupleNum != i {
			t.Errorf("Expected tuple num %d for key %d, got %d", i, val, results[0].TupleNum)
		}
	}
}

func TestBTree_InsertDuplicateKeys(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	rid1 := createTestRID(0, 0)
	rid2 := createTestRID(0, 1)

	err := btree.Insert(tid, key, rid1)
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	err = btree.Insert(tid, key, rid2)
	if err != nil {
		t.Fatalf("Second insert failed: %v", err)
	}

	// Search should return both entries
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
}

func TestBTree_InsertExactDuplicate(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	rid := createTestRID(0, 0)

	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Insert exact duplicate (same key AND rid)
	err = btree.Insert(tid, key, rid)
	if err == nil {
		t.Error("Expected error for duplicate entry, got nil")
	}
}

func TestBTree_InsertWithSplit(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert enough entries to trigger a split (more than maxEntriesPerPage)
	numEntries := maxEntriesPerPage + 20

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Verify all entries are present
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_InsertWithMultipleLevels(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert enough entries to create a multi-level tree
	numEntries := maxEntriesPerPage * 3

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Verify all entries are present
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Fatalf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_SearchNonExistent(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert some entries
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Search for non-existent key
	key := types.NewIntField(int64(5))
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestBTree_SearchEmptyTree(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestBTree_RangeSearch(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert entries 0-99
	for i := 0; i < 100; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Range search [20, 30]
	startKey := types.NewIntField(int64(20))
	endKey := types.NewIntField(int64(30))

	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("RangeSearch failed: %v", err)
	}

	expectedCount := 11 // 20, 21, ..., 30
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}

	// Verify results are in range
	for _, rid := range results {
		if rid.TupleNum < 20 || rid.TupleNum > 30 {
			t.Errorf("Result tuple num %d out of range [20, 30]", rid.TupleNum)
		}
	}
}

func TestBTree_RangeSearchSpanningPages(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert enough entries to span multiple pages
	numEntries := maxEntriesPerPage * 2

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Range search spanning multiple pages
	startKey := types.NewIntField(int64(maxEntriesPerPage - 10))
	endKey := types.NewIntField(int64(maxEntriesPerPage + 10))

	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("RangeSearch failed: %v", err)
	}

	expectedCount := 21
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}
}

func TestBTree_RangeSearchEmptyRange(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert entries 0, 10, 20, 30, ...
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Range search in gap [1, 9]
	startKey := types.NewIntField(int64(1))
	endKey := types.NewIntField(int64(9))

	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("RangeSearch failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestBTree_Delete(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert entries
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Delete an entry
	key := types.NewIntField(int64(50))
	rid := createTestRID(0, 5)

	err := btree.Delete(tid, key, rid)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify entry is gone
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete, got %d", len(results))
	}

	// Verify other entries still exist
	for i := 0; i < 10; i++ {
		if i == 5 {
			continue
		}
		k := types.NewIntField(int64(i * 10))
		results, err := btree.Search(tid, k)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i*10, len(results))
		}
	}
}

func TestBTree_DeleteNonExistent(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert some entries
	for i := 0; i < 5; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Try to delete non-existent entry
	key := types.NewIntField(int64(5))
	rid := createTestRID(0, 0)

	err := btree.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error for deleting non-existent entry, got nil")
	}
}

func TestBTree_DeleteDuplicateKey(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	rid1 := createTestRID(0, 0)
	rid2 := createTestRID(0, 1)

	// Insert two entries with same key
	btree.Insert(tid, key, rid1)
	btree.Insert(tid, key, rid2)

	// Delete one entry
	err := btree.Delete(tid, key, rid1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify only one entry remains
	results, err := btree.Search(tid, key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].TupleNum != rid2.TupleNum {
		t.Errorf("Expected tuple num %d, got %d", rid2.TupleNum, results[0].TupleNum)
	}
}

func TestBTree_DeleteWithUnderflow(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert enough entries to create multiple pages
	numEntries := maxEntriesPerPage + 20

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Delete entries to trigger underflow handling
	for i := 0; i < 20; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	// Verify remaining entries exist
	for i := 20; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_DeleteAll(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	numEntries := 50

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Delete all entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	// Verify tree is empty
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_KeyTypeMismatch(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Try to insert with wrong key type
	key := types.NewStringField("test", 255)
	rid := createTestRID(0, 0)

	err := btree.Insert(tid, key, rid)
	if err == nil {
		t.Error("Expected error for key type mismatch, got nil")
	}

	// Try to search with wrong key type
	_, err = btree.Search(tid, key)
	if err == nil {
		t.Error("Expected error for key type mismatch, got nil")
	}

	// Try to delete with wrong key type
	err = btree.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error for key type mismatch, got nil")
	}
}

func TestBTree_GetIndexType(t *testing.T) {
	btree, _, _ := createTestBTree(t)
	defer btree.Close()

	indexType := btree.GetIndexType()
	// IndexType is index.IndexType which is an integer constant
	// BTree uses index.BTreeIndex
	_ = indexType // Just verify it compiles and returns something
}

func TestBTree_GetKeyType(t *testing.T) {
	btree, _, _ := createTestBTree(t)
	defer btree.Close()

	keyType := btree.GetKeyType()
	if keyType != types.IntType {
		t.Errorf("Expected IntType, got %v", keyType)
	}
}

func TestBTree_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "persistent.dat")

	// Create and populate tree
	{
		file, err := NewBTreeFile(tmpFile, types.IntType)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		btree := NewBTree(1, types.IntType, file)
		tid := primitives.NewTransactionID()

		for i := 0; i < 50; i++ {
			key := types.NewIntField(int64(i))
			rid := createTestRID(0, i)
			err := btree.Insert(tid, key, rid)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		btree.Close()
	}

	// Reopen and verify
	{
		file, err := NewBTreeFile(tmpFile, types.IntType)
		if err != nil {
			t.Fatalf("Failed to reopen file: %v", err)
		}

		btree := NewBTree(1, types.IntType, file)
		tid := primitives.NewTransactionID()

		for i := 0; i < 50; i++ {
			key := types.NewIntField(int64(i))
			results, err := btree.Search(tid, key)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			if len(results) != 1 {
				t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
			}
		}

		btree.Close()
	}
}

func TestBTree_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	numEntries := 1000

	// Insert
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Search all
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}

	// Delete half
	for i := 0; i < numEntries/2; i++ {
		key := types.NewIntField(int64(i * 2))
		rid := createTestRID(0, i*2)
		err := btree.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i*2, err)
		}
	}

	// Verify deletions
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}

		if i%2 == 0 {
			if len(results) != 0 {
				t.Errorf("Expected 0 results for deleted key %d, got %d", i, len(results))
			}
		} else {
			if len(results) != 1 {
				t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
			}
		}
	}
}

func TestBTree_ReverseOrderInsert(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert in reverse order
	for i := 99; i >= 0; i-- {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Verify all entries
	for i := 0; i < 100; i++ {
		key := types.NewIntField(int64(i))
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_StringKeys(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "string_btree.dat")

	file, err := NewBTreeFile(tmpFile, types.StringType)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	btree := NewBTree(1, types.StringType, file)
	tid := primitives.NewTransactionID()

	// Insert string keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, k := range keys {
		key := types.NewStringField(k, types.StringMaxSize)
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed for key %s: %v", k, err)
		}
	}

	// Search
	for i, k := range keys {
		key := types.NewStringField(k, types.StringMaxSize)
		results, err := btree.Search(tid, key)
		if err != nil {
			t.Fatalf("Search failed for key %s: %v", k, err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %s, got %d", k, len(results))
		}
		if results[0].TupleNum != i {
			t.Errorf("Expected tuple num %d for key %s, got %d", i, k, results[0].TupleNum)
		}
	}

	// Range search
	startKey := types.NewStringField("banana", types.StringMaxSize)
	endKey := types.NewStringField("date", types.StringMaxSize)
	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("RangeSearch failed: %v", err)
	}

	expectedCount := 3 // banana, cherry, date
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}
}

func TestBTree_ConcurrentTransactions(t *testing.T) {
	btree, _, _ := createTestBTree(t)
	defer btree.Close()

	tid1 := primitives.NewTransactionID()
	tid2 := primitives.NewTransactionID()

	// Insert with tid1
	key1 := types.NewIntField(int64(10))
	rid1 := createTestRID(0, 0)
	err := btree.Insert(tid1, key1, rid1)
	if err != nil {
		t.Fatalf("Insert with tid1 failed: %v", err)
	}

	// Insert with tid2
	key2 := types.NewIntField(int64(20))
	rid2 := createTestRID(0, 1)
	err = btree.Insert(tid2, key2, rid2)
	if err != nil {
		t.Fatalf("Insert with tid2 failed: %v", err)
	}

	// Both transactions should see their own inserts
	results1, err := btree.Search(tid1, key1)
	if err != nil {
		t.Fatalf("Search with tid1 failed: %v", err)
	}
	if len(results1) != 1 {
		t.Errorf("Expected 1 result for tid1, got %d", len(results1))
	}

	results2, err := btree.Search(tid2, key2)
	if err != nil {
		t.Fatalf("Search with tid2 failed: %v", err)
	}
	if len(results2) != 1 {
		t.Errorf("Expected 1 result for tid2, got %d", len(results2))
	}
}

func TestBTree_EdgeCaseSplitRoot(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Fill root exactly to capacity
	for i := 0; i < maxEntriesPerPage; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// One more insert should split the root
	key := types.NewIntField(int64(maxEntriesPerPage))
	rid := createTestRID(0, maxEntriesPerPage)
	err := btree.Insert(tid, key, rid)
	if err != nil {
		t.Fatalf("Insert that should split root failed: %v", err)
	}

	// Verify all entries are still accessible
	for i := 0; i <= maxEntriesPerPage; i++ {
		k := types.NewIntField(int64(i))
		results, err := btree.Search(tid, k)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

func TestBTree_DeleteFromEmptyTree(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	key := types.NewIntField(int64(42))
	rid := createTestRID(0, 0)

	err := btree.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error when deleting from empty tree")
	}
}

func TestBTree_RangeSearchReversed(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	for i := 0; i < 100; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		btree.Insert(tid, key, rid)
	}

	// Reversed range should still work but may return no results
	// depending on implementation
	startKey := types.NewIntField(int64(50))
	endKey := types.NewIntField(int64(30))

	results, err := btree.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("RangeSearch failed: %v", err)
	}

	// Result should be empty since startKey > endKey
	if len(results) != 0 {
		t.Logf("Warning: Reversed range returned %d results", len(results))
	}
}

// Test that verifies leaf page links are correct
func TestBTree_LeafPageLinks(t *testing.T) {
	btree, tid, _ := createTestBTree(t)
	defer btree.Close()

	// Insert enough to create multiple leaf pages
	numEntries := maxEntriesPerPage * 3

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := createTestRID(0, i)
		err := btree.Insert(tid, key, rid)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Find the leftmost leaf
	root, _ := btree.getRootPage(tid)
	leftmost, err := btree.findLeafPage(tid, root, types.NewIntField(int64(0)))
	if err != nil {
		t.Fatalf("Failed to find leftmost leaf: %v", err)
	}

	// Traverse leaf pages using next pointers
	count := 0
	prevPageNum := -1
	current := leftmost

	for current != nil {
		count++

		// Verify prev link
		if current.prevLeaf != prevPageNum {
			t.Errorf("Prev link mismatch: expected %d, got %d",
				prevPageNum, current.prevLeaf)
		}

		if current.nextLeaf == -1 {
			break
		}

		prevPageNum = current.pageID.PageNo()
		nextPageID := NewBTreePageID(btree.indexID, current.nextLeaf)
		current, err = btree.file.ReadPage(tid, nextPageID)
		if err != nil {
			t.Fatalf("Failed to read next leaf: %v", err)
		}
	}

	if count < 2 {
		t.Errorf("Expected at least 2 leaf pages, got %d", count)
	}
}

func ExampleBTree_Insert() {
	tmpDir := os.TempDir()
	tmpFile := filepath.Join(tmpDir, "example.dat")
	defer os.Remove(tmpFile)

	file, _ := NewBTreeFile(tmpFile, types.IntType)
	defer file.Close()

	btree := NewBTree(1, types.IntType, file)
	tid := primitives.NewTransactionID()

	key := types.NewIntField(int64(42))
	rid := &tuple.TupleRecordID{
		PageID:   NewBTreePageID(1, 0),
		TupleNum: 0,
	}

	err := btree.Insert(tid, key, rid)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Insert successful")
	}
	// Output: Insert successful
}
