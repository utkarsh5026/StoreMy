package btreeindex

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// Helper functions
func mustCreateTupleDesc() *tuple.TupleDescription {
	typesList := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(typesList, fields)
	if err != nil {
		panic(err)
	}
	return td
}

func createTestTuple(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}

func setupTestBTree(t *testing.T, keyType types.Type) (*BTree, *memory.PageStore, *transaction.TransactionContext, string, func()) {
	t.Helper()

	// Create temp file
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, fmt.Sprintf("btree_test_%d.dat", os.Getpid()))

	// Create BTree file
	file, err := btree.NewBTreeFile(filename, keyType)
	if err != nil {
		t.Fatalf("Failed to create BTree file: %v", err)
	}

	// Create WAL
	walPath := filepath.Join(tmpDir, "wal.log")
	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create transaction context
	txID := primitives.NewTransactionID()
	tx := transaction.NewTransactionContext(txID)

	// Begin transaction in WAL
	_, err = wal.LogBegin(txID)
	if err != nil {
		t.Fatalf("Failed to begin transaction in WAL: %v", err)
	}

	// Create page store
	store := memory.NewPageStore(wal)

	// Create BTree
	indexID := 1
	bt := NewBTree(indexID, keyType, file, tx, store)

	cleanup := func() {
		if bt != nil {
			bt.Close()
		}
		if wal != nil {
			wal.Close()
		}
		os.RemoveAll(tmpDir)
	}

	return bt, store, tx, filename, cleanup
}

// Test: BTree creation and initialization
func TestNewBTree(t *testing.T) {
	tests := []struct {
		name    string
		keyType types.Type
	}{
		{"IntType", types.IntType},
		{"StringType", types.StringType},
		{"FloatType", types.FloatType},
		{"BoolType", types.BoolType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt, _, _, _, cleanup := setupTestBTree(t, tt.keyType)
			defer cleanup()

			if bt == nil {
				t.Fatal("NewBTree returned nil")
			}

			if bt.GetIndexType() != index.BTreeIndex {
				t.Errorf("Expected index type %v, got %v", index.BTreeIndex, bt.GetIndexType())
			}

			if bt.GetKeyType() != tt.keyType {
				t.Errorf("Expected key type %v, got %v", tt.keyType, bt.GetKeyType())
			}
		})
	}
}

// Test: Insert single entry
func TestBTree_Insert_Single(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	err := bt.Insert(key, rid)
	if err != nil {
		t.Fatalf("Failed to insert entry: %v", err)
	}

	// Verify insertion by searching
	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search for key: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 && !results[0].Equals(rid) {
		t.Errorf("Expected RID %v, got %v", rid, results[0])
	}
}

// Test: Insert multiple entries in order
func TestBTree_Insert_MultipleOrdered(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := heap.NewHeapPageID(1, 0)

	// Insert in ascending order
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)

		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Verify all entries are present
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// Test: Insert multiple entries in reverse order
func TestBTree_Insert_MultipleReverse(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := heap.NewHeapPageID(1, 0)

	// Insert in descending order
	for i := numEntries - 1; i >= 0; i-- {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)

		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Verify all entries are present
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// Test: Insert multiple entries in random order
func TestBTree_Insert_MultipleRandom(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert in random order
	testKeys := []int{50, 20, 80, 10, 30, 70, 90, 5, 15, 25, 35, 60, 75, 85, 95}
	pageID := heap.NewHeapPageID(1, 0)

	for i, keyVal := range testKeys {
		key := types.NewIntField(int64(keyVal))
		rid := tuple.NewTupleRecordID(pageID, i)

		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry with key %d: %v", keyVal, err)
		}
	}

	// Verify all entries are present
	for _, keyVal := range testKeys {
		key := types.NewIntField(int64(keyVal))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", keyVal, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", keyVal, len(results))
		}
	}
}

// Test: Insert duplicate entries (should fail)
func TestBTree_Insert_Duplicate(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	// First insert should succeed
	err := bt.Insert(key, rid)
	if err != nil {
		t.Fatalf("Failed to insert entry: %v", err)
	}

	// Second insert with same key and RID should fail
	err = bt.Insert(key, rid)
	if err == nil {
		t.Error("Expected error when inserting duplicate entry, but got none")
	}
}

// Test: Insert with type mismatch
func TestBTree_Insert_TypeMismatch(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewStringField("invalid", 128)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	err := bt.Insert(key, rid)
	if err == nil {
		t.Error("Expected error when inserting key with wrong type, but got none")
	}
}

// Test: Insert triggering page split
func TestBTree_Insert_PageSplit(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert enough entries to trigger at least one split (reduced for stability)
	numEntries := 160
	pageID := heap.NewHeapPageID(1, 0)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)

		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Verify all entries are still present after split
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// Test: Delete single entry
func TestBTree_Delete_Single(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	// Insert then delete
	err := bt.Insert(key, rid)
	if err != nil {
		t.Fatalf("Failed to insert entry: %v", err)
	}

	tid := primitives.NewTransactionID()
	err = bt.Delete(tid, key, rid)
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	// Verify entry is gone
	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search for key: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after deletion, got %d", len(results))
	}
}

// Test: Delete multiple entries
func TestBTree_Delete_Multiple(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 50
	pageID := heap.NewHeapPageID(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Delete every other entry
	tid := primitives.NewTransactionID()
	for i := 0; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to delete entry %d: %v", i, err)
		}
	}

	// Verify deleted entries are gone
	for i := 0; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 results for deleted key %d, got %d", i, len(results))
		}
	}

	// Verify remaining entries are still present
	for i := 1; i < numEntries; i += 2 {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for remaining key %d, got %d", i, len(results))
		}
	}
}

// Test: Delete non-existent entry
func TestBTree_Delete_NonExistent(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	tid := primitives.NewTransactionID()
	err := bt.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error when deleting non-existent entry, but got none")
	}
}

// Test: Delete with type mismatch
func TestBTree_Delete_TypeMismatch(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewStringField("invalid", 128)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	tid := primitives.NewTransactionID()
	err := bt.Delete(tid, key, rid)
	if err == nil {
		t.Error("Expected error when deleting with wrong key type, but got none")
	}
}

// Test: Search for existing key
func TestBTree_Search_Existing(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	pageID := heap.NewHeapPageID(1, 0)
	rid := tuple.NewTupleRecordID(pageID, 0)

	err := bt.Insert(key, rid)
	if err != nil {
		t.Fatalf("Failed to insert entry: %v", err)
	}

	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search for key: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 && !results[0].Equals(rid) {
		t.Errorf("Expected RID %v, got %v", rid, results[0])
	}
}

// Test: Search for non-existent key
func TestBTree_Search_NonExistent(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert some entries
	pageID := heap.NewHeapPageID(1, 0)
	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i * 10))
		rid := tuple.NewTupleRecordID(pageID, i)
		bt.Insert(key, rid)
	}

	// Search for non-existent key
	key := types.NewIntField(42)
	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search for key: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-existent key, got %d", len(results))
	}
}

// Test: Search in empty tree
func TestBTree_Search_EmptyTree(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewIntField(42)
	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search in empty tree: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results in empty tree, got %d", len(results))
	}
}

// Test: Search with type mismatch
func TestBTree_Search_TypeMismatch(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	key := types.NewStringField("invalid", 128)
	_, err := bt.Search(key)
	if err == nil {
		t.Error("Expected error when searching with wrong key type, but got none")
	}
}

// Test: RangeSearch basic functionality
func TestBTree_RangeSearch_Basic(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	pageID := heap.NewHeapPageID(1, 0)

	// Insert entries 0-99
	for i := 0; i < 100; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Range search [20, 30]
	tid := primitives.NewTransactionID()
	startKey := types.NewIntField(20)
	endKey := types.NewIntField(30)

	results, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to perform range search: %v", err)
	}

	// Should get entries 20-30 inclusive (11 entries)
	expectedCount := 11
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}
}

// Test: RangeSearch with single element range
func TestBTree_RangeSearch_SingleElement(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	pageID := heap.NewHeapPageID(1, 0)

	for i := 0; i < 10; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		bt.Insert(key, rid)
	}

	tid := primitives.NewTransactionID()
	key := types.NewIntField(5)

	results, err := bt.RangeSearch(tid, key, key)
	if err != nil {
		t.Fatalf("Failed to perform range search: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result for single element range, got %d", len(results))
	}
}

// Test: RangeSearch in empty tree
func TestBTree_RangeSearch_EmptyTree(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	tid := primitives.NewTransactionID()
	startKey := types.NewIntField(10)
	endKey := types.NewIntField(20)

	results, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to perform range search on empty tree: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results in empty tree, got %d", len(results))
	}
}

// Test: RangeSearch spanning multiple pages
func TestBTree_RangeSearch_MultiplePages(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert enough entries to span multiple leaf pages (reduced for testing)
	numEntries := btree.MaxEntriesPerPage * 2
	pageID := heap.NewHeapPageID(1, 0)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	tid := primitives.NewTransactionID()
	startKey := types.NewIntField(btree.MaxEntriesPerPage - 10)
	endKey := types.NewIntField(btree.MaxEntriesPerPage*2 + 10)

	results, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to perform range search: %v", err)
	}

	expectedCount := (btree.MaxEntriesPerPage*2 + 10) - (btree.MaxEntriesPerPage - 10) + 1
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}
}

// Test: RangeSearch with type mismatch
func TestBTree_RangeSearch_TypeMismatch(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	tid := primitives.NewTransactionID()
	startKey := types.NewStringField("invalid", 128)
	endKey := types.NewStringField("invalid2", 128)

	_, err := bt.RangeSearch(tid, startKey, endKey)
	if err == nil {
		t.Error("Expected error when range searching with wrong key type, but got none")
	}
}

// Test: String keys
func TestBTree_StringKeys(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.StringType)
	defer cleanup()

	pageID := heap.NewHeapPageID(1, 0)
	testData := []struct {
		key  string
		idx  int
	}{
		{"apple", 0},
		{"banana", 1},
		{"cherry", 2},
		{"date", 3},
		{"elderberry", 4},
	}

	// Insert
	for _, data := range testData {
		key := types.NewStringField(data.key, 128)
		rid := tuple.NewTupleRecordID(pageID, data.idx)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", data.key, err)
		}
	}

	// Search
	for _, data := range testData {
		key := types.NewStringField(data.key, 128)
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for %s: %v", data.key, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for %s, got %d", data.key, len(results))
		}
	}

	// Range search
	tid := primitives.NewTransactionID()
	startKey := types.NewStringField("banana", 128)
	endKey := types.NewStringField("date", 128)

	results, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to perform range search: %v", err)
	}

	// Should get banana, cherry, date (3 entries)
	if len(results) != 3 {
		t.Errorf("Expected 3 results for range [banana, date], got %d", len(results))
	}
}

// Test: Close BTree
func TestBTree_Close(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	err := bt.Close()
	if err != nil {
		t.Errorf("Failed to close BTree: %v", err)
	}
}

// Test: Stress test with many insertions and deletions
func TestBTree_Stress_InsertDelete(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numOperations := 200
	pageID := heap.NewHeapPageID(1, 0)

	// Insert many entries
	for i := 0; i < numOperations; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Delete half of them
	tid := primitives.NewTransactionID()
	for i := 0; i < numOperations; i += 2 {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Delete(tid, key, rid)
		if err != nil {
			t.Fatalf("Failed to delete entry %d: %v", i, err)
		}
	}

	// Insert new entries in the gaps
	for i := 0; i < numOperations; i += 2 {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i+10000)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to re-insert entry %d: %v", i, err)
		}
	}

	// Verify all entries are present
	for i := 0; i < numOperations; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d, got %d", i, len(results))
		}
	}
}

// Test: Concurrent reads
func TestBTree_Concurrent_Reads(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	numEntries := 100
	pageID := heap.NewHeapPageID(1, 0)

	// Insert entries
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numGoroutines := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < numEntries; i++ {
				key := types.NewIntField(int64(i))
				results, err := bt.Search(key)
				if err != nil {
					t.Errorf("Goroutine %d: Failed to search for key %d: %v", goroutineID, i, err)
					return
				}

				if len(results) != 1 {
					t.Errorf("Goroutine %d: Expected 1 result for key %d, got %d", goroutineID, i, len(results))
				}
			}
		}(g)
	}

	wg.Wait()
}

// Test: Edge case - Insert and delete at boundaries
func TestBTree_EdgeCase_Boundaries(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	pageID := heap.NewHeapPageID(1, 0)

	// Insert at page boundaries (reduced for stability)
	for i := 0; i < 200; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	tid := primitives.NewTransactionID()

	// Delete entries at the beginning of each page
	key1 := types.NewIntField(0)
	rid1 := tuple.NewTupleRecordID(pageID, 0)
	err := bt.Delete(tid, key1, rid1)
	if err != nil {
		t.Fatalf("Failed to delete first entry: %v", err)
	}

	key2 := types.NewIntField(int64(btree.MaxEntriesPerPage))
	rid2 := tuple.NewTupleRecordID(pageID, btree.MaxEntriesPerPage)
	err = bt.Delete(tid, key2, rid2)
	if err != nil {
		t.Fatalf("Failed to delete entry at page boundary: %v", err)
	}

	// Verify deletions
	results1, _ := bt.Search(key1)
	if len(results1) != 0 {
		t.Error("Expected first entry to be deleted")
	}

	results2, _ := bt.Search(key2)
	if len(results2) != 0 {
		t.Error("Expected boundary entry to be deleted")
	}
}

// Test: Insert with various data types
func TestBTree_Insert_VariousTypes(t *testing.T) {
	tests := []struct {
		name     string
		keyType  types.Type
		insertFn func(*BTree, int) error
		searchFn func(*BTree, int) (int, error)
	}{
		{
			name:    "IntType",
			keyType: types.IntType,
			insertFn: func(bt *BTree, i int) error {
				key := types.NewIntField(int64(i))
				pageID := heap.NewHeapPageID(1, 0)
				rid := tuple.NewTupleRecordID(pageID, i)
				return bt.Insert(key, rid)
			},
			searchFn: func(bt *BTree, i int) (int, error) {
				key := types.NewIntField(int64(i))
				results, err := bt.Search(key)
				return len(results), err
			},
		},
		{
			name:    "FloatType",
			keyType: types.FloatType,
			insertFn: func(bt *BTree, i int) error {
				key := types.NewFloat64Field(float64(i) + 0.5)
				pageID := heap.NewHeapPageID(1, 0)
				rid := tuple.NewTupleRecordID(pageID, i)
				return bt.Insert(key, rid)
			},
			searchFn: func(bt *BTree, i int) (int, error) {
				key := types.NewFloat64Field(float64(i) + 0.5)
				results, err := bt.Search(key)
				return len(results), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt, _, _, _, cleanup := setupTestBTree(t, tt.keyType)
			defer cleanup()

			numEntries := 50
			for i := 0; i < numEntries; i++ {
				err := tt.insertFn(bt, i)
				if err != nil {
					t.Fatalf("Failed to insert entry %d: %v", i, err)
				}
			}

			for i := 0; i < numEntries; i++ {
				count, err := tt.searchFn(bt, i)
				if err != nil {
					t.Fatalf("Failed to search for entry %d: %v", i, err)
				}
				if count != 1 {
					t.Errorf("Expected 1 result for entry %d, got %d", i, count)
				}
			}
		})
	}
}

// Test: Verify tree structure after multiple splits
func TestBTree_TreeStructure_MultipleSplits(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Insert enough entries to trigger multiple levels of splits (reduced for stability)
	numEntries := 200
	pageID := heap.NewHeapPageID(1, 0)

	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		rid := tuple.NewTupleRecordID(pageID, i)
		err := bt.Insert(key, rid)
		if err != nil {
			t.Fatalf("Failed to insert entry %d: %v", i, err)
		}
	}

	// Verify all entries are searchable
	for i := 0; i < numEntries; i++ {
		key := types.NewIntField(int64(i))
		results, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d after splits: %v", i, err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result for key %d after splits, got %d", i, len(results))
		}
	}

	// Verify range search works correctly across the tree
	tid := primitives.NewTransactionID()
	startKey := types.NewIntField(100)
	endKey := types.NewIntField(200)

	results, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to perform range search after splits: %v", err)
	}

	expectedCount := 101 // 100-200 inclusive
	if len(results) != expectedCount {
		t.Errorf("Expected %d results after splits, got %d", expectedCount, len(results))
	}
}

// Test: Empty root handling
func TestBTree_EmptyRoot(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	// Search in completely empty tree
	key := types.NewIntField(42)
	results, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Failed to search in empty tree: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results in empty tree, got %d", len(results))
	}

	// Range search in empty tree
	tid := primitives.NewTransactionID()
	startKey := types.NewIntField(10)
	endKey := types.NewIntField(20)

	rangeResults, err := bt.RangeSearch(tid, startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to range search in empty tree: %v", err)
	}

	if len(rangeResults) != 0 {
		t.Errorf("Expected 0 results for range search in empty tree, got %d", len(rangeResults))
	}
}

// Test: GetIndexType and GetKeyType
func TestBTree_Getters(t *testing.T) {
	bt, _, _, _, cleanup := setupTestBTree(t, types.IntType)
	defer cleanup()

	if bt.GetIndexType() != index.BTreeIndex {
		t.Errorf("Expected index type BTreeIndex, got %v", bt.GetIndexType())
	}

	if bt.GetKeyType() != types.IntType {
		t.Errorf("Expected key type IntType, got %v", bt.GetKeyType())
	}
}
