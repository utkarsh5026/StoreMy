package scanner

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// ============================================================================
// TEST HELPERS AND MOCKS
// ============================================================================

// mockIndex implements the index.Index interface for testing
type mockIndex struct {
	indexType   index.IndexType
	keyType     types.Type
	entries     map[string][]*tuple.TupleRecordID // key -> list of RIDs
	searchError error
	rangeError  error
	insertError error
	deleteError error
	isClosed    bool
}

func newMockIndex(indexType index.IndexType, keyType types.Type) *mockIndex {
	return &mockIndex{
		indexType: indexType,
		keyType:   keyType,
		entries:   make(map[string][]*tuple.TupleRecordID),
	}
}

func (m *mockIndex) Insert(key types.Field, rid index.RecID) error {
	if m.insertError != nil {
		return m.insertError
	}
	keyStr := key.String()
	m.entries[keyStr] = append(m.entries[keyStr], rid)
	return nil
}

func (m *mockIndex) Delete(key types.Field, rid index.RecID) error {
	if m.deleteError != nil {
		return m.deleteError
	}
	keyStr := key.String()
	rids := m.entries[keyStr]
	for i, r := range rids {
		if r.PageID.Equals(rid.PageID) && r.TupleNum == rid.TupleNum {
			m.entries[keyStr] = append(rids[:i], rids[i+1:]...)
			break
		}
	}
	return nil
}

func (m *mockIndex) Search(key types.Field) ([]index.RecID, error) {
	if m.searchError != nil {
		return nil, m.searchError
	}
	keyStr := key.String()
	return m.entries[keyStr], nil
}

func (m *mockIndex) RangeSearch(startKey, endKey types.Field) ([]index.RecID, error) {
	if m.rangeError != nil {
		return nil, m.rangeError
	}

	// Simple range search implementation for testing
	var results []index.RecID

	// For IntField, do numeric comparison
	if startInt, ok := startKey.(*types.IntField); ok {
		endInt := endKey.(*types.IntField)
		for keyStr, rids := range m.entries {
			// Parse the key string back to int (this is simplified for testing)
			var keyVal int64
			fmt.Sscanf(keyStr, "%d", &keyVal)
			if keyVal >= startInt.Value && keyVal <= endInt.Value {
				results = append(results, rids...)
			}
		}
	}

	return results, nil
}

func (m *mockIndex) GetIndexType() index.IndexType {
	return m.indexType
}

func (m *mockIndex) GetKeyType() types.Type {
	return m.keyType
}

func (m *mockIndex) Close() error {
	m.isClosed = true
	return nil
}

// Helper functions for creating test data
func createIndexScanTestTupleDesc() *tuple.TupleDescription {
	fieldTypes := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(fieldTypes, fields)
	if err != nil {
		panic(fmt.Sprintf("Failed to create TupleDescription: %v", err))
	}
	return td
}

func createIndexScanTestTuple(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}

func createTestPageStore(t *testing.T) (*memory.PageStore, func()) {
	t.Helper()
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")
	wal, err := wal.NewWAL(walPath, 4096)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	store := memory.NewPageStore(wal)
	cleanup := func() {
		store.Close()
		wal.Close()
	}
	return store, cleanup
}

func createTestHeapFileWithData(t *testing.T, td *tuple.TupleDescription, tuples []*tuple.Tuple) (*heap.HeapFile, func()) {
	t.Helper()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test_heap.dat")

	hf, err := heap.NewHeapFile(primitives.Filepath(filePath), td)
	if err != nil {
		t.Fatalf("Failed to create HeapFile: %v", err)
	}

	// Keep track of pages and how many tuples each has
	pages := make(map[int]*heap.HeapPage)
	tupleCounts := make(map[int]int)

	// Add tuples to the heap file
	for i, tup := range tuples {
		pageNum := i / 10 // Distribute tuples across pages
		pageID := page.NewPageDescriptor(hf.GetID(), primitives.PageNumber(pageNum))

		// Read or create page
		var heapPage *heap.HeapPage
		if existingPage, ok := pages[pageNum]; ok {
			heapPage = existingPage
		} else {
			pg, err := hf.ReadPage(pageID)
			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if pg == nil {
				pageData := make([]byte, page.PageSize)
				heapPage, err = heap.NewHeapPage(pageID, pageData, td)
				if err != nil {
					t.Fatalf("Failed to create HeapPage: %v", err)
				}
			} else {
				heapPage = pg.(*heap.HeapPage)
			}
			pages[pageNum] = heapPage
			tupleCounts[pageNum] = 0
		}

		// Add tuple - the AddTuple method sets the RecordID automatically
		err = heapPage.AddTuple(tup)
		if err != nil {
			t.Fatalf("Failed to add tuple: %v", err)
		}

		tupleCounts[pageNum]++
	}

	// Write all pages back
	for _, heapPage := range pages {
		err = hf.WritePage(heapPage)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	cleanup := func() {
		hf.Close()
		os.RemoveAll(tempDir)
	}

	return hf, cleanup
}

// ============================================================================
// CONSTRUCTOR TESTS
// ============================================================================

func TestNewIndexEqualityScan_ValidInputs(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	searchKey := types.NewIntField(1)

	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan returned error: %v", err)
	}

	if scan == nil {
		t.Fatal("NewIndexEqualityScan returned nil")
	}

	if scan.scanType != EqualityScan {
		t.Errorf("Expected scan type EqualityScan, got %v", scan.scanType)
	}

	if scan.searchKey != searchKey {
		t.Errorf("Expected search key %v, got %v", searchKey, scan.searchKey)
	}
}

func TestNewIndexEqualityScan_NilStore(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	searchKey := types.NewIntField(1)

	scan, err := NewIndexEqualityScan(tx, idx, hf, nil, searchKey)
	if err == nil {
		t.Error("Expected error when store is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when store is nil")
	}
}

func TestNewIndexEqualityScan_NilIndex(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	searchKey := types.NewIntField(1)

	scan, err := NewIndexEqualityScan(tx, nil, hf, store, searchKey)
	if err == nil {
		t.Error("Expected error when index is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when index is nil")
	}
}

func TestNewIndexEqualityScan_NilHeapFile(t *testing.T) {
	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	searchKey := types.NewIntField(1)

	scan, err := NewIndexEqualityScan(tx, idx, nil, store, searchKey)
	if err == nil {
		t.Error("Expected error when heap file is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when heap file is nil")
	}
}

func TestNewIndexRangeScan_ValidInputs(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)

	scan, err := NewIndexRangeScan(tx, idx, hf, store, startKey, endKey)
	if err != nil {
		t.Fatalf("NewIndexRangeScan returned error: %v", err)
	}

	if scan == nil {
		t.Fatal("NewIndexRangeScan returned nil")
	}

	if scan.scanType != RangeScan {
		t.Errorf("Expected scan type RangeScan, got %v", scan.scanType)
	}

	if scan.startKey != startKey {
		t.Errorf("Expected start key %v, got %v", startKey, scan.startKey)
	}

	if scan.endKey != endKey {
		t.Errorf("Expected end key %v, got %v", endKey, scan.endKey)
	}
}

func TestNewIndexRangeScan_NilStore(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)

	scan, err := NewIndexRangeScan(tx, idx, hf, nil, startKey, endKey)
	if err == nil {
		t.Error("Expected error when store is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when store is nil")
	}
}

func TestNewIndexRangeScan_NilIndex(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)

	scan, err := NewIndexRangeScan(tx, nil, hf, store, startKey, endKey)
	if err == nil {
		t.Error("Expected error when index is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when index is nil")
	}
}

func TestNewIndexRangeScan_NilHeapFile(t *testing.T) {
	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)

	scan, err := NewIndexRangeScan(tx, idx, nil, store, startKey, endKey)
	if err == nil {
		t.Error("Expected error when heap file is nil")
	}
	if scan != nil {
		t.Error("Expected nil scan when heap file is nil")
	}
}

// ============================================================================
// EQUALITY SCAN TESTS
// ============================================================================

func TestIndexEqualityScan_SingleMatch(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
		createIndexScanTestTuple(td, 2, "Bob"),
		createIndexScanTestTuple(td, 3, "Charlie"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Search for id = 2
	searchKey := types.NewIntField(2)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tup)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 {
		idField, _ := results[0].GetField(0)
		if idField.(*types.IntField).Value != 2 {
			t.Errorf("Expected id 2, got %d", idField.(*types.IntField).Value)
		}
		nameField, _ := results[0].GetField(1)
		if nameField.(*types.StringField).Value != "Bob" {
			t.Errorf("Expected name 'Bob', got '%s'", nameField.(*types.StringField).Value)
		}
	}
}

func TestIndexEqualityScan_MultipleMatches(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 5, "Alice"),
		createIndexScanTestTuple(td, 5, "Bob"),
		createIndexScanTestTuple(td, 5, "Charlie"),
		createIndexScanTestTuple(td, 3, "David"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Search for id = 5 (should return 3 tuples)
	searchKey := types.NewIntField(5)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tup)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Verify all results have id = 5
	for i, res := range results {
		idField, _ := res.GetField(0)
		if idField.(*types.IntField).Value != 5 {
			t.Errorf("Result %d: expected id 5, got %d", i, idField.(*types.IntField).Value)
		}
	}
}

func TestIndexEqualityScan_NoMatches(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
		createIndexScanTestTuple(td, 2, "Bob"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Search for id = 99 (no matches)
	searchKey := types.NewIntField(99)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	hasNext, err := scan.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false when no matches")
	}
}

// ============================================================================
// RANGE SCAN TESTS
// ============================================================================

func TestIndexRangeScan_MultipleMatches(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
		createIndexScanTestTuple(td, 3, "Bob"),
		createIndexScanTestTuple(td, 5, "Charlie"),
		createIndexScanTestTuple(td, 7, "David"),
		createIndexScanTestTuple(td, 9, "Eve"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Range search: 3 <= id <= 7 (should return Bob, Charlie, David)
	startKey := types.NewIntField(3)
	endKey := types.NewIntField(7)
	scan, err := NewIndexRangeScan(tx, idx, hf, store, startKey, endKey)
	if err != nil {
		t.Fatalf("NewIndexRangeScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tup)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Verify all results are in range [3, 7]
	for i, res := range results {
		idField, _ := res.GetField(0)
		idVal := idField.(*types.IntField).Value
		if idVal < 3 || idVal > 7 {
			t.Errorf("Result %d: id %d is out of range [3, 7]", i, idVal)
		}
	}
}

func TestIndexRangeScan_NoMatches(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
		createIndexScanTestTuple(td, 2, "Bob"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Range search: 10 <= id <= 20 (no matches)
	startKey := types.NewIntField(10)
	endKey := types.NewIntField(20)
	scan, err := NewIndexRangeScan(tx, idx, hf, store, startKey, endKey)
	if err != nil {
		t.Fatalf("NewIndexRangeScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	hasNext, err := scan.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false when no matches in range")
	}
}

func TestIndexRangeScan_AllMatch(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 5, "Alice"),
		createIndexScanTestTuple(td, 6, "Bob"),
		createIndexScanTestTuple(td, 7, "Charlie"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	// Add entries to the mock index
	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	// Range search: 1 <= id <= 10 (all match)
	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)
	scan, err := NewIndexRangeScan(tx, idx, hf, store, startKey, endKey)
	if err != nil {
		t.Fatalf("NewIndexRangeScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	var results []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		results = append(results, tup)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

func TestIndexScan_SearchError(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	idx.searchError = fmt.Errorf("mock search error")
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err == nil {
		t.Error("Expected error when index search fails")
	}
}

func TestIndexScan_RangeSearchError(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.BTreeIndex, types.IntType)
	idx.rangeError = fmt.Errorf("mock range search error")
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	startKey := types.NewIntField(1)
	endKey := types.NewIntField(10)
	scan, err := NewIndexRangeScan(tx, idx, hf, store, startKey, endKey)
	if err != nil {
		t.Fatalf("NewIndexRangeScan failed: %v", err)
	}

	err = scan.Open()
	if err == nil {
		t.Error("Expected error when index range search fails")
	}
}

func TestIndexScan_NotOpened(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	// Try to use scan without opening
	_, err = scan.HasNext()
	if err == nil {
		t.Error("Expected error when calling HasNext on unopened scan")
	}

	_, err = scan.Next()
	if err == nil {
		t.Error("Expected error when calling Next on unopened scan")
	}
}

// ============================================================================
// LIFECYCLE TESTS
// ============================================================================

func TestIndexScan_OpenClose(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}

	err = scan.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify scan is properly closed
	if scan.resultRIDs != nil {
		t.Error("Expected resultRIDs to be nil after close")
	}
	if scan.currentPos != 0 {
		t.Error("Expected currentPos to be 0 after close")
	}
}

func TestIndexScan_Rewind(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 5, "Alice"),
		createIndexScanTestTuple(td, 5, "Bob"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	searchKey := types.NewIntField(5)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	// First iteration
	var firstRun []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		firstRun = append(firstRun, tup)
	}

	if len(firstRun) != 2 {
		t.Errorf("Expected 2 tuples in first run, got %d", len(firstRun))
	}

	// Rewind
	err = scan.Rewind()
	if err != nil {
		t.Errorf("Rewind failed: %v", err)
	}

	// Second iteration
	var secondRun []*tuple.Tuple
	for {
		hasNext, err := scan.HasNext()
		if err != nil {
			t.Errorf("HasNext returned error: %v", err)
			break
		}
		if !hasNext {
			break
		}

		tup, err := scan.Next()
		if err != nil {
			t.Errorf("Next returned error: %v", err)
			break
		}
		secondRun = append(secondRun, tup)
	}

	if len(secondRun) != 2 {
		t.Errorf("Expected 2 tuples in second run, got %d", len(secondRun))
	}

	// Verify results are identical
	if len(firstRun) == len(secondRun) {
		for i := 0; i < len(firstRun); i++ {
			if firstRun[i] != secondRun[i] {
				t.Errorf("Tuple %d differs between runs", i)
			}
		}
	}
}

func TestIndexScan_GetTupleDesc(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{createIndexScanTestTuple(td, 1, "Alice")}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	resultTD := scan.GetTupleDesc()
	if !resultTD.Equals(td) {
		t.Error("GetTupleDesc returned incorrect tuple description")
	}
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

func TestIndexScan_EmptyIndex(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())
	// Don't insert any entries into the index

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer scan.Close()

	hasNext, err := scan.HasNext()
	if err != nil {
		t.Errorf("HasNext returned error: %v", err)
	}
	if hasNext {
		t.Error("Expected HasNext to return false for empty index")
	}
}

func TestIndexScan_MultipleOpens(t *testing.T) {
	td := createIndexScanTestTupleDesc()
	tuples := []*tuple.Tuple{
		createIndexScanTestTuple(td, 1, "Alice"),
	}
	hf, cleanup := createTestHeapFileWithData(t, td, tuples)
	defer cleanup()

	store, storeCleanup := createTestPageStore(t)
	defer storeCleanup()
	idx := newMockIndex(index.HashIndex, types.IntType)
	tx := transaction.NewTransactionContext(primitives.NewTransactionID())

	for _, tup := range tuples {
		idField, _ := tup.GetField(0)
		idx.Insert(idField, tup.RecordID)
	}

	searchKey := types.NewIntField(1)
	scan, err := NewIndexEqualityScan(tx, idx, hf, store, searchKey)
	if err != nil {
		t.Fatalf("NewIndexEqualityScan failed: %v", err)
	}

	// Open multiple times
	err = scan.Open()
	if err != nil {
		t.Errorf("First Open failed: %v", err)
	}

	err = scan.Close()
	if err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	err = scan.Open()
	if err != nil {
		t.Errorf("Second Open failed: %v", err)
	}

	err = scan.Close()
	if err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}
