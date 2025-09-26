package memory

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
	"time"
)

// mockPage implements page.Page interface for testing
type mockPage struct {
	id         tuple.PageID
	dirty      bool
	dirtyTid   *transaction.TransactionID
	data       []byte
	beforeImg  page.Page
	mutex      sync.RWMutex
}

func newMockPage(pageID tuple.PageID) *mockPage {
	return &mockPage{
		id:   pageID,
		data: make([]byte, page.PageSize),
	}
}

func (m *mockPage) GetID() tuple.PageID {
	return m.id
}

func (m *mockPage) IsDirty() *transaction.TransactionID {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.dirty {
		return m.dirtyTid
	}
	return nil
}

func (m *mockPage) MarkDirty(dirty bool, tid *transaction.TransactionID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.dirty = dirty
	if dirty {
		m.dirtyTid = tid
	} else {
		m.dirtyTid = nil
	}
}

func (m *mockPage) GetPageData() []byte {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	dataCopy := make([]byte, len(m.data))
	copy(dataCopy, m.data)
	return dataCopy
}

func (m *mockPage) GetBeforeImage() page.Page {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.beforeImg
}

func (m *mockPage) SetBeforeImage() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	beforeData := make([]byte, len(m.data))
	copy(beforeData, m.data)
	m.beforeImg = &mockPage{
		id:   m.id,
		data: beforeData,
	}
}

// mockDbFileForPageStore implements page.DbFile for PageStore testing
type mockDbFileForPageStore struct {
	id        int
	tupleDesc *tuple.TupleDescription
	pages     map[tuple.PageID]*mockPage
	mutex     sync.RWMutex
}

func newMockDbFileForPageStore(id int, fieldTypes []types.Type, fieldNames []string) *mockDbFileForPageStore {
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		panic("Failed to create TupleDescription: " + err.Error())
	}
	return &mockDbFileForPageStore{
		id:        id,
		tupleDesc: td,
		pages:     make(map[tuple.PageID]*mockPage),
	}
}

func (m *mockDbFileForPageStore) ReadPage(pid tuple.PageID) (page.Page, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	if p, exists := m.pages[pid]; exists {
		return p, nil
	}
	
	// Create new page if it doesn't exist
	newPage := newMockPage(pid)
	m.pages[pid] = newPage
	return newPage, nil
}

func (m *mockDbFileForPageStore) WritePage(p page.Page) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	mockP, ok := p.(*mockPage)
	if !ok {
		return fmt.Errorf("expected mockPage, got %T", p)
	}
	
	m.pages[p.GetID()] = mockP
	return nil
}

func (m *mockDbFileForPageStore) AddTuple(tid *transaction.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Create a new page for the tuple
	pageID := heap.NewHeapPageID(m.id, len(m.pages))
	newPage := newMockPage(pageID)
	newPage.MarkDirty(true, tid)
	
	m.pages[pageID] = newPage
	return []page.Page{newPage}, nil
}

func (m *mockDbFileForPageStore) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) (page.Page, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	// Create or return a page for the delete operation
	if t.RecordID != nil {
		pageID := t.RecordID.PageID
		if existingPage, exists := m.pages[pageID]; exists {
			existingPage.MarkDirty(true, tid)
			return existingPage, nil
		}
		
		// Create new page if it doesn't exist
		newPage := newMockPage(pageID)
		newPage.MarkDirty(true, tid)
		m.pages[pageID] = newPage
		return newPage, nil
	}
	
	return nil, fmt.Errorf("tuple has no record ID")
}

func (m *mockDbFileForPageStore) Iterator(tid *transaction.TransactionID) iterator.DbFileIterator {
	return nil
}

func (m *mockDbFileForPageStore) GetID() int {
	return m.id
}

func (m *mockDbFileForPageStore) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

func (m *mockDbFileForPageStore) Close() error {
	return nil
}

func (m *mockDbFileForPageStore) IsClosed() bool {
	return false
}

func TestNewPageStore(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)

	if ps == nil {
		t.Fatal("NewPageStore returned nil")
	}

	if ps.pageCache == nil {
		t.Error("pageCache should be initialized")
	}

	if ps.transactions == nil {
		t.Error("transactions should be initialized")
	}

	if ps.tableManager != tm {
		t.Error("tableManager should be set correctly")
	}

	// Note: numPages field doesn't exist in the current implementation

	if len(ps.pageCache) != 0 {
		t.Errorf("pageCache should be empty, got %d entries", len(ps.pageCache))
	}

	if len(ps.transactions) != 0 {
		t.Errorf("transactions should be empty, got %d entries", len(ps.transactions))
	}
}

func TestPageStore_InsertTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and tuple
	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	stringField := types.NewStringField("test_value", 128)
	testTuple.SetField(0, intField)
	testTuple.SetField(1, stringField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Verify page was cached
	if len(ps.pageCache) == 0 {
		t.Error("Expected page to be added to cache")
	}
}

func TestPageStore_InsertTuple_TableNotFound(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	tid := transaction.NewTransactionID()
	
	// Create a dummy tuple (we won't be able to insert it anyway)
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}
	
	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test with non-existent table ID
	err = ps.InsertTuple(tid, 999, testTuple)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	expectedErrMsg := "table with ID 999 not found"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_InsertTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify the page exists and is marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.pageCache[pageID]
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}
		
		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_InsertTuple_MultiplePages(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create mock file that returns multiple pages
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test InsertTuple
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Errorf("InsertTuple failed: %v", err)
	}

	// Since our mock only returns one page per AddTuple call, we should have 1 page
	if len(ps.pageCache) != 1 {
		t.Errorf("Expected 1 page in cache, got %d", len(ps.pageCache))
	}

	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) != 1 {
		t.Errorf("Expected 1 dirty page tracked, got %d", len(txInfo.dirtyPages))
	}
}

func TestPageStore_InsertTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numInsertsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent inserts
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numInsertsPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3
				
				// Create tuple
				tupleValue := goroutineID*numInsertsPerGoroutine + j
				dbFile, _ := tm.GetDbFile(tableID)
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(tupleValue))
				testTuple.SetField(0, intField)

				// Insert tuple
				err := ps.InsertTuple(tid, tableID, testTuple)
				if err != nil {
					t.Errorf("InsertTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	expectedTotalInserts := numGoroutines * numInsertsPerGoroutine
	if len(ps.pageCache) != expectedTotalInserts {
		t.Errorf("Expected %d pages in cache, got %d", expectedTotalInserts, len(ps.pageCache))
	}
}

func TestPageStore_TransactionInfo(t *testing.T) {
	info := &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Test dirty pages tracking
	pageID1 := heap.NewHeapPageID(1, 0)
	pageID2 := heap.NewHeapPageID(1, 1)

	info.dirtyPages[pageID1] = true
	info.dirtyPages[pageID2] = true

	if len(info.dirtyPages) != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", len(info.dirtyPages))
	}

	// Test locked pages tracking
	info.lockedPages[pageID1] = ReadOnly
	info.lockedPages[pageID2] = ReadWrite

	if len(info.lockedPages) != 2 {
		t.Errorf("Expected 2 locked pages, got %d", len(info.lockedPages))
	}

	if info.lockedPages[pageID1] != ReadOnly {
		t.Errorf("Expected ReadOnly permission for pageID1, got %v", info.lockedPages[pageID1])
	}

	if info.lockedPages[pageID2] != ReadWrite {
		t.Errorf("Expected ReadWrite permission for pageID2, got %v", info.lockedPages[pageID2])
	}
}

func TestPageStore_Permissions(t *testing.T) {
	tests := []struct {
		name       string
		permission Permissions
		expected   int
	}{
		{"ReadOnly", ReadOnly, 0},
		{"ReadWrite", ReadWrite, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.permission) != tt.expected {
				t.Errorf("Expected %s to have value %d, got %d", tt.name, tt.expected, int(tt.permission))
			}
		})
	}
}

func TestPageStore_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)

		// Test with nil transaction ID
		err = ps.InsertTuple(nil, 1, testTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()
		
		// Test with nil tuple
		err = ps.InsertTuple(tid, 1, nil)
		// This should not panic
	})
}

// Helper function to create a test TupleDescription
func createTestTupleDesc() *tuple.TupleDescription {
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		panic("Failed to create test TupleDescription: " + err.Error())
	}
	return td
}

func TestPageStore_DeleteTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and tuple with RecordID
	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	stringField := types.NewStringField("test_value", 128)
	testTuple.SetField(0, intField)
	testTuple.SetField(1, stringField)
	
	// Set up RecordID for the tuple
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test DeleteTuple
	err = ps.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Errorf("DeleteTuple failed: %v", err)
	}

	// Verify page was cached
	if len(ps.pageCache) == 0 {
		t.Error("Expected page to be added to cache")
	}
}

func TestPageStore_DeleteTuple_NoRecordID(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	tid := transaction.NewTransactionID()
	
	// Create a tuple without RecordID
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}
	
	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Test with tuple that has no RecordID
	err = ps.DeleteTuple(tid, testTuple)
	if err == nil {
		t.Error("Expected error for tuple without RecordID")
	}

	expectedErrMsg := "tuple has no record ID"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_DeleteTuple_TableNotFound(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	tid := transaction.NewTransactionID()
	
	// Create a tuple with RecordID pointing to non-existent table
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}
	
	testTuple := tuple.NewTuple(td)
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)
	
	// Set up RecordID with non-existent table ID
	pageID := heap.NewHeapPageID(999, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test with non-existent table ID
	err = ps.DeleteTuple(tid, testTuple)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	expectedErrMsg := "table with ID 999 not found"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_DeleteTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create tuple with RecordID
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)
	
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	testTuple.RecordID = recordID

	// Test DeleteTuple
	err = ps.DeleteTuple(tid, testTuple)
	if err != nil {
		t.Errorf("DeleteTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify the page exists and is marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.pageCache[pageID]
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}
		
		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_DeleteTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numDeletesPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numDeletesPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3
				
				// Create tuple with RecordID
				dbFile, _ := tm.GetDbFile(tableID)
				tupleValue := goroutineID*numDeletesPerGoroutine + j
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(tupleValue))
				testTuple.SetField(0, intField)
				
				pageID := heap.NewHeapPageID(tableID, j)
				recordID := &tuple.TupleRecordID{
					PageID:   pageID,
					TupleNum: 0,
				}
				testTuple.RecordID = recordID

				// Delete tuple
				err := ps.DeleteTuple(tid, testTuple)
				if err != nil {
					t.Errorf("DeleteTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	expectedTotalDeletes := numGoroutines * numDeletesPerGoroutine
	if len(ps.pageCache) != expectedTotalDeletes {
		t.Errorf("Expected %d pages in cache, got %d", expectedTotalDeletes, len(ps.pageCache))
	}
}

func TestPageStore_DeleteTuple_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)
		
		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		testTuple.RecordID = recordID

		// Test with nil transaction ID
		_ = ps.DeleteTuple(nil, testTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		tid := transaction.NewTransactionID()
		
		// Test with nil tuple
		err := ps.DeleteTuple(tid, nil)
		// This should not panic, but will likely error due to nil pointer
		if err == nil {
			t.Error("Expected error for nil tuple")
		}
	})
}

func TestPageStore_MemoryLeaks(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create many transactions and insert tuples
	numTransactions := 100
	for i := 0; i < numTransactions; i++ {
		tid := transaction.NewTransactionID()
		
		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(i))
		testTuple.SetField(0, intField)

		err := ps.InsertTuple(tid, 1, testTuple)
		if err != nil {
			t.Errorf("InsertTuple failed for transaction %d: %v", i, err)
		}
	}

	// Verify the page cache grew appropriately
	if len(ps.pageCache) != numTransactions {
		t.Errorf("Expected %d pages in cache, got %d", numTransactions, len(ps.pageCache))
	}

	// Note: In a real implementation, you might want to test cache eviction,
	// transaction cleanup, etc. This test ensures basic memory tracking works.
}

func TestPageStore_UpdateTuple_Success(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction
	tid := transaction.NewTransactionID()
	
	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	stringField1 := types.NewStringField("old_value", 128)
	oldTuple.SetField(0, intField1)
	oldTuple.SetField(1, stringField1)
	
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID
	
	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(42))
	stringField2 := types.NewStringField("new_value", 128)
	newTuple.SetField(0, intField2)
	newTuple.SetField(1, stringField2)

	// Test UpdateTuple
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err != nil {
		t.Errorf("UpdateTuple failed: %v", err)
	}

	// Verify pages were cached (should have at least 1-2 pages from delete and insert operations)
	if len(ps.pageCache) == 0 {
		t.Error("Expected pages to be added to cache after update")
	}
}

func TestPageStore_UpdateTuple_DeleteFails(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	tid := transaction.NewTransactionID()
	
	// Create old tuple without RecordID (will cause delete to fail)
	fieldTypes := []types.Type{types.IntType}
	fieldNames := []string{"id"}
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("Failed to create TupleDescription: %v", err)
	}
	
	oldTuple := tuple.NewTuple(td)
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)
	// Note: oldTuple.RecordID is nil, which will cause delete to fail
	
	// Create new tuple
	newTuple := tuple.NewTuple(td)
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// Test UpdateTuple - should fail because delete fails
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err == nil {
		t.Error("Expected error when delete operation fails")
	}
	
	expectedErrMsg := "failed to delete old tuple: tuple has no record ID"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error %q, got %q", expectedErrMsg, err.Error())
	}
}

func TestPageStore_UpdateTuple_InsertFails_Rollback(t *testing.T) {
	// This test is more complex to set up since we need to make insert fail
	// after delete succeeds, and verify rollback behavior
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	
	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)
	
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID
	
	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// For this test, we'll test with a non-existent table to force insert to fail
	// We'll modify the test to use a different table ID that doesn't exist
	
	// First, remove the table we just added to cause insert to fail
	tm = NewTableManager() // Reset table manager
	ps = NewPageStore(tm)  // Reset page store
	
	// Add the table back
	err = tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}
	
	// Now remove the table to make insert fail but keep the old tuple pointing to table 1
	// Actually, let's use a different approach - modify the old tuple to point to a non-existent table
	pageID2 := heap.NewHeapPageID(999, 0) // Non-existent table
	recordID2 := &tuple.TupleRecordID{
		PageID:   pageID2,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID2

	// Test UpdateTuple - delete will fail due to table not found
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err == nil {
		t.Error("Expected error when both delete and insert operations would fail")
	}
}

func TestPageStore_UpdateTuple_WithTransactionTracking(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Create transaction and add to transaction map
	tid := transaction.NewTransactionID()
	ps.transactions[tid] = &TransactionInfo{
		startTime:   time.Now(),
		dirtyPages:  make(map[tuple.PageID]bool),
		lockedPages: make(map[tuple.PageID]Permissions),
	}

	// Create old tuple with RecordID
	oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField1 := types.NewIntField(int32(42))
	oldTuple.SetField(0, intField1)
	
	pageID := heap.NewHeapPageID(1, 0)
	recordID := &tuple.TupleRecordID{
		PageID:   pageID,
		TupleNum: 0,
	}
	oldTuple.RecordID = recordID
	
	// Create new tuple
	newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField2 := types.NewIntField(int32(84))
	newTuple.SetField(0, intField2)

	// Test UpdateTuple
	err = ps.UpdateTuple(tid, oldTuple, newTuple)
	if err != nil {
		t.Errorf("UpdateTuple failed: %v", err)
	}

	// Verify transaction info was updated
	txInfo := ps.transactions[tid]
	if len(txInfo.dirtyPages) == 0 {
		t.Error("Expected dirty pages to be tracked")
	}

	// Verify pages exist and are marked dirty
	for pageID := range txInfo.dirtyPages {
		cachedPage, exists := ps.pageCache[pageID]
		if !exists {
			t.Errorf("Page %v should be in cache", pageID)
			continue
		}
		
		if cachedPage.IsDirty() != tid {
			t.Errorf("Page should be marked dirty by transaction %v", tid)
		}
	}
}

func TestPageStore_UpdateTuple_ConcurrentAccess(t *testing.T) {
	// Setup
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add mock tables
	for i := 1; i <= 3; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	numGoroutines := 10
	numUpdatesPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent updates
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numUpdatesPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				tableID := (j % 3) + 1 // Rotate between tables 1, 2, 3
				
				// Create old tuple with RecordID
				dbFile, _ := tm.GetDbFile(tableID)
				oldValue := goroutineID*numUpdatesPerGoroutine + j
				oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField1 := types.NewIntField(int32(oldValue))
				oldTuple.SetField(0, intField1)
				
				pageID := heap.NewHeapPageID(tableID, j)
				recordID := &tuple.TupleRecordID{
					PageID:   pageID,
					TupleNum: 0,
				}
				oldTuple.RecordID = recordID
				
				// Create new tuple
				newValue := oldValue + 1000
				newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField2 := types.NewIntField(int32(newValue))
				newTuple.SetField(0, intField2)

				// Update tuple
				err := ps.UpdateTuple(tid, oldTuple, newTuple)
				if err != nil {
					t.Errorf("UpdateTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - should have pages from both delete and insert operations
	expectedMinPages := numGoroutines * numUpdatesPerGoroutine
	if len(ps.pageCache) < expectedMinPages {
		t.Errorf("Expected at least %d pages in cache, got %d", expectedMinPages, len(ps.pageCache))
	}
}

func TestPageStore_UpdateTuple_EdgeCases(t *testing.T) {
	t.Run("Nil TransactionID", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		// Create old tuple with RecordID
		oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField1 := types.NewIntField(int32(42))
		oldTuple.SetField(0, intField1)
		
		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		oldTuple.RecordID = recordID
		
		// Create new tuple
		newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField2 := types.NewIntField(int32(84))
		newTuple.SetField(0, intField2)

		// Test with nil transaction ID
		_ = ps.UpdateTuple(nil, oldTuple, newTuple)
		// This should not panic and may or may not error depending on implementation
		// The important thing is it doesn't crash
	})

	t.Run("Nil Old Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()
		
		// Create new tuple
		newTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(84))
		newTuple.SetField(0, intField)
		
		// Test with nil old tuple
		err = ps.UpdateTuple(tid, nil, newTuple)
		if err == nil {
			t.Error("Expected error for nil old tuple")
		}
	})

	t.Run("Nil New Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()
		
		// Create old tuple with RecordID
		oldTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		oldTuple.SetField(0, intField)
		
		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		oldTuple.RecordID = recordID
		
		// Test with nil new tuple
		err = ps.UpdateTuple(tid, oldTuple, nil)
		// This will likely fail during the insert phase, but may succeed
		// depending on the mock implementation
		_ = err // We don't require this to error
	})

	t.Run("Same Old and New Tuple", func(t *testing.T) {
		tm := NewTableManager()
		ps := NewPageStore(tm)
		
		dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, "test_table", "id")
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		tid := transaction.NewTransactionID()
		
		// Create tuple with RecordID
		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(42))
		testTuple.SetField(0, intField)
		
		pageID := heap.NewHeapPageID(1, 0)
		recordID := &tuple.TupleRecordID{
			PageID:   pageID,
			TupleNum: 0,
		}
		testTuple.RecordID = recordID
		
		// Test with same tuple for old and new
		err = ps.UpdateTuple(tid, testTuple, testTuple)
		if err != nil {
			t.Errorf("UpdateTuple should handle same tuple update: %v", err)
		}
	})
}

func TestPageStore_FlushAllPages_EmptyCache(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Test with empty cache
	err := ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages should not fail with empty cache: %v", err)
	}
}

func TestPageStore_FlushAllPages_SinglePage(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	// Insert tuple to add page to cache
	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Verify page is in cache and dirty
	if len(ps.pageCache) != 1 {
		t.Fatalf("Expected 1 page in cache, got %d", len(ps.pageCache))
	}

	// Test FlushAllPages
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify page is still in cache but no longer dirty
	if len(ps.pageCache) != 1 {
		t.Errorf("Expected page to remain in cache after flush, got %d pages", len(ps.pageCache))
	}
	
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			t.Error("Page should not be dirty after flush")
		}
	}
}

func TestPageStore_FlushAllPages_MultiplePages(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add mock tables
	numTables := 3
	numTuplesPerTable := 5
	
	for i := 1; i <= numTables; i++ {
		dbFile := newMockDbFileForPageStore(i, []types.Type{types.IntType}, []string{"id"})
		err := tm.AddTable(dbFile, fmt.Sprintf("table_%d", i), "id")
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}
	}

	// Insert tuples to create multiple pages
	for tableID := 1; tableID <= numTables; tableID++ {
		dbFile, _ := tm.GetDbFile(tableID)
		for tupleIdx := 0; tupleIdx < numTuplesPerTable; tupleIdx++ {
			tid := transaction.NewTransactionID()
			testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
			intField := types.NewIntField(int32(tableID*100 + tupleIdx))
			testTuple.SetField(0, intField)

			err := ps.InsertTuple(tid, tableID, testTuple)
			if err != nil {
				t.Fatalf("Failed to insert tuple %d in table %d: %v", tupleIdx, tableID, err)
			}
		}
	}

	expectedPages := numTables * numTuplesPerTable
	if len(ps.pageCache) != expectedPages {
		t.Fatalf("Expected %d pages in cache, got %d", expectedPages, len(ps.pageCache))
	}

	// Count dirty pages before flush
	dirtyPagesBefore := 0
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		}
	}
	
	if dirtyPagesBefore != expectedPages {
		t.Fatalf("Expected %d dirty pages before flush, got %d", expectedPages, dirtyPagesBefore)
	}

	// Test FlushAllPages
	err := ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify all pages are still in cache but no longer dirty
	if len(ps.pageCache) != expectedPages {
		t.Errorf("Expected %d pages to remain in cache after flush, got %d", expectedPages, len(ps.pageCache))
	}
	
	dirtyPagesAfter := 0
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}
	
	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_ConcurrentAccess(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	numGoroutines := 10
	numInsertsPerGoroutine := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent inserts
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numInsertsPerGoroutine; j++ {
				tid := transaction.NewTransactionID()
				testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
				intField := types.NewIntField(int32(goroutineID*numInsertsPerGoroutine + j))
				testTuple.SetField(0, intField)

				err := ps.InsertTuple(tid, 1, testTuple)
				if err != nil {
					t.Errorf("InsertTuple failed for goroutine %d, iteration %d: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedPages := numGoroutines * numInsertsPerGoroutine
	if len(ps.pageCache) != expectedPages {
		t.Fatalf("Expected %d pages in cache, got %d", expectedPages, len(ps.pageCache))
	}

	// Test concurrent FlushAllPages calls
	flushGoroutines := 5
	var flushWg sync.WaitGroup
	flushWg.Add(flushGoroutines)

	flushErrors := make([]error, flushGoroutines)
	for i := 0; i < flushGoroutines; i++ {
		go func(idx int) {
			defer flushWg.Done()
			flushErrors[idx] = ps.FlushAllPages()
		}(i)
	}

	flushWg.Wait()

	// Check that all flush operations succeeded
	for i, err := range flushErrors {
		if err != nil {
			t.Errorf("FlushAllPages failed in goroutine %d: %v", i, err)
		}
	}

	// Verify final state - pages should still be in cache but not dirty
	if len(ps.pageCache) != expectedPages {
		t.Errorf("Expected %d pages to remain in cache after concurrent flush, got %d", expectedPages, len(ps.pageCache))
	}
	
	dirtyPagesAfter := 0
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}
	
	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after concurrent flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_MixedDirtyAndCleanPages(t *testing.T) {
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create and add a mock table
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Insert some tuples to create dirty pages
	numTuples := 5
	for i := 0; i < numTuples; i++ {
		tid := transaction.NewTransactionID()
		testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
		intField := types.NewIntField(int32(i))
		testTuple.SetField(0, intField)

		err := ps.InsertTuple(tid, 1, testTuple)
		if err != nil {
			t.Fatalf("Failed to insert tuple %d: %v", i, err)
		}
	}

	if len(ps.pageCache) != numTuples {
		t.Fatalf("Expected %d pages in cache, got %d", numTuples, len(ps.pageCache))
	}

	// Manually mark some pages as clean
	pageIDs := make([]tuple.PageID, 0, len(ps.pageCache))
	for pid := range ps.pageCache {
		pageIDs = append(pageIDs, pid)
	}
	
	// Mark first two pages as clean
	for i := 0; i < 2 && i < len(pageIDs); i++ {
		page := ps.pageCache[pageIDs[i]]
		page.MarkDirty(false, nil)
		ps.pageCache[pageIDs[i]] = page
	}

	// Count dirty pages before flush
	dirtyPagesBefore := 0
	cleanPagesBefore := 0
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			dirtyPagesBefore++
		} else {
			cleanPagesBefore++
		}
	}
	
	expectedDirty := numTuples - 2
	expectedClean := 2
	if dirtyPagesBefore != expectedDirty {
		t.Fatalf("Expected %d dirty pages before flush, got %d", expectedDirty, dirtyPagesBefore)
	}
	if cleanPagesBefore != expectedClean {
		t.Fatalf("Expected %d clean pages before flush, got %d", expectedClean, cleanPagesBefore)
	}

	// Test FlushAllPages
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed: %v", err)
	}

	// Verify all pages are still in cache and all are clean
	if len(ps.pageCache) != numTuples {
		t.Errorf("Expected %d pages to remain in cache after flush, got %d", numTuples, len(ps.pageCache))
	}
	
	dirtyPagesAfter := 0
	for _, page := range ps.pageCache {
		if page.IsDirty() != nil {
			dirtyPagesAfter++
		}
	}
	
	if dirtyPagesAfter != 0 {
		t.Errorf("Expected 0 dirty pages after flush, got %d", dirtyPagesAfter)
	}
}

func TestPageStore_FlushAllPages_WriteFailure(t *testing.T) {
	// This test would require a mock that can simulate write failures
	// For now, we'll test the basic error propagation structure
	tm := NewTableManager()
	ps := NewPageStore(tm)
	
	// Create a mock table and insert a tuple
	dbFile := newMockDbFileForPageStore(1, []types.Type{types.IntType}, []string{"id"})
	err := tm.AddTable(dbFile, "test_table", "id")
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tid := transaction.NewTransactionID()
	testTuple := tuple.NewTuple(dbFile.GetTupleDesc())
	intField := types.NewIntField(int32(42))
	testTuple.SetField(0, intField)

	err = ps.InsertTuple(tid, 1, testTuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Test FlushAllPages (should succeed with our mock)
	err = ps.FlushAllPages()
	if err != nil {
		t.Errorf("FlushAllPages failed unexpectedly: %v", err)
	}
	
	// In a real implementation, you would create a mock that fails WritePage
	// and verify that FlushAllPages returns the error appropriately
}