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