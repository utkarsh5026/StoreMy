package operations

import (
	"errors"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
	"time"
)

// mockFileGetter returns a mock DbFile for testing
// Note: Returns a non-HeapFile to simulate page count of 0 unless we can properly mock HeapFile
func mockFileGetter(tableID int) (page.DbFile, error) {
	if tableID == 999 {
		return nil, errors.New("file not found")
	}
	// Return a simple mock that doesn't match *heap.HeapFile type assertion
	// This will result in pageCount = 0 in the implementation
	return &mockDbFile{}, nil
}

// mockDbFile implements page.DbFile for testing but is not a HeapFile
type mockDbFile struct{}

func (m *mockDbFile) NumPages() (int, error) {
	return 5, nil
}

func (m *mockDbFile) ReadPage(pid primitives.PageID) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) WritePage(p page.Page) error {
	return nil
}

func (m *mockDbFile) GetID() int {
	return 1
}

func (m *mockDbFile) GetTupleDesc() *tuple.TupleDescription {
	return nil
}

func (m *mockDbFile) Close() error {
	return nil
}

// mockStatsCache implements StatsCacheSetter for testing
type mockStatsCache struct {
	cached map[int]*tableStats
}

func newMockStatsCache() *mockStatsCache {
	return &mockStatsCache{
		cached: make(map[int]*tableStats),
	}
}

func (m *mockStatsCache) SetCachedStatistics(tableID int, stats *tableStats) error {
	m.cached[tableID] = stats
	return nil
}

// createStatsTuple creates a statistics tuple for testing
func createStatsTuple(tableID, cardinality, pageCount, avgTupleSize, distinctValues int) *tuple.Tuple {
	stats := &tableStats{
		TableID:        tableID,
		Cardinality:    cardinality,
		PageCount:      pageCount,
		AvgTupleSize:   avgTupleSize,
		DistinctValues: distinctValues,
		LastUpdated:    time.Now(),
	}
	return systemtable.Stats.CreateTuple(stats)
}

func TestNewStatsOperations(t *testing.T) {
	mock := newMockCatalogAccess()
	cache := newMockStatsCache()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, cache)

	if so == nil {
		t.Fatal("expected StatsOperations, got nil")
	}

	if so.TableID() != 1 {
		t.Errorf("expected statsTableID 1, got %d", so.TableID())
	}

	if so.cache == nil {
		t.Error("expected cache to be set")
	}
}

func TestNewStatsOperations_NilCache(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	if so == nil {
		t.Fatal("expected StatsOperations, got nil")
	}

	if so.cache != nil {
		t.Error("expected cache to be nil")
	}
}

func TestGetTableStatistics_Success(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Add statistics for table 100
	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(100, 1000, 5, 50, 100),
		createStatsTuple(200, 2000, 10, 60, 200),
	}

	tx := &transaction.TransactionContext{}
	stats, err := so.GetTableStatistics(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats == nil {
		t.Fatal("expected stats, got nil")
	}

	if stats.TableID != 100 {
		t.Errorf("expected table ID 100, got %d", stats.TableID)
	}

	if stats.Cardinality != 1000 {
		t.Errorf("expected cardinality 1000, got %d", stats.Cardinality)
	}

	if stats.PageCount != 5 {
		t.Errorf("expected page count 5, got %d", stats.PageCount)
	}

	if stats.AvgTupleSize != 50 {
		t.Errorf("expected avg tuple size 50, got %d", stats.AvgTupleSize)
	}

	if stats.DistinctValues != 100 {
		t.Errorf("expected distinct values 100, got %d", stats.DistinctValues)
	}
}

func TestGetTableStatistics_NotFound(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Add statistics for different table
	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(200, 2000, 10, 60, 200),
	}

	tx := &transaction.TransactionContext{}
	stats, err := so.GetTableStatistics(tx, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if stats != nil {
		t.Errorf("expected nil stats, got %+v", stats)
	}
}

func TestGetTableStatistics_EmptyTable(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	mock.tuples[1] = []*tuple.Tuple{}

	tx := &transaction.TransactionContext{}
	stats, err := so.GetTableStatistics(tx, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if stats != nil {
		t.Errorf("expected nil stats, got %+v", stats)
	}
}

func TestUpdateTableStatistics_InsertNew(t *testing.T) {
	mock := newMockCatalogAccess()
	cache := newMockStatsCache()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, cache)

	// Setup column metadata for table 100 (id is primary key at position 0)
	setupTableColumns(mock, 2, 100, []struct {
		name      string
		fieldType types.Type
		isPrimary bool
	}{
		{"id", types.IntType, true},
		{"name", types.StringType, false},
	})

	// Create test data for table 100
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	t1 := tuple.NewTuple(td)
	t1.SetField(0, types.NewIntField(1))
	t1.SetField(1, types.NewStringField("Alice", 256))

	t2 := tuple.NewTuple(td)
	t2.SetField(0, types.NewIntField(2))
	t2.SetField(1, types.NewStringField("Bob", 256))

	t3 := tuple.NewTuple(td)
	t3.SetField(0, types.NewIntField(3))
	t3.SetField(1, types.NewStringField("Charlie", 256))

	mock.tuples[100] = []*tuple.Tuple{t1, t2, t3}

	tx := &transaction.TransactionContext{}
	err = so.UpdateTableStatistics(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify statistics were inserted
	if len(mock.tuples[1]) != 1 {
		t.Errorf("expected 1 statistics tuple, got %d", len(mock.tuples[1]))
	}

	stats, err := systemtable.Stats.Parse(mock.tuples[1][0])
	if err != nil {
		t.Fatalf("failed to parse stats: %v", err)
	}

	if stats.TableID != 100 {
		t.Errorf("expected table ID 100, got %d", stats.TableID)
	}

	if stats.Cardinality != 3 {
		t.Errorf("expected cardinality 3, got %d", stats.Cardinality)
	}

	// Page count will be 0 since mockDbFile is not a *heap.HeapFile
	if stats.PageCount != 0 {
		t.Errorf("expected page count 0, got %d", stats.PageCount)
	}

	if stats.DistinctValues != 3 {
		t.Errorf("expected distinct values 3, got %d", stats.DistinctValues)
	}

	// Verify cache was updated
	if cache.cached[100] == nil {
		t.Error("expected cache to be updated")
	} else if cache.cached[100].Cardinality != 3 {
		t.Errorf("expected cached cardinality 3, got %d", cache.cached[100].Cardinality)
	}
}

func TestUpdateTableStatistics_UpdateExisting(t *testing.T) {
	mock := newMockCatalogAccess()
	cache := newMockStatsCache()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, cache)

	// Add existing statistics
	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(100, 1000, 5, 50, 100),
	}

	// Create updated test data
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType, types.StringType}, []string{"id", "name"})
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	t1 := tuple.NewTuple(td)
	t1.SetField(0, types.NewIntField(1))
	t1.SetField(1, types.NewStringField("Alice", 256))

	t2 := tuple.NewTuple(td)
	t2.SetField(0, types.NewIntField(2))
	t2.SetField(1, types.NewStringField("Bob", 256))

	mock.tuples[100] = []*tuple.Tuple{t1, t2}

	tx := &transaction.TransactionContext{}
	err = so.UpdateTableStatistics(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify old statistics were replaced
	if len(mock.tuples[1]) != 1 {
		t.Errorf("expected 1 statistics tuple, got %d", len(mock.tuples[1]))
	}

	stats, err := systemtable.Stats.Parse(mock.tuples[1][0])
	if err != nil {
		t.Fatalf("failed to parse stats: %v", err)
	}

	if stats.Cardinality != 2 {
		t.Errorf("expected updated cardinality 2, got %d", stats.Cardinality)
	}

	// Verify cache was updated
	if cache.cached[100] == nil {
		t.Error("expected cache to be updated")
	} else if cache.cached[100].Cardinality != 2 {
		t.Errorf("expected cached cardinality 2, got %d", cache.cached[100].Cardinality)
	}
}

func TestUpdateTableStatistics_NilCache(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Create test data
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"id"})
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	t1 := tuple.NewTuple(td)
	t1.SetField(0, types.NewIntField(1))

	mock.tuples[100] = []*tuple.Tuple{t1}

	tx := &transaction.TransactionContext{}
	err = so.UpdateTableStatistics(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not panic with nil cache
	if len(mock.tuples[1]) != 1 {
		t.Errorf("expected 1 statistics tuple, got %d", len(mock.tuples[1]))
	}
}

func TestCollectTableStatistics_EmptyTable(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// No tuples in table
	mock.tuples[100] = []*tuple.Tuple{}

	tx := &transaction.TransactionContext{}
	stats, err := so.collectStats(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.Cardinality != 0 {
		t.Errorf("expected cardinality 0, got %d", stats.Cardinality)
	}

	if stats.AvgTupleSize != 0 {
		t.Errorf("expected avg tuple size 0, got %d", stats.AvgTupleSize)
	}

	if stats.DistinctValues != 0 {
		t.Errorf("expected distinct values 0, got %d", stats.DistinctValues)
	}
}

func TestCollectTableStatistics_SingleColumn(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Setup column metadata for table 100 (id is primary key at position 0)
	setupTableColumns(mock, 2, 100, []struct {
		name      string
		fieldType types.Type
		isPrimary bool
	}{
		{"id", types.IntType, true},
	})

	// Create table with distinct values
	td, err := tuple.NewTupleDesc([]types.Type{types.IntType}, []string{"id"})
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	t1 := tuple.NewTuple(td)
	t1.SetField(0, types.NewIntField(1))

	t2 := tuple.NewTuple(td)
	t2.SetField(0, types.NewIntField(2))

	t3 := tuple.NewTuple(td)
	t3.SetField(0, types.NewIntField(1)) // Duplicate

	mock.tuples[100] = []*tuple.Tuple{t1, t2, t3}

	tx := &transaction.TransactionContext{}
	stats, err := so.collectStats(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.Cardinality != 3 {
		t.Errorf("expected cardinality 3, got %d", stats.Cardinality)
	}

	if stats.DistinctValues != 2 {
		t.Errorf("expected distinct values 2, got %d", stats.DistinctValues)
	}

	if stats.AvgTupleSize == 0 {
		t.Error("expected non-zero avg tuple size")
	}
}

func TestCollectTableStatistics_NoFields(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Empty table
	mock.tuples[100] = []*tuple.Tuple{}

	tx := &transaction.TransactionContext{}
	stats, err := so.collectStats(tx, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.Cardinality != 0 {
		t.Errorf("expected cardinality 0, got %d", stats.Cardinality)
	}

	if stats.DistinctValues != 0 {
		t.Errorf("expected distinct values 0, got %d", stats.DistinctValues)
	}
}

func TestCollectTableStatistics_FileError(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	tx := &transaction.TransactionContext{}
	stats, err := so.collectStats(tx, 999) // Invalid table ID
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if stats != nil {
		t.Errorf("expected nil stats, got %+v", stats)
	}
}

func TestInsertNewStatistics(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	stats := &tableStats{
		TableID:        100,
		Cardinality:    1000,
		PageCount:      5,
		AvgTupleSize:   50,
		DistinctValues: 100,
		LastUpdated:    time.Now(),
	}

	tx := &transaction.TransactionContext{}
	err := so.insert(tx, stats)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.tuples[1]) != 1 {
		t.Errorf("expected 1 tuple, got %d", len(mock.tuples[1]))
	}

	parsed, err := systemtable.Stats.Parse(mock.tuples[1][0])
	if err != nil {
		t.Fatalf("failed to parse stats: %v", err)
	}

	if parsed.TableID != 100 {
		t.Errorf("expected table ID 100, got %d", parsed.TableID)
	}

	if parsed.Cardinality != 1000 {
		t.Errorf("expected cardinality 1000, got %d", parsed.Cardinality)
	}
}

func TestUpdateExistingStatistics(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Add old statistics
	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(100, 1000, 5, 50, 100),
	}

	newStats := &tableStats{
		TableID:        100,
		Cardinality:    2000,
		PageCount:      10,
		AvgTupleSize:   60,
		DistinctValues: 200,
		LastUpdated:    time.Now(),
	}

	tx := &transaction.TransactionContext{}
	err := so.update(tx, 100, newStats)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have exactly 1 tuple (old deleted, new inserted)
	if len(mock.tuples[1]) != 1 {
		t.Errorf("expected 1 tuple, got %d", len(mock.tuples[1]))
	}

	parsed, err := systemtable.Stats.Parse(mock.tuples[1][0])
	if err != nil {
		t.Fatalf("failed to parse stats: %v", err)
	}

	if parsed.Cardinality != 2000 {
		t.Errorf("expected updated cardinality 2000, got %d", parsed.Cardinality)
	}
}

func TestIterateTable(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	// Add statistics for multiple tables
	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(100, 1000, 5, 50, 100),
		createStatsTuple(200, 2000, 10, 60, 200),
		createStatsTuple(300, 3000, 15, 70, 300),
	}

	tx := &transaction.TransactionContext{}
	count := 0
	err := so.Iterate(tx, func(t *tableStats) error {
		count++
		if t.TableID < 100 || t.TableID > 300 {
			return errors.New("invalid table ID")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 iterations, got %d", count)
	}
}

func TestIterateTable_ErrorHandling(t *testing.T) {
	mock := newMockCatalogAccess()
	so := NewStatsOperations(mock, 1, 2, mockFileGetter, nil)

	mock.tuples[1] = []*tuple.Tuple{
		createStatsTuple(100, 1000, 5, 50, 100),
	}

	tx := &transaction.TransactionContext{}
	testError := errors.New("test error")
	err := so.Iterate(tx, func(t *tableStats) error {
		return testError
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Error should be wrapped
	if err.Error() != "failed to process entity: test error" {
		t.Errorf("expected wrapped test error, got %v", err)
	}
}
