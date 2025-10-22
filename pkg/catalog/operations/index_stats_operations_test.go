package operations

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"testing"
)

// mockIndexStatsAccess implements a mock for testing IndexStatsOperations
type mockIndexStatsAccess struct {
	indexStatsData map[int]*systemtable.IndexStatisticsRow
	indexData      map[int]*systemtable.IndexMetadata
	iterateError   error
}

func (m *mockIndexStatsAccess) IterateTable(tableID int, tx *transaction.TransactionContext, processFunc func(*tuple.Tuple) error) error {
	if m.iterateError != nil {
		return m.iterateError
	}

	// Mock iteration over index statistics table
	for _, stats := range m.indexStatsData {
		tup := systemtable.IndexStats.CreateTuple(stats)
		if err := processFunc(tup); err != nil {
			return err
		}
	}

	// Mock iteration over indexes table
	for _, idx := range m.indexData {
		tup := systemtable.Indexes.CreateTuple(*idx)
		if err := processFunc(tup); err != nil {
			return err
		}
	}

	return nil
}

func (m *mockIndexStatsAccess) InsertRow(tableID int, tx *transaction.TransactionContext, t *tuple.Tuple) error {
	stats, err := systemtable.IndexStats.Parse(t)
	if err != nil {
		return err
	}
	m.indexStatsData[stats.IndexID] = stats
	return nil
}

func (m *mockIndexStatsAccess) DeleteRow(tableID int, tx *transaction.TransactionContext, t *tuple.Tuple) error {
	stats, err := systemtable.IndexStats.Parse(t)
	if err != nil {
		return err
	}
	delete(m.indexStatsData, stats.IndexID)
	return nil
}

func mockIndexStatsFileGetter(tableID int) (page.DbFile, error) {
	return nil, nil
}

func TestIndexStatsOperations_GetIndexStatistics(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: map[int]*systemtable.IndexStatisticsRow{
			1: {
				IndexID:          1,
				TableID:          100,
				IndexName:        "idx_test",
				IndexType:        index.BTreeIndex,
				ColumnName:       "id",
				NumEntries:       1000,
				NumPages:         10,
				BTreeHeight:      3,
				DistinctKeys:     800,
				ClusteringFactor: 0.75,
				AvgKeySize:       8,
			},
		},
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if stats.IndexID != 1 {
		t.Errorf("Expected IndexID 1, got %d", stats.IndexID)
	}
	if stats.IndexName != "idx_test" {
		t.Errorf("Expected IndexName 'idx_test', got '%s'", stats.IndexName)
	}
	if stats.NumEntries != 1000 {
		t.Errorf("Expected NumEntries 1000, got %d", stats.NumEntries)
	}
}

func TestIndexStatsOperations_GetIndexStatistics_NotFound(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: map[int]*systemtable.IndexStatisticsRow{},
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	_, err := ops.GetIndexStatistics(nil, 999)
	if err == nil {
		t.Fatal("Expected error for non-existent index, got nil")
	}
}

func TestIndexStatsOperations_StoreIndexStatistics(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: map[int]*systemtable.IndexStatisticsRow{},
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats := &systemtable.IndexStatisticsRow{
		IndexID:          2,
		TableID:          200,
		IndexName:        "idx_new",
		IndexType:        index.HashIndex,
		ColumnName:       "name",
		NumEntries:       500,
		NumPages:         5,
		BTreeHeight:      1,
		DistinctKeys:     400,
		ClusteringFactor: 0.5,
		AvgKeySize:       16,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify it was stored
	stored, err := ops.GetIndexStatistics(nil, 2)
	if err != nil {
		t.Fatalf("Expected no error retrieving stored stats, got %v", err)
	}

	if stored.IndexName != "idx_new" {
		t.Errorf("Expected IndexName 'idx_new', got '%s'", stored.IndexName)
	}
}

func TestIndexStatsOperations_StoreIndexStatistics_Update(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: map[int]*systemtable.IndexStatisticsRow{
			3: {
				IndexID:          3,
				TableID:          300,
				IndexName:        "idx_old",
				IndexType:        index.BTreeIndex,
				ColumnName:       "id",
				NumEntries:       100,
				NumPages:         2,
				BTreeHeight:      2,
				DistinctKeys:     80,
				ClusteringFactor: 0.6,
				AvgKeySize:       8,
			},
		},
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Update existing statistics
	updatedStats := &systemtable.IndexStatisticsRow{
		IndexID:          3,
		TableID:          300,
		IndexName:        "idx_updated",
		IndexType:        index.BTreeIndex,
		ColumnName:       "id",
		NumEntries:       2000,
		NumPages:         20,
		BTreeHeight:      4,
		DistinctKeys:     1500,
		ClusteringFactor: 0.85,
		AvgKeySize:       8,
	}

	err := ops.StoreIndexStatistics(nil, updatedStats)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify it was updated
	stored, err := ops.GetIndexStatistics(nil, 3)
	if err != nil {
		t.Fatalf("Expected no error retrieving updated stats, got %v", err)
	}

	if stored.IndexName != "idx_updated" {
		t.Errorf("Expected updated IndexName 'idx_updated', got '%s'", stored.IndexName)
	}
	if stored.NumEntries != 2000 {
		t.Errorf("Expected updated NumEntries 2000, got %d", stored.NumEntries)
	}
}

// TestIndexStatsOperations_MultipleStatistics tests managing multiple index statistics
func TestIndexStatsOperations_MultipleStatistics(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Store multiple statistics
	for i := 1; i <= 5; i++ {
		stats := &systemtable.IndexStatisticsRow{
			IndexID:          i,
			TableID:          100,
			IndexName:        "idx_" + string(rune('0'+i)),
			IndexType:        index.BTreeIndex,
			ColumnName:       "col" + string(rune('0'+i)),
			NumEntries:       int64(i * 1000),
			NumPages:         i * 10,
			BTreeHeight:      i + 1,
			DistinctKeys:     int64(i * 800),
			ClusteringFactor: float64(i) * 0.15,
			AvgKeySize:       8,
		}
		err := ops.StoreIndexStatistics(nil, stats)
		if err != nil {
			t.Fatalf("Failed to store stats for index %d: %v", i, err)
		}
	}

	// Verify all were stored
	if len(mock.indexStatsData) != 5 {
		t.Errorf("Expected 5 statistics stored, got %d", len(mock.indexStatsData))
	}

	// Retrieve and verify each one
	for i := 1; i <= 5; i++ {
		stats, err := ops.GetIndexStatistics(nil, i)
		if err != nil {
			t.Errorf("Failed to retrieve stats for index %d: %v", i, err)
		}
		if stats.IndexID != i {
			t.Errorf("Expected IndexID %d, got %d", i, stats.IndexID)
		}
		if stats.NumEntries != int64(i*1000) {
			t.Errorf("Expected NumEntries %d, got %d", i*1000, stats.NumEntries)
		}
	}
}

// TestIndexStatsOperations_ZeroEntries tests statistics with zero entries
func TestIndexStatsOperations_ZeroEntries(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_empty",
		IndexType:        index.BTreeIndex,
		ColumnName:       "id",
		NumEntries:       0,
		NumPages:         0,
		BTreeHeight:      1,
		DistinctKeys:     0,
		ClusteringFactor: 1.0,
		AvgKeySize:       0,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Expected no error storing empty stats, got %v", err)
	}

	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Expected no error retrieving empty stats, got %v", err)
	}

	if retrieved.NumEntries != 0 {
		t.Errorf("Expected 0 entries, got %d", retrieved.NumEntries)
	}
	if retrieved.DistinctKeys != 0 {
		t.Errorf("Expected 0 distinct keys, got %d", retrieved.DistinctKeys)
	}
}

// TestIndexStatsOperations_LargeEntries tests statistics with very large numbers
func TestIndexStatsOperations_LargeEntries(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_large",
		IndexType:        index.BTreeIndex,
		ColumnName:       "id",
		NumEntries:       10000000000, // 10 billion
		NumPages:         1000000,     // 1 million
		BTreeHeight:      10,
		DistinctKeys:     5000000000, // 5 billion
		ClusteringFactor: 0.95,
		AvgKeySize:       128,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Expected no error storing large stats, got %v", err)
	}

	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Expected no error retrieving large stats, got %v", err)
	}

	if retrieved.NumEntries != 10000000000 {
		t.Errorf("Expected 10000000000 entries, got %d", retrieved.NumEntries)
	}
	if retrieved.DistinctKeys != 5000000000 {
		t.Errorf("Expected 5000000000 distinct keys, got %d", retrieved.DistinctKeys)
	}
}

// TestIndexStatsOperations_ClusteringFactorEdgeCases tests clustering factor boundaries
func TestIndexStatsOperations_ClusteringFactorEdgeCases(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	testCases := []struct {
		name             string
		clusteringFactor float64
	}{
		{"Perfect clustering", 1.0},
		{"Random clustering", 0.0},
		{"High clustering", 0.95},
		{"Low clustering", 0.05},
		{"Medium clustering", 0.5},
	}

	for i, tc := range testCases {
		stats := &systemtable.IndexStatisticsRow{
			IndexID:          i + 1,
			TableID:          100,
			IndexName:        "idx_" + tc.name,
			IndexType:        index.BTreeIndex,
			ColumnName:       "id",
			NumEntries:       1000,
			NumPages:         10,
			BTreeHeight:      3,
			DistinctKeys:     800,
			ClusteringFactor: tc.clusteringFactor,
			AvgKeySize:       8,
		}

		err := ops.StoreIndexStatistics(nil, stats)
		if err != nil {
			t.Fatalf("Failed to store stats with clustering factor %.2f: %v", tc.clusteringFactor, err)
		}

		retrieved, err := ops.GetIndexStatistics(nil, i+1)
		if err != nil {
			t.Fatalf("Failed to retrieve stats: %v", err)
		}

		// Allow for small floating point precision differences
		diff := retrieved.ClusteringFactor - tc.clusteringFactor
		if diff < -0.01 || diff > 0.01 {
			t.Errorf("Expected clustering factor %.2f, got %.2f", tc.clusteringFactor, retrieved.ClusteringFactor)
		}
	}
}

// TestIndexStatsOperations_HashIndexType tests statistics for Hash indexes
func TestIndexStatsOperations_HashIndexType(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_hash",
		IndexType:        index.HashIndex,
		ColumnName:       "email",
		NumEntries:       5000,
		NumPages:         50,
		BTreeHeight:      1, // Hash indexes should have height 1
		DistinctKeys:     4500,
		ClusteringFactor: 0.3, // Hash indexes typically have lower clustering
		AvgKeySize:       32,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Expected no error storing hash index stats, got %v", err)
	}

	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Expected no error retrieving hash index stats, got %v", err)
	}

	if retrieved.IndexType != index.HashIndex {
		t.Errorf("Expected HashIndex type, got %v", retrieved.IndexType)
	}
	if retrieved.BTreeHeight != 1 {
		t.Errorf("Expected BTreeHeight 1 for hash index, got %d", retrieved.BTreeHeight)
	}
}

// TestIndexStatsOperations_BTreeVsHash tests both index types side by side
func TestIndexStatsOperations_BTreeVsHash(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	btreeStats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_btree",
		IndexType:        index.BTreeIndex,
		ColumnName:       "id",
		NumEntries:       10000,
		NumPages:         100,
		BTreeHeight:      4,
		DistinctKeys:     9000,
		ClusteringFactor: 0.85,
		AvgKeySize:       8,
	}

	hashStats := &systemtable.IndexStatisticsRow{
		IndexID:          2,
		TableID:          100,
		IndexName:        "idx_hash",
		IndexType:        index.HashIndex,
		ColumnName:       "email",
		NumEntries:       10000,
		NumPages:         100,
		BTreeHeight:      1,
		DistinctKeys:     9500,
		ClusteringFactor: 0.25,
		AvgKeySize:       32,
	}

	// Store both
	if err := ops.StoreIndexStatistics(nil, btreeStats); err != nil {
		t.Fatalf("Failed to store BTree stats: %v", err)
	}
	if err := ops.StoreIndexStatistics(nil, hashStats); err != nil {
		t.Fatalf("Failed to store Hash stats: %v", err)
	}

	// Retrieve and verify
	retrievedBTree, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Failed to retrieve BTree stats: %v", err)
	}
	if retrievedBTree.IndexType != index.BTreeIndex {
		t.Errorf("Expected BTreeIndex, got %v", retrievedBTree.IndexType)
	}

	retrievedHash, err := ops.GetIndexStatistics(nil, 2)
	if err != nil {
		t.Fatalf("Failed to retrieve Hash stats: %v", err)
	}
	if retrievedHash.IndexType != index.HashIndex {
		t.Errorf("Expected HashIndex, got %v", retrievedHash.IndexType)
	}
}

// TestIndexStatsOperations_CollectIndexStatistics tests the CollectIndexStatistics method
func TestIndexStatsOperations_CollectIndexStatistics(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats, err := ops.CollectIndexStatistics(nil, 1, 100, "idx_test", index.BTreeIndex, "id")
	if err != nil {
		t.Fatalf("Failed to collect index statistics: %v", err)
	}

	if stats.IndexID != 1 {
		t.Errorf("Expected IndexID 1, got %d", stats.IndexID)
	}
	if stats.TableID != 100 {
		t.Errorf("Expected TableID 100, got %d", stats.TableID)
	}
	if stats.IndexName != "idx_test" {
		t.Errorf("Expected IndexName 'idx_test', got '%s'", stats.IndexName)
	}
	// When statsOps is nil, NumEntries will be estimated based on defaults
	// Just verify the basic fields are set correctly
	if stats.BTreeHeight < 1 {
		t.Errorf("Expected BTreeHeight >= 1, got %d", stats.BTreeHeight)
	}
	if stats.ClusteringFactor < 0 || stats.ClusteringFactor > 1 {
		t.Errorf("Expected ClusteringFactor between 0 and 1, got %.2f", stats.ClusteringFactor)
	}
}

// TestIndexStatsOperations_CollectIndexStatistics_HashIndex tests collecting stats for hash index
func TestIndexStatsOperations_CollectIndexStatistics_HashIndex(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	stats, err := ops.CollectIndexStatistics(nil, 2, 200, "idx_hash", index.HashIndex, "email")
	if err != nil {
		t.Fatalf("Failed to collect hash index statistics: %v", err)
	}

	if stats.IndexType != index.HashIndex {
		t.Errorf("Expected HashIndex type, got %v", stats.IndexType)
	}
	if stats.BTreeHeight != 1 {
		t.Errorf("Expected BTreeHeight 1 for hash index, got %d", stats.BTreeHeight)
	}
}

// TestIndexStatsOperations_StoreAndRetrieveMultiple tests storing and retrieving multiple stats
func TestIndexStatsOperations_StoreAndRetrieveMultiple(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Store 10 different statistics
	for i := 1; i <= 10; i++ {
		stats := &systemtable.IndexStatisticsRow{
			IndexID:          i,
			TableID:          100 + i,
			IndexName:        "idx_" + string(rune('A'+i-1)),
			IndexType:        index.BTreeIndex,
			ColumnName:       "col" + string(rune('0'+i)),
			NumEntries:       int64(i * 100),
			NumPages:         i,
			BTreeHeight:      2,
			DistinctKeys:     int64(i * 80),
			ClusteringFactor: float64(i) * 0.09,
			AvgKeySize:       8,
		}
		err := ops.StoreIndexStatistics(nil, stats)
		if err != nil {
			t.Fatalf("Failed to store stats %d: %v", i, err)
		}
	}

	// Verify all can be retrieved
	for i := 1; i <= 10; i++ {
		stats, err := ops.GetIndexStatistics(nil, i)
		if err != nil {
			t.Errorf("Failed to retrieve stats %d: %v", i, err)
			continue
		}
		if stats.TableID != 100+i {
			t.Errorf("Stats %d: Expected TableID %d, got %d", i, 100+i, stats.TableID)
		}
	}
}

// TestIndexStatsOperations_UpdateMultipleTimes tests updating statistics multiple times
func TestIndexStatsOperations_UpdateMultipleTimes(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Initial stats
	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_test",
		IndexType:        index.BTreeIndex,
		ColumnName:       "id",
		NumEntries:       100,
		NumPages:         1,
		BTreeHeight:      2,
		DistinctKeys:     80,
		ClusteringFactor: 0.5,
		AvgKeySize:       8,
	}

	// Store and update 5 times
	for i := 0; i < 5; i++ {
		stats.NumEntries = int64((i + 1) * 1000)
		stats.DistinctKeys = int64((i + 1) * 800)
		err := ops.StoreIndexStatistics(nil, stats)
		if err != nil {
			t.Fatalf("Failed to update stats (iteration %d): %v", i, err)
		}
	}

	// Verify final state
	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Failed to retrieve final stats: %v", err)
	}

	if retrieved.NumEntries != 5000 {
		t.Errorf("Expected NumEntries 5000, got %d", retrieved.NumEntries)
	}
	if retrieved.DistinctKeys != 4000 {
		t.Errorf("Expected DistinctKeys 4000, got %d", retrieved.DistinctKeys)
	}
}

// TestIndexStatsOperations_DifferentColumnNames tests indexes on different columns
func TestIndexStatsOperations_DifferentColumnNames(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	columnNames := []string{
		"id",
		"name",
		"email",
		"created_at",
		"updated_at",
		"user_id",
		"product_sku",
		"order_total",
	}

	for i, colName := range columnNames {
		stats := &systemtable.IndexStatisticsRow{
			IndexID:          i + 1,
			TableID:          100,
			IndexName:        "idx_" + colName,
			IndexType:        index.BTreeIndex,
			ColumnName:       colName,
			NumEntries:       1000,
			NumPages:         10,
			BTreeHeight:      3,
			DistinctKeys:     800,
			ClusteringFactor: 0.7,
			AvgKeySize:       8,
		}
		err := ops.StoreIndexStatistics(nil, stats)
		if err != nil {
			t.Fatalf("Failed to store stats for column '%s': %v", colName, err)
		}
	}

	// Verify all were stored with correct column names
	for i, colName := range columnNames {
		stats, err := ops.GetIndexStatistics(nil, i+1)
		if err != nil {
			t.Errorf("Failed to retrieve stats for column '%s': %v", colName, err)
			continue
		}
		if stats.ColumnName != colName {
			t.Errorf("Expected ColumnName '%s', got '%s'", colName, stats.ColumnName)
		}
	}
}

// TestIndexStatsOperations_HighDistinctKeys tests indexes with high cardinality
func TestIndexStatsOperations_HighDistinctKeys(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Test case: DistinctKeys equals NumEntries (unique index)
	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_unique",
		IndexType:        index.BTreeIndex,
		ColumnName:       "uuid",
		NumEntries:       1000000,
		NumPages:         10000,
		BTreeHeight:      5,
		DistinctKeys:     1000000, // 100% unique
		ClusteringFactor: 0.9,
		AvgKeySize:       36,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Failed to store unique index stats: %v", err)
	}

	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Failed to retrieve unique index stats: %v", err)
	}

	if retrieved.DistinctKeys != retrieved.NumEntries {
		t.Errorf("Expected DistinctKeys to equal NumEntries for unique index, got %d != %d",
			retrieved.DistinctKeys, retrieved.NumEntries)
	}
}

// TestIndexStatsOperations_LowDistinctKeys tests indexes with low cardinality
func TestIndexStatsOperations_LowDistinctKeys(t *testing.T) {
	mock := &mockIndexStatsAccess{
		indexStatsData: make(map[int]*systemtable.IndexStatisticsRow),
	}

	ops := NewIndexStatsOperations(mock, 1, 2, mockIndexStatsFileGetter, nil)

	// Test case: Very few distinct keys (e.g., boolean column)
	stats := &systemtable.IndexStatisticsRow{
		IndexID:          1,
		TableID:          100,
		IndexName:        "idx_boolean",
		IndexType:        index.BTreeIndex,
		ColumnName:       "is_active",
		NumEntries:       1000000,
		NumPages:         10000,
		BTreeHeight:      3,
		DistinctKeys:     2, // Only true/false
		ClusteringFactor: 0.4,
		AvgKeySize:       1,
	}

	err := ops.StoreIndexStatistics(nil, stats)
	if err != nil {
		t.Fatalf("Failed to store low-cardinality index stats: %v", err)
	}

	retrieved, err := ops.GetIndexStatistics(nil, 1)
	if err != nil {
		t.Fatalf("Failed to retrieve low-cardinality index stats: %v", err)
	}

	if retrieved.DistinctKeys != 2 {
		t.Errorf("Expected DistinctKeys 2 for boolean index, got %d", retrieved.DistinctKeys)
	}
}

// MockStatsOperations is a mock for testing
type MockStatsOperations struct {
	stats map[int]*systemtable.TableStatistics
}

func (m *MockStatsOperations) GetTableStatistics(tx TxContext, tableID int) (*systemtable.TableStatistics, error) {
	stats, ok := m.stats[tableID]
	if !ok {
		return nil, fmt.Errorf("table statistics not found")
	}
	return stats, nil
}
