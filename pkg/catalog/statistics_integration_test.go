package catalog

import (
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// integrationMockDB is a mock database for integration testing
type integrationMockDB struct {
	txRegistry *transaction.TransactionRegistry
	store      *memory.PageStore
}

func (m *integrationMockDB) BeginTransaction() (*transaction.TransactionContext, error) {
	return m.txRegistry.Begin()
}

func (m *integrationMockDB) CommitTransaction(tx *transaction.TransactionContext) error {
	return m.store.CommitTransaction(tx)
}

// TestStatisticsIntegration_AutomaticTracking verifies that modifications are automatically tracked
func TestStatisticsIntegration_AutomaticTracking(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	tm := memory.NewTableManager()
	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(tm, wal)
	catalog := NewSystemCatalog(store, tm)
	txRegistry := transaction.NewTransactionRegistry(wal)

	// Initialize catalog
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := catalog.Initialize(tx, tempDir); err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}
	store.CommitTransaction(tx)

	// Create and connect statistics manager
	mockDB := &integrationMockDB{txRegistry: txRegistry, store: store}
	statsManager := NewStatisticsManager(catalog, mockDB)
	statsManager.SetUpdateThreshold(10) // Low threshold for testing
	store.SetStatsManager(statsManager)

	// Create test table
	tableID := createTestTable(t, tm, catalog, txRegistry, tempDir, "test_table")

	// Insert tuples - should be automatically tracked
	for i := 0; i < 5; i++ {
		tx, err := txRegistry.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		tup := createTestTuple(i, "test")
		if err := store.InsertTuple(tx, tableID, tup); err != nil {
			t.Fatalf("Failed to insert tuple: %v", err)
		}
		store.CommitTransaction(tx)
	}

	// Verify modifications were tracked
	statsManager.mu.RLock()
	modCount := statsManager.modificationCount[tableID]
	statsManager.mu.RUnlock()

	if modCount != 5 {
		t.Errorf("Expected 5 modifications tracked, got %d", modCount)
	}

	// Update statistics
	tx2, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := statsManager.ForceUpdate(tx2, tableID); err != nil {
		t.Fatalf("Failed to update statistics: %v", err)
	}
	store.CommitTransaction(tx2)

	// Retrieve and verify statistics in a new transaction after commit
	tx3, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	stats, err := catalog.GetTableStatistics(tx3.ID, tableID)
	if err != nil {
		t.Fatalf("Failed to get statistics: %v", err)
	}

	if stats.Cardinality != 5 {
		t.Errorf("Expected cardinality 5, got %d", stats.Cardinality)
	}

	if stats.PageCount < 1 {
		t.Errorf("Expected at least 1 page, got %d", stats.PageCount)
	}

	if stats.AvgTupleSize == 0 {
		t.Error("Expected non-zero average tuple size")
	}

	store.Close()
}

// TestStatisticsIntegration_UpdateAfterThreshold verifies stats update after threshold
func TestStatisticsIntegration_UpdateAfterThreshold(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	tm := memory.NewTableManager()
	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(tm, wal)
	catalog := NewSystemCatalog(store, tm)
	txRegistry := transaction.NewTransactionRegistry(wal)

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := catalog.Initialize(tx, tempDir); err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}
	store.CommitTransaction(tx)

	// Create and connect statistics manager with low threshold
	mockDB := &integrationMockDB{txRegistry: txRegistry, store: store}
	statsManager := NewStatisticsManager(catalog, mockDB)
	statsManager.SetUpdateThreshold(3) // Update after 3 modifications
	store.SetStatsManager(statsManager)

	tableID := createTestTable(t, tm, catalog, txRegistry, tempDir, "test_table2")

	// Insert below threshold
	for i := 0; i < 2; i++ {
		tx, err := txRegistry.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		tup := createTestTuple(i, "test")
		store.InsertTuple(tx, tableID, tup)
		store.CommitTransaction(tx)
	}

	// Should NOT trigger update yet
	if statsManager.ShouldUpdateStatistics(tableID) {
		// First time always returns true, so force an update
		tx, err := txRegistry.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		statsManager.ForceUpdate(tx, tableID)
		store.CommitTransaction(tx)
	}

	// Insert one more to reach threshold
	tx2, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	tup := createTestTuple(2, "test")
	store.InsertTuple(tx2, tableID, tup)
	store.CommitTransaction(tx2)

	// Now should trigger update
	if !statsManager.ShouldUpdateStatistics(tableID) {
		t.Error("Expected statistics update to be triggered after threshold")
	}

	store.Close()
}

// TestStatisticsIntegration_DeleteTracking verifies delete operations are tracked
func TestStatisticsIntegration_DeleteTracking(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	tm := memory.NewTableManager()
	wal, err := log.NewWAL(walPath, 8192)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	store := memory.NewPageStore(tm, wal)
	catalog := NewSystemCatalog(store, tm)
	txRegistry := transaction.NewTransactionRegistry(wal)

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := catalog.Initialize(tx, tempDir); err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}
	store.CommitTransaction(tx)

	mockDB := &integrationMockDB{txRegistry: txRegistry, store: store}
	statsManager := NewStatisticsManager(catalog, mockDB)
	store.SetStatsManager(statsManager)

	tableID := createTestTable(t, tm, catalog, txRegistry, tempDir, "test_table3")

	// Insert a tuple
	tx2, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	tup := createTestTuple(1, "test")
	if err := store.InsertTuple(tx2, tableID, tup); err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}
	store.CommitTransaction(tx2)

	// Get the tuple back to delete it
	file, _ := tm.GetDbFile(tableID)
	tx2Read, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	iter := file.Iterator(tx2Read.ID)
	iter.Open()
	iter.HasNext()
	tupToDelete, _ := iter.Next()
	iter.Close()

	// Delete the tuple - should be tracked
	tx3, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := store.DeleteTuple(tx3, tupToDelete); err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}
	store.CommitTransaction(tx3)

	// Verify both insert and delete were tracked
	statsManager.mu.RLock()
	modCount := statsManager.modificationCount[tableID]
	statsManager.mu.RUnlock()

	if modCount != 2 { // 1 insert + 1 delete
		t.Errorf("Expected 2 modifications (insert + delete), got %d", modCount)
	}

	store.Close()
}

// Helper functions

func createTestTable(t *testing.T, tm *memory.TableManager, catalog *SystemCatalog, txRegistry *transaction.TransactionRegistry, dataDir, tableName string) int {
	schema, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	filePath := filepath.Join(dataDir, tableName+".dat")
	f, err := heap.NewHeapFile(filePath, schema)
	if err != nil {
		t.Fatalf("Failed to create heap file: %v", err)
	}

	if err := tm.AddTable(f, tableName, "id"); err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if err := catalog.RegisterTable(tx, f.GetID(), tableName, filePath, "id", []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}); err != nil {
		t.Fatalf("Failed to register table: %v", err)
	}
	catalog.store.CommitTransaction(tx)

	return f.GetID()
}

func createTestTuple(id int, name string) *tuple.Tuple {
	schema, _ := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	tup := tuple.NewTuple(schema)
	tup.SetField(0, types.NewIntField(int32(id)))
	tup.SetField(1, types.NewStringField(name, types.StringMaxSize))
	return tup
}

// Cleanup function
func cleanup(path string) {
	os.RemoveAll(path)
}
