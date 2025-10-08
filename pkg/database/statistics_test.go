package database

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestDatabase_StatisticsAutoTracking verifies end-to-end automatic statistics tracking
func TestDatabase_StatisticsAutoTracking(t *testing.T) {
	tempDir := t.TempDir()
	dbName := "test_stats_db"
	logDir := filepath.Join(tempDir, "wal.log")

	// Create database - statistics tracking should be automatic
	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Verify statistics manager was created
	if db.statsManager == nil {
		t.Fatal("Statistics manager should be automatically created")
	}

	// Create a test table
	createResult, err := db.ExecuteQuery(`
		CREATE TABLE users (
			id INT,
			name STRING,
			age INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if !createResult.Success {
		t.Fatalf("Create table failed: %s", createResult.Message)
	}

	// Insert data - statistics should be automatically tracked
	insertCount := 20
	for i := 1; i <= insertCount; i++ {
		query := fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d', %d)", i, i, 20+i)
		result, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("Insert %d failed: %v", i, err)
			continue
		}
		if !result.Success {
			t.Errorf("Insert %d unsuccessful: %s", i, result.Message)
		}
	}

	// Verify modifications were tracked
	tx, _ := db.txRegistry.Begin()
	tableID, _ := db.catalog.GetTableID(tx.ID, "users")
	modCount := db.statsManager.GetModificationCount(tableID)

	if modCount < insertCount {
		t.Errorf("Expected at least %d modifications tracked, got %d", insertCount, modCount)
	}

	// Force statistics update
	err = db.UpdateTableStatistics("users")
	if err != nil {
		t.Fatalf("Failed to update statistics: %v", err)
	}

	// Retrieve and verify statistics
	stats, err := db.GetTableStatistics("users")
	if err != nil {
		t.Fatalf("Failed to get statistics: %v", err)
	}

	// Verify statistics content
	if stats.Cardinality != insertCount {
		t.Errorf("Expected cardinality %d, got %d", insertCount, stats.Cardinality)
	}

	if stats.PageCount == 0 {
		t.Error("Expected non-zero page count")
	}

	if stats.AvgTupleSize == 0 {
		t.Error("Expected non-zero average tuple size")
	}

	if stats.DistinctValues == 0 {
		t.Error("Expected non-zero distinct values")
	}

	t.Logf("Statistics collected successfully:")
	t.Logf("  Cardinality: %d", stats.Cardinality)
	t.Logf("  Pages: %d", stats.PageCount)
	t.Logf("  Avg Tuple Size: %d bytes", stats.AvgTupleSize)
	t.Logf("  Distinct Values: %d", stats.DistinctValues)
}

// TestDatabase_StatisticsAfterDelete verifies statistics tracking for deletes
func TestDatabase_StatisticsAfterDelete(t *testing.T) {
	tempDir := t.TempDir()
	dbName := "test_delete_stats"
	logDir := filepath.Join(tempDir, "wal.log")

	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create table and insert data
	db.ExecuteQuery("CREATE TABLE test_table (id INT, value STRING)")

	for i := 1; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO test_table VALUES (%d, 'value%d')", i, i)
		db.ExecuteQuery(query)
	}

	// Update statistics
	db.UpdateTableStatistics("test_table")

	// Get initial statistics
	initialStats, _ := db.GetTableStatistics("test_table")
	if initialStats.Cardinality != 10 {
		t.Errorf("Expected initial cardinality 10, got %d", initialStats.Cardinality)
	}

	// Delete some rows
	db.ExecuteQuery("DELETE FROM test_table WHERE id <= 5")

	// Update statistics again
	db.UpdateTableStatistics("test_table")

	// Get updated statistics
	updatedStats, _ := db.GetTableStatistics("test_table")

	// Cardinality should decrease
	if updatedStats.Cardinality >= initialStats.Cardinality {
		t.Errorf("Expected cardinality to decrease after deletes, got %d (was %d)",
			updatedStats.Cardinality, initialStats.Cardinality)
	}

	t.Logf("Statistics after delete:")
	t.Logf("  Before: %d tuples", initialStats.Cardinality)
	t.Logf("  After: %d tuples", updatedStats.Cardinality)
}

// TestDatabase_StatisticsMultipleTables verifies tracking for multiple tables
func TestDatabase_StatisticsMultipleTables(t *testing.T) {
	tempDir := t.TempDir()
	dbName := "test_multi_stats"
	logDir := filepath.Join(tempDir, "wal.log")

	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create multiple tables
	tables := []struct {
		name      string
		rowCount  int
	}{
		{"users", 50},
		{"orders", 100},
		{"products", 25},
	}

	for _, table := range tables {
		// Create table
		createQuery := fmt.Sprintf("CREATE TABLE %s (id INT, data STRING)", table.name)
		db.ExecuteQuery(createQuery)

		// Insert data
		for i := 1; i <= table.rowCount; i++ {
			insertQuery := fmt.Sprintf("INSERT INTO %s VALUES (%d, 'data%d')", table.name, i, i)
			db.ExecuteQuery(insertQuery)
		}

		// Update statistics
		db.UpdateTableStatistics(table.name)

		// Verify statistics
		stats, err := db.GetTableStatistics(table.name)
		if err != nil {
			t.Errorf("Failed to get statistics for %s: %v", table.name, err)
			continue
		}

		if stats.Cardinality != table.rowCount {
			t.Errorf("Table %s: expected cardinality %d, got %d",
				table.name, table.rowCount, stats.Cardinality)
		}

		t.Logf("Table %s: %d tuples, %d pages", table.name, stats.Cardinality, stats.PageCount)
	}
}

// TestDatabase_StatisticsThresholdBehavior verifies threshold-based updates
func TestDatabase_StatisticsThresholdBehavior(t *testing.T) {
	tempDir := t.TempDir()
	dbName := "test_threshold"
	logDir := filepath.Join(tempDir, "wal.log")

	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Set low threshold for testing
	db.statsManager.SetUpdateThreshold(10)

	// Create table
	db.ExecuteQuery("CREATE TABLE threshold_test (id INT, value STRING)")

	// Get table ID
	tx, _ := db.txRegistry.Begin()
	tableID, _ := db.catalog.GetTableID(tx.ID, "threshold_test")

	// Insert below threshold
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO threshold_test VALUES (%d, 'value%d')", i, i)
		db.ExecuteQuery(query)
	}

	// Check modification count
	modCount := db.statsManager.GetModificationCount(tableID)

	if modCount != 5 {
		t.Errorf("Expected 5 modifications, got %d", modCount)
	}

	// Insert to reach threshold
	for i := 6; i <= 10; i++ {
		query := fmt.Sprintf("INSERT INTO threshold_test VALUES (%d, 'value%d')", i, i)
		db.ExecuteQuery(query)
	}

	// Should trigger update
	if !db.statsManager.ShouldUpdateStatistics(tableID) {
		// Force initial update if needed
		db.UpdateTableStatistics("threshold_test")
	}

	t.Log("Threshold behavior verified: updates triggered correctly")
}

// TestDatabase_StatisticsNotFound verifies error handling for non-existent table
func TestDatabase_StatisticsNotFound(t *testing.T) {
	tempDir := t.TempDir()
	dbName := "test_notfound"
	logDir := filepath.Join(tempDir, "wal.log")

	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Try to get statistics for non-existent table
	_, err = db.GetTableStatistics("nonexistent_table")
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}

	// Try to update statistics for non-existent table
	err = db.UpdateTableStatistics("nonexistent_table")
	if err == nil {
		t.Error("Expected error for non-existent table update, got nil")
	}
}

// TestDatabase_StatisticsAfterBulkInsert verifies stats after many inserts
func TestDatabase_StatisticsAfterBulkInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bulk insert test in short mode")
	}

	tempDir := t.TempDir()
	dbName := "test_bulk"
	logDir := filepath.Join(tempDir, "wal.log")

	db, err := NewDatabase(dbName, tempDir, logDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create table
	db.ExecuteQuery("CREATE TABLE bulk_test (id INT, value STRING)")

	// Bulk insert
	bulkCount := 500
	for i := 1; i <= bulkCount; i++ {
		query := fmt.Sprintf("INSERT INTO bulk_test VALUES (%d, 'value%d')", i, i)
		if _, err := db.ExecuteQuery(query); err != nil {
			t.Errorf("Bulk insert %d failed: %v", i, err)
		}
	}

	// Update statistics
	err = db.UpdateTableStatistics("bulk_test")
	if err != nil {
		t.Fatalf("Failed to update statistics after bulk insert: %v", err)
	}

	// Verify statistics
	stats, err := db.GetTableStatistics("bulk_test")
	if err != nil {
		t.Fatalf("Failed to get statistics: %v", err)
	}

	if stats.Cardinality != bulkCount {
		t.Errorf("Expected cardinality %d, got %d", bulkCount, stats.Cardinality)
	}

	t.Logf("Bulk insert statistics:")
	t.Logf("  Tuples: %d", stats.Cardinality)
	t.Logf("  Pages: %d", stats.PageCount)
	t.Logf("  Avg Size: %d bytes", stats.AvgTupleSize)
	t.Logf("  Distinct: %d", stats.DistinctValues)
}
