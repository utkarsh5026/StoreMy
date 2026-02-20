package database

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTestDBInit(t *testing.T, name string) (*Database, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := NewDatabase(name, dataDir, logDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("NewDatabase failed: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, cleanup
}

// TestNewDatabase_Success tests successful database initialization
func TestNewDatabase_Success(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "testdb")
	defer cleanup()

	if db == nil {
		t.Fatal("expected non-nil database")
	}

	if db.name != "testdb" {
		t.Errorf("expected db name 'testdb', got '%s'", db.name)
	}

	// Verify components are initialized
	if db.catalogMgr == nil {
		t.Error("catalogMgr should be initialized")
	}
	if db.pageStore == nil {
		t.Error("pageStore should be initialized")
	}
	if db.queryPlanner == nil {
		t.Error("queryPlanner should be initialized")
	}
	if db.walInstance == nil {
		t.Error("wal should be initialized")
	}
	if db.stats == nil {
		t.Error("stats should be initialized")
	}
}

// TestNewDatabase_DirectoryCreation tests that data directory is created
func TestNewDatabase_DirectoryCreation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := NewDatabase("mydb", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	defer db.Close()

	// Verify data directory was created
	expectedPath := filepath.Join(dataDir, "mydb")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("data directory not created at %s", expectedPath)
	}
}

// TestNewDatabase_InitialStats tests that stats are initialized to zero
func TestNewDatabase_InitialStats(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "statsdb")
	defer cleanup()

	stats := db.GetStatistics()
	if stats.QueriesExecuted != 0 {
		t.Errorf("expected QueriesExecuted=0, got %d", stats.QueriesExecuted)
	}
	if stats.TransactionsCount != 0 {
		t.Errorf("expected TransactionsCount=0, got %d", stats.TransactionsCount)
	}
	if stats.ErrorCount != 0 {
		t.Errorf("expected ErrorCount=0, got %d", stats.ErrorCount)
	}
	if stats.Name != "statsdb" {
		t.Errorf("expected Name='statsdb', got '%s'", stats.Name)
	}
	if stats.TableCount != 0 {
		t.Errorf("expected TableCount=0, got %d", stats.TableCount)
	}
}

// TestNewDatabase_MultipleDatabases tests creating multiple database instances
func TestNewDatabase_MultipleDatabases(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db1, err := NewDatabase("db1", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase db1 failed: %v", err)
	}
	defer db1.Close()

	db2, err := NewDatabase("db2", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase db2 failed: %v", err)
	}
	defer db2.Close()

	if db1.name == db2.name {
		t.Error("database names should be different")
	}

	if db1.dataDir == db2.dataDir {
		t.Error("database data directories should be different")
	}
}

// TestDatabase_GetTables_Empty tests GetTables on empty database
func TestDatabase_GetTables_Empty(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "emptydb")
	defer cleanup()

	tables := db.GetTables()
	// Should have exactly 6 catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_INDEXES, CATALOG_STATISTICS, CATALOG_COLUMN_STATISTICS, CATALOG_INDEX_STATISTICS)
	if len(tables) != 6 {
		t.Errorf("expected 6 catalog tables, got %d", len(tables))
	}

	// Verify they are the catalog tables
	hasCatalogTables := false
	hasCatalogColumns := false
	for _, table := range tables {
		if table == "CATALOG_TABLES" {
			hasCatalogTables = true
		}
		if table == "CATALOG_COLUMNS" {
			hasCatalogColumns = true
		}
	}

	if !hasCatalogTables || !hasCatalogColumns {
		t.Error("expected CATALOG_TABLES and CATALOG_COLUMNS to exist")
	}
}

// TestDatabase_Close tests database cleanup
func TestDatabase_Close(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := NewDatabase("closedb", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// After close, tableManager should be cleared
	tables := db.GetTables()
	if len(tables) != 0 {
		t.Errorf("expected 0 tables after close, got %d", len(tables))
	}
}

// TestDatabase_LoadExistingTables_NoCatalogFile tests loading when no catalog file exists
func TestDatabase_LoadExistingTables_NoCatalogFile(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "nocatalog")
	defer cleanup()

	tables := db.GetTables()
	// Should have exactly 6 catalog tables
	if len(tables) != 6 {
		t.Errorf("expected 6 catalog tables, got %d", len(tables))
	}
}

// TestDatabase_RecordError tests error recording
func TestDatabase_RecordError(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "errordb")
	defer cleanup()
	// Initial error count should be 0
	stats := db.GetStatistics()
	if stats.ErrorCount != 0 {
		t.Errorf("expected ErrorCount=0, got %d", stats.ErrorCount)
	}

	// Record an error
	db.recordError()

	// Error count should increment
	stats = db.GetStatistics()
	if stats.ErrorCount != 1 {
		t.Errorf("expected ErrorCount=1, got %d", stats.ErrorCount)
	}

	// Record multiple errors
	db.recordError()
	db.recordError()

	stats = db.GetStatistics()
	if stats.ErrorCount != 3 {
		t.Errorf("expected ErrorCount=3, got %d", stats.ErrorCount)
	}
}

// TestDatabase_RecordSuccess tests success recording
func TestDatabase_RecordSuccess(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "success")
	defer cleanup()
	// Initial query count should be 0
	stats := db.GetStatistics()
	if stats.QueriesExecuted != 0 {
		t.Errorf("expected QueriesExecuted=0, got %d", stats.QueriesExecuted)
	}

	// Record a success
	db.recordSuccess()

	// Query count should increment
	stats = db.GetStatistics()
	if stats.QueriesExecuted != 1 {
		t.Errorf("expected QueriesExecuted=1, got %d", stats.QueriesExecuted)
	}

	// Record multiple successes
	db.recordSuccess()
	db.recordSuccess()

	stats = db.GetStatistics()
	if stats.QueriesExecuted != 3 {
		t.Errorf("expected QueriesExecuted=3, got %d", stats.QueriesExecuted)
	}
}

// TestDatabase_CatalogInitialization tests that catalog is properly initialized
func TestDatabase_CatalogInitialization(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "catalog")
	defer cleanup()

	// Verify catalog tables exist
	tables := db.GetTables()
	catalogTablesExist := false
	catalogColumnsExist := false

	for _, table := range tables {
		if table == "CATALOG_TABLES" {
			catalogTablesExist = true
		}
		if table == "CATALOG_COLUMNS" {
			catalogColumnsExist = true
		}
	}

	if !catalogTablesExist {
		t.Error("CATALOG_TABLES should exist after initialization")
	}
	if !catalogColumnsExist {
		t.Error("CATALOG_COLUMNS should exist after initialization")
	}
}

// TestDatabase_Name tests database name getter
func TestDatabase_Name(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	tests := []string{"db1", "test_database", "my-db", "db_123"}

	for _, name := range tests {
		db, err := NewDatabase(name, dataDir, logDir)
		if err != nil {
			t.Fatalf("NewDatabase failed for %s: %v", name, err)
		}

		stats := db.GetStatistics()
		if stats.Name != name {
			t.Errorf("expected name '%s', got '%s'", name, stats.Name)
		}

		db.Close()
	}
}

// TestDatabase_GetStatistics_AfterOperations tests statistics tracking
func TestDatabase_GetStatistics_AfterOperations(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "stats")
	defer cleanup()

	// Record some operations
	db.recordSuccess()
	db.recordSuccess()
	db.recordError()

	stats := db.GetStatistics()

	if stats.QueriesExecuted != 2 {
		t.Errorf("expected QueriesExecuted=2, got %d", stats.QueriesExecuted)
	}
	if stats.ErrorCount != 1 {
		t.Errorf("expected ErrorCount=1, got %d", stats.ErrorCount)
	}
	if len(stats.Tables) != stats.TableCount {
		t.Errorf("Tables length (%d) should match TableCount (%d)", len(stats.Tables), stats.TableCount)
	}
}

// TestDatabase_ConcurrentStatsUpdates tests concurrent statistics updates
func TestDatabase_ConcurrentStatsUpdates(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "concurrent")
	defer cleanup()

	numGoroutines := 100
	operationsPerGoroutine := 100

	done := make(chan bool, numGoroutines*2)

	// Concurrent success recording
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < operationsPerGoroutine; j++ {
				db.recordSuccess()
			}
			done <- true
		}()
	}

	// Concurrent error recording
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < operationsPerGoroutine; j++ {
				db.recordError()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines*2; i++ {
		<-done
	}

	stats := db.GetStatistics()
	expectedSuccesses := numGoroutines * operationsPerGoroutine
	expectedErrors := numGoroutines * operationsPerGoroutine

	if stats.QueriesExecuted != int64(expectedSuccesses) {
		t.Errorf("expected QueriesExecuted=%d, got %d", expectedSuccesses, stats.QueriesExecuted)
	}
	if stats.ErrorCount != int64(expectedErrors) {
		t.Errorf("expected ErrorCount=%d, got %d", expectedErrors, stats.ErrorCount)
	}
}

// TestDatabase_DataDirPermissions tests data directory permissions
func TestDatabase_DataDirPermissions(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "db_init_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := NewDatabase("permdb", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}
	defer db.Close()

	dbDataDir := filepath.Join(dataDir, "permdb")
	info, err := os.Stat(dbDataDir)
	if err != nil {
		t.Fatalf("failed to stat data dir: %v", err)
	}

	if !info.IsDir() {
		t.Error("data path should be a directory")
	}

	// On Unix systems, check permissions
	if info.Mode().Perm()&0700 != 0700 {
		t.Logf("Note: data directory permissions are %v (expected at least 0700)", info.Mode().Perm())
	}
}

// TestDatabase_EmptyStringName tests database with empty name
func TestDatabase_EmptyStringName(t *testing.T) {
	db, cleanup := setupTestDBInit(t, "")
	defer cleanup()
	if db.name != "" {
		t.Errorf("expected empty name, got '%s'", db.name)
	}
}
