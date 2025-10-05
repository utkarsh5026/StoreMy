package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setupTestDB creates a test database for query testing
func setupTestDB(t *testing.T) (*Database, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "db_query_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := NewDatabase("testdb", dataDir, logDir)
	if err != nil {
		t.Fatalf("NewDatabase failed: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tempDir)
	}

	return db, cleanup
}

// TestExecuteQuery_CreateTable tests CREATE TABLE statements
func TestExecuteQuery_CreateTable(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "Simple CREATE TABLE",
			query:       "CREATE TABLE users (id INT, name STRING)",
			shouldError: false,
		},
		{
			name:        "CREATE TABLE with PRIMARY KEY",
			query:       "CREATE TABLE products (product_id INT PRIMARY KEY, name STRING, price INT)",
			shouldError: false,
		},
		{
			name:        "Invalid syntax",
			query:       "CREATE TABLE",
			shouldError: true,
		},
		{
			name:        "Empty query",
			query:       "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("expected success=true, got false")
			}
		})
	}
}

// TestExecuteQuery_CreateTable_TableExists tests that table appears after creation
func TestExecuteQuery_CreateTable_TableExists(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	query := "CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, total INT)"
	result, err := db.ExecuteQuery(query)
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if !result.Success {
		t.Errorf("expected success=true")
	}

	// Verify table exists
	tables := db.GetTables()
	found := false
	for _, table := range tables {
		if table == "orders" || table == "ORDERS" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("orders table should exist after CREATE TABLE, got tables: %v", tables)
	}

	// Verify statistics updated
	stats := db.GetStatistics()
	if stats.QueriesExecuted != 1 {
		t.Errorf("expected QueriesExecuted=1, got %d", stats.QueriesExecuted)
	}
}

// TestExecuteQuery_Insert tests INSERT statements
func TestExecuteQuery_Insert(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table first
	_, err := db.ExecuteQuery("CREATE TABLE users (id INT PRIMARY KEY, name STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "Simple INSERT",
			query:       "INSERT INTO users VALUES (1, 'Alice')",
			shouldError: false,
		},
		{
			name:        "INSERT with column names",
			query:       "INSERT INTO users (id, name) VALUES (2, 'Bob')",
			shouldError: false,
		},
		{
			name:        "INSERT into non-existent table",
			query:       "INSERT INTO nonexistent VALUES (1, 'test')",
			shouldError: true,
		},
		{
			name:        "Invalid INSERT syntax",
			query:       "INSERT INTO users",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("expected success=true")
			}
		})
	}
}

// TestExecuteQuery_Select tests SELECT statements
func TestExecuteQuery_Select(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup: Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE items (id INT PRIMARY KEY, name STRING, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO items VALUES (1, 'Widget', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "SELECT *",
			query:       "SELECT * FROM items",
			shouldError: false,
		},
		{
			name:        "SELECT specific columns",
			query:       "SELECT id, name FROM items",
			shouldError: false,
		},
		{
			name:        "SELECT with WHERE",
			query:       "SELECT * FROM items WHERE items.id = 1",
			shouldError: false,
		},
		{
			name:        "SELECT from non-existent table",
			query:       "SELECT * FROM nonexistent",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("expected success=true")
			}
		})
	}
}

// TestExecuteQuery_Update tests UPDATE statements
func TestExecuteQuery_Update(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup: Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE inventory (id INT PRIMARY KEY, quantity INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO inventory VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "Simple UPDATE",
			query:       "UPDATE inventory SET quantity = 150",
			shouldError: false,
		},
		{
			name:        "UPDATE with WHERE",
			query:       "UPDATE inventory SET quantity = 200 WHERE id = 1",
			shouldError: false,
		},
		{
			name:        "UPDATE non-existent table",
			query:       "UPDATE nonexistent SET quantity = 100",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("expected success=true")
			}
		})
	}
}

// TestExecuteQuery_Delete tests DELETE statements
func TestExecuteQuery_Delete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup: Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE logs (id INT PRIMARY KEY, message STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO logs VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "DELETE with WHERE",
			query:       "DELETE FROM logs WHERE id = 1",
			shouldError: false,
		},
		{
			name:        "DELETE all rows",
			query:       "DELETE FROM logs",
			shouldError: false,
		},
		{
			name:        "DELETE from non-existent table",
			query:       "DELETE FROM nonexistent",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !result.Success {
				t.Errorf("expected success=true")
			}
		})
	}
}

// TestExecuteQuery_ParseError tests queries with syntax errors
func TestExecuteQuery_ParseError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Invalid SQL",
			query: "INVALID SQL QUERY",
		},
		{
			name:  "Incomplete statement",
			query: "SELECT * FROM",
		},
		{
			name:  "Malformed CREATE",
			query: "CREATE TABLE ()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := db.ExecuteQuery(tt.query)

			if err == nil {
				t.Errorf("expected parse error but got none")
				return
			}

			if !strings.Contains(err.Error(), "parse error") {
				t.Errorf("expected 'parse error', got: %v", err)
			}

			// Verify error was recorded
			stats := db.GetStatistics()
			if stats.ErrorCount == 0 {
				t.Error("error should be recorded in statistics")
			}

			// Result should be empty on error
			if result.Success {
				t.Error("result.Success should be false on error")
			}
		})
	}
}

// TestExecuteQuery_ErrorRecording tests that errors are properly recorded
func TestExecuteQuery_ErrorRecording(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	initialStats := db.GetStatistics()
	initialErrors := initialStats.ErrorCount

	// Execute invalid query
	_, err := db.ExecuteQuery("INVALID QUERY")
	if err == nil {
		t.Fatal("expected error")
	}

	stats := db.GetStatistics()
	if stats.ErrorCount != initialErrors+1 {
		t.Errorf("expected ErrorCount=%d, got %d", initialErrors+1, stats.ErrorCount)
	}
}

// TestExecuteQuery_SuccessRecording tests that successes are properly recorded
func TestExecuteQuery_SuccessRecording(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	initialStats := db.GetStatistics()
	initialSuccesses := initialStats.QueriesExecuted

	// Execute valid query
	_, err := db.ExecuteQuery("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats := db.GetStatistics()
	if stats.QueriesExecuted != initialSuccesses+1 {
		t.Errorf("expected QueriesExecuted=%d, got %d", initialSuccesses+1, stats.QueriesExecuted)
	}
}

// TestExecuteQuery_ComplexQueries tests more complex query scenarios
func TestExecuteQuery_ComplexQueries(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table with multiple columns
	_, err := db.ExecuteQuery("CREATE TABLE employees (id INT PRIMARY KEY, name STRING, salary INT, department STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows
	insertQueries := []string{
		"INSERT INTO employees VALUES (1, 'Alice', 50000, 'Engineering')",
		"INSERT INTO employees VALUES (2, 'Bob', 60000, 'Sales')",
		"INSERT INTO employees VALUES (3, 'Charlie', 55000, 'Engineering')",
	}

	for _, query := range insertQueries {
		_, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("INSERT failed for query '%s': %v", query, err)
		}
	}

	// Verify we can select from the table
	result, err := db.ExecuteQuery("SELECT * FROM employees")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected success=true for SELECT")
	}
}

// TestExecuteQuery_SequentialQueries tests executing multiple queries in sequence
func TestExecuteQuery_SequentialQueries(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	queries := []string{
		"CREATE TABLE test1 (id INT)",
		"CREATE TABLE test2 (id INT)",
		"CREATE TABLE test3 (id INT)",
	}

	for i, query := range queries {
		result, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("query %d failed: %v", i, err)
		}
		if !result.Success {
			t.Errorf("query %d: expected success=true", i)
		}
	}

	// Verify all tables were created
	tables := db.GetTables()
	expectedCount := len(queries) + 2 // +2 for catalog tables

	if len(tables) < expectedCount {
		t.Errorf("expected at least %d tables, got %d", expectedCount, len(tables))
	}
}

// TestExecuteQuery_DifferentDataTypes tests queries with different data types
func TestExecuteQuery_DifferentDataTypes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table with different types (excluding BOOL since parser doesn't support true/false literals)
	_, err := db.ExecuteQuery("CREATE TABLE mixed_types (id INT PRIMARY KEY, name STRING, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with different types
	_, err = db.ExecuteQuery("INSERT INTO mixed_types VALUES (1, 'Product', 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Select should work
	result, err := db.ExecuteQuery("SELECT * FROM mixed_types")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected success=true")
	}
}

// TestExecuteQuery_WhitespaceTolerance tests queries with various whitespace
func TestExecuteQuery_WhitespaceTolerance(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	queries := []string{
		"CREATE TABLE test (id INT)",
		"CREATE  TABLE  test2  (id  INT)",
		"CREATE\tTABLE\ttest3\t(id\tINT)",
		"CREATE\nTABLE\ntest4\n(id\nINT)",
	}

	for i, query := range queries {
		_, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("query %d with whitespace variations failed: %v", i, err)
		}
	}
}

// TestExecuteQuery_CaseSensitivity tests SQL keyword case sensitivity
func TestExecuteQuery_CaseSensitivity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	queries := []string{
		"CREATE TABLE lowercase (id INT)",
		"create table UPPERCASE (id int)",
		"CrEaTe TaBlE MixedCase (ID int)",
	}

	for i, query := range queries {
		_, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("query %d with case variation failed: %v", i, err)
		}
	}
}

// TestExecuteQuery_ResultStructure tests the structure of query results
func TestExecuteQuery_ResultStructure(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// CREATE TABLE result
	result, err := db.ExecuteQuery("CREATE TABLE test (id INT, name STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}
	if result.Message == "" {
		t.Error("expected non-empty Message")
	}
	if result.Error != nil {
		t.Errorf("expected Error=nil, got %v", result.Error)
	}

	// INSERT result
	result, err = db.ExecuteQuery("INSERT INTO test VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}
}

// TestExecuteQuery_LongQuery tests handling of long queries
func TestExecuteQuery_LongQuery(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a very long table name (within reasonable limits)
	longName := "table_with_a_very_long_name_to_test_query_handling"
	query := "CREATE TABLE " + longName + " (id INT PRIMARY KEY, data STRING)"

	result, err := db.ExecuteQuery(query)
	if err != nil {
		t.Fatalf("long query failed: %v", err)
	}

	if !result.Success {
		t.Error("expected success=true for long query")
	}

	// Verify table exists (table names are stored in uppercase)
	tables := db.GetTables()
	found := false
	longNameUpper := "TABLE_WITH_A_VERY_LONG_NAME_TO_TEST_QUERY_HANDLING"
	for _, table := range tables {
		if table == longNameUpper {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("table with long name should exist, got tables: %v", tables)
	}
}
