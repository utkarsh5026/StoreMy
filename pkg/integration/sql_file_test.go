package integration

import (
	"path/filepath"
	"testing"
)

// TestSQLFile_ImportSchema tests importing SQL schema from file
func TestSQLFile_ImportSchema(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Import schema file
	schemaPath := filepath.Join("testdata", "sample_schema.sql")
	results := td.ExecuteSQLFile(t, schemaPath)

	if len(results) == 0 {
		t.Fatal("expected statements to be executed from SQL file")
	}

	// Verify tables were created
	td.VerifyTableExists(t, "EMPLOYEES")
	td.VerifyTableExists(t, "DEPARTMENTS")

	// Verify data was inserted
	td.VerifyRowCount(t, "SELECT * FROM employees", 5)
	td.VerifyRowCount(t, "SELECT * FROM departments", 4)

	// Verify data integrity
	result := td.MustExecute(t, "SELECT name FROM employees WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 employee with id=1, got %d", len(result.Rows))
	}
}

// TestSQLFile_ComplexQueries tests executing complex queries from file
func TestSQLFile_ComplexQueries(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// First import the schema
	schemaPath := filepath.Join("testdata", "sample_schema.sql")
	td.ExecuteSQLFile(t, schemaPath)

	// Then execute complex queries
	queriesPath := filepath.Join("testdata", "complex_queries.sql")
	results := td.ExecuteSQLFile(t, queriesPath)

	if len(results) == 0 {
		t.Fatal("expected queries to be executed from SQL file")
	}

	// All queries should succeed
	for i, result := range results {
		if !result.Success {
			t.Errorf("query %d should succeed", i)
		}
	}

	// Verify at least some results
	t.Logf("Executed %d queries from file successfully", len(results))
}

// TestSQLFile_StepByStepExecution tests executing SQL file statements one by one
func TestSQLFile_StepByStepExecution(t *testing.T) {
	td := SetupTestDB(t)
	defer td.Cleanup()

	// Create table
	td.MustExecute(t, "CREATE TABLE test_data (id INT PRIMARY KEY, value STRING)")

	// Insert multiple rows
	td.MustExecute(t, "INSERT INTO test_data VALUES (1, 'first')")
	td.MustExecute(t, "INSERT INTO test_data VALUES (2, 'second')")
	td.MustExecute(t, "INSERT INTO test_data VALUES (3, 'third')")

	// Verify each step
	td.VerifyRowCount(t, "SELECT * FROM test_data", 3)

	// Update and verify
	td.MustExecute(t, "UPDATE test_data SET value = 'updated' WHERE id = 2")
	result := td.MustExecute(t, "SELECT value FROM test_data WHERE id = 2")
	if len(result.Rows) > 0 && result.Rows[0][0] != "updated" {
		t.Errorf("expected 'updated', got %s", result.Rows[0][0])
	}

	// Delete and verify
	td.MustExecute(t, "DELETE FROM test_data WHERE id = 1")
	td.VerifyRowCount(t, "SELECT * FROM test_data", 2)
}
