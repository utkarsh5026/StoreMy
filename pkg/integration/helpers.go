package integration

import (
	"os"
	"path/filepath"
	"storemy/pkg/database"
	"testing"
)

// TestDatabase wraps database instance with cleanup
type TestDatabase struct {
	DB      *database.Database
	DataDir string
	LogDir  string
	cleanup func()
}

// SetupTestDB creates a new test database instance with cleanup
func SetupTestDB(t *testing.T) *TestDatabase {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "integration_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dataDir := filepath.Join(tempDir, "data")
	logDir := filepath.Join(tempDir, "logs")

	db, err := database.NewDatabase("testdb", dataDir, logDir)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		t.Fatalf("failed to create database: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Logf("warning: failed to close database: %v", err)
		}
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to remove temp dir: %v", err)
		}
	}

	return &TestDatabase{
		DB:      db,
		DataDir: dataDir,
		LogDir:  logDir,
		cleanup: cleanup,
	}
}

// Cleanup closes the database and removes test files
func (td *TestDatabase) Cleanup() {
	if td.cleanup != nil {
		td.cleanup()
	}
}

// ExecuteSQL executes a SQL query and returns the result
func (td *TestDatabase) ExecuteSQL(t *testing.T, query string) database.QueryResult {
	t.Helper()
	result, err := td.DB.ExecuteQuery(query)
	if err != nil {
		t.Fatalf("query failed: %s\nError: %v", query, err)
	}
	return result
}

// ExecuteSQLExpectError executes a SQL query expecting an error
func (td *TestDatabase) ExecuteSQLExpectError(t *testing.T, query string) error {
	t.Helper()
	_, err := td.DB.ExecuteQuery(query)
	if err == nil {
		t.Fatalf("expected error for query: %s", query)
	}
	return err
}

// MustExecute executes a query and fails the test on error
func (td *TestDatabase) MustExecute(t *testing.T, query string) database.QueryResult {
	t.Helper()
	result, err := td.DB.ExecuteQuery(query)
	if err != nil {
		t.Fatalf("query failed: %s\nError: %v", query, err)
	}
	if !result.Success {
		t.Fatalf("query unsuccessful: %s", query)
	}
	return result
}

// ExecuteSQLFile reads and executes all statements from a SQL file
func (td *TestDatabase) ExecuteSQLFile(t *testing.T, filepath string) []database.QueryResult {
	t.Helper()

	content, err := os.ReadFile(filepath) // #nosec G304
	if err != nil {
		t.Fatalf("failed to read SQL file %s: %v", filepath, err)
	}

	// Simple split by semicolon (can be improved with proper parsing)
	statements := splitSQLStatements(string(content))
	results := make([]database.QueryResult, 0, len(statements))

	for _, stmt := range statements {
		if stmt == "" {
			continue
		}
		result, err := td.DB.ExecuteQuery(stmt)
		if err != nil {
			t.Fatalf("failed to execute statement from %s:\n%s\nError: %v", filepath, stmt, err)
		}
		results = append(results, result)
	}

	return results
}

// VerifyTableExists checks if a table exists
func (td *TestDatabase) VerifyTableExists(t *testing.T, tableName string) {
	t.Helper()
	tables := td.DB.GetTables()
	for _, table := range tables {
		if table == tableName {
			return
		}
	}
	t.Fatalf("table %s does not exist. Available tables: %v", tableName, tables)
}

// VerifyRowCount verifies the number of rows in a query result
func (td *TestDatabase) VerifyRowCount(t *testing.T, query string, expectedCount int) {
	t.Helper()
	result := td.MustExecute(t, query)
	if len(result.Rows) != expectedCount {
		t.Fatalf("expected %d rows, got %d for query: %s", expectedCount, len(result.Rows), query)
	}
}

// VerifyColumnCount verifies the number of columns in a query result
func (td *TestDatabase) VerifyColumnCount(t *testing.T, query string, expectedCount int) {
	t.Helper()
	result := td.MustExecute(t, query)
	if len(result.Columns) != expectedCount {
		t.Fatalf("expected %d columns, got %d for query: %s", expectedCount, len(result.Columns), query)
	}
}

// splitSQLStatements splits SQL content by semicolons (basic implementation)
func splitSQLStatements(content string) []string {
	// This is a simple implementation
	// A production version would need to handle strings, comments, etc.
	var statements []string
	var current string

	for _, line := range splitLines(content) {
		trimmed := trimWhitespace(line)
		if trimmed == "" || startsWithComment(trimmed) {
			continue
		}

		current += line + " "
		if endsWithSemicolon(trimmed) {
			statements = append(statements, current[:len(current)-1])
			current = ""
		}
	}

	if trimWhitespace(current) != "" {
		statements = append(statements, current)
	}

	return statements
}

func splitLines(s string) []string {
	var lines []string
	var current string
	for _, ch := range s {
		if ch == '\n' || ch == '\r' {
			if current != "" || len(lines) == 0 {
				lines = append(lines, current)
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func trimWhitespace(s string) string {
	start := 0
	end := len(s)

	for start < end && isWhitespace(rune(s[start])) {
		start++
	}

	for end > start && isWhitespace(rune(s[end-1])) {
		end--
	}

	return s[start:end]
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func startsWithComment(s string) bool {
	return len(s) >= 2 && s[0] == '-' && s[1] == '-'
}

func endsWithSemicolon(s string) bool {
	return s != "" && s[len(s)-1] == ';'
}
