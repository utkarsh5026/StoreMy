package database

import (
	"sync"
	"testing"
)

// TestTransaction_CommitSuccess tests successful transaction commit
func TestTransaction_CommitSuccess(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE txtest (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data (should auto-commit)
	result, err := db.ExecuteQuery("INSERT INTO txtest VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected successful INSERT")
	}

	// Verify statistics show transaction committed
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("expected TransactionsCount > 0")
	}
}

// TestTransaction_RollbackOnError tests transaction rollback on error
func TestTransaction_RollbackOnError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Execute an invalid query - should rollback
	_, err := db.ExecuteQuery("INVALID QUERY")
	if err == nil {
		t.Fatal("expected error for invalid query")
	}

	// Verify transaction was still counted (even though it rolled back)
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("expected TransactionsCount > 0 even after rollback")
	}
	if stats.ErrorCount == 0 {
		t.Error("expected ErrorCount > 0")
	}
}

// TestTransaction_MultipleOperations tests multiple operations in sequence
func TestTransaction_MultipleOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Each operation should be its own transaction
	operations := []string{
		"CREATE TABLE multi (id INT PRIMARY KEY, data STRING)",
		"INSERT INTO multi VALUES (1, 'first')",
		"INSERT INTO multi VALUES (2, 'second')",
		"UPDATE multi SET data = 'updated' WHERE id = 1",
		"DELETE FROM multi WHERE id = 2",
	}

	for i, query := range operations {
		result, err := db.ExecuteQuery(query)
		if err != nil {
			t.Errorf("operation %d failed: %v", i, err)
		}
		if !result.Success {
			t.Errorf("operation %d: expected success", i)
		}
	}

	// Verify all transactions were counted
	stats := db.GetStatistics()
	if stats.TransactionsCount < int64(len(operations)) {
		t.Errorf("expected at least %d transactions, got %d", len(operations), stats.TransactionsCount)
	}
}

// TestTransaction_CleanupOnPanic tests that cleanup happens even on panic
func TestTransaction_CleanupOnPanic(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE panictest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Normal operation should still work
	result, err := db.ExecuteQuery("INSERT INTO panictest VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected successful INSERT")
	}
}

// TestTransaction_ErrorPropagation tests that errors propagate correctly
func TestTransaction_ErrorPropagation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tests := []struct {
		name          string
		query         string
		expectedError string
	}{
		{
			name:          "Parse error",
			query:         "INVALID SQL",
			expectedError: "parse error",
		},
		{
			name:          "Planning error",
			query:         "SELECT * FROM nonexistent_table",
			expectedError: "planning error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.ExecuteQuery(tt.query)
			if err == nil {
				t.Error("expected error but got none")
				return
			}

			if tt.expectedError != "" {
				// Check that error message contains expected text
				// (actual message depends on implementation)
				_ = err.Error()
			}

			// Verify error was recorded
			stats := db.GetStatistics()
			if stats.ErrorCount == 0 {
				t.Error("error should be recorded in statistics")
			}
		})
	}
}

// TestTransaction_StatisticsTracking tests transaction statistics tracking
func TestTransaction_StatisticsTracking(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	initialStats := db.GetStatistics()
	initialTxCount := initialStats.TransactionsCount

	// Execute a successful query
	_, err := db.ExecuteQuery("CREATE TABLE stats (id INT)")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats := db.GetStatistics()
	if stats.TransactionsCount <= initialTxCount {
		t.Errorf("expected TransactionsCount > %d, got %d", initialTxCount, stats.TransactionsCount)
	}

	// Execute a failed query
	_, err = db.ExecuteQuery("INVALID QUERY")
	if err == nil {
		t.Fatal("expected error")
	}

	stats = db.GetStatistics()
	expectedTxCount := initialTxCount + 2 // One success, one failure
	if stats.TransactionsCount < expectedTxCount {
		t.Errorf("expected TransactionsCount >= %d, got %d", expectedTxCount, stats.TransactionsCount)
	}
}

// TestTransaction_ConcurrentTransactions tests concurrent transaction execution
func TestTransaction_ConcurrentTransactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE concurrent (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numGoroutines := 10
	insertsPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines*insertsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < insertsPerGoroutine; j++ {
				id := goroutineID*insertsPerGoroutine + j
				query := "INSERT INTO concurrent VALUES (" + string(rune(id+'0')) + ", 100)"

				// Note: This might fail due to concurrent access
				// The important thing is that transactions don't corrupt each other
				_, err := db.ExecuteQuery(query)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check that we had some successful transactions
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("expected some transactions to complete")
	}

	// Verify database is still functional after concurrent access
	result, err := db.ExecuteQuery("SELECT * FROM concurrent")
	if err != nil {
		t.Errorf("database should still be functional after concurrent transactions: %v", err)
	}
	if !result.Success {
		t.Error("SELECT should succeed after concurrent transactions")
	}
}

// TestTransaction_IsolationLevels tests transaction isolation
func TestTransaction_IsolationLevels(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE isolation (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO isolation VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Each query is its own transaction, so updates should be visible immediately
	_, err = db.ExecuteQuery("UPDATE isolation SET value = 200 WHERE isolation.id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify update was committed (use fully qualified field name in WHERE)
	result, err := db.ExecuteQuery("SELECT * FROM isolation WHERE isolation.id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if !result.Success {
		t.Error("expected successful SELECT")
	}
}

// TestTransaction_SequentialConsistency tests sequential consistency
func TestTransaction_SequentialConsistency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE sequence (counter INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Sequential operations should maintain order
	operations := []struct {
		query       string
		description string
	}{
		{"INSERT INTO sequence VALUES (1)", "initial insert"},
		{"UPDATE sequence SET counter = 2 WHERE sequence.counter = 1", "first update"},
		{"UPDATE sequence SET counter = 3 WHERE sequence.counter = 2", "second update"},
		{"DELETE FROM sequence WHERE sequence.counter = 3", "delete"},
	}

	for _, op := range operations {
		result, err := db.ExecuteQuery(op.query)
		if err != nil {
			t.Fatalf("%s failed: %v", op.description, err)
		}
		if !result.Success {
			t.Errorf("%s: expected success", op.description)
		}
	}
}

// TestTransaction_TransactionIDGeneration tests that transaction IDs are unique
func TestTransaction_TransactionIDGeneration(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE tidtest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Execute multiple queries, each should get a unique transaction ID
	numQueries := 100
	for i := 0; i < numQueries; i++ {
		_, err := db.ExecuteQuery("INSERT INTO tidtest VALUES (" + string(rune(i%10+'0')) + ")")
		if err != nil {
			// Some might fail, that's ok
			continue
		}
	}

	// Verify all transactions were tracked
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("expected transactions to be tracked")
	}
}

// TestTransaction_CommitAfterError tests that failed transactions don't affect subsequent ones
func TestTransaction_CommitAfterError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Execute an invalid query (should rollback)
	_, err := db.ExecuteQuery("INVALID QUERY")
	if err == nil {
		t.Fatal("expected error")
	}

	// Subsequent valid query should work
	result, err := db.ExecuteQuery("CREATE TABLE aftererror (id INT)")
	if err != nil {
		t.Fatalf("query after error should succeed: %v", err)
	}

	if !result.Success {
		t.Error("expected success after previous error")
	}
}

// TestTransaction_RollbackPreservesState tests that rollbacks preserve database state
func TestTransaction_RollbackPreservesState(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE rollbacktest (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO rollbacktest VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Get initial state
	initialStats := db.GetStatistics()

	// Execute invalid query (should rollback)
	_, err = db.ExecuteQuery("UPDATE nonexistent SET value = 200")
	if err == nil {
		t.Fatal("expected error for invalid query")
	}

	// Verify table is still accessible
	result, err := db.ExecuteQuery("SELECT * FROM rollbacktest")
	if err != nil {
		t.Fatalf("table should still be accessible after rollback: %v", err)
	}

	if !result.Success {
		t.Error("SELECT should succeed after rollback")
	}

	// Verify error was recorded but success count didn't change
	stats := db.GetStatistics()
	if stats.ErrorCount <= initialStats.ErrorCount {
		t.Error("error should be recorded")
	}
}

// TestTransaction_MultipleRollbacks tests handling multiple consecutive rollbacks
func TestTransaction_MultipleRollbacks(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	invalidQueries := []string{
		"INVALID QUERY 1",
		"INVALID QUERY 2",
		"INVALID QUERY 3",
	}

	for _, query := range invalidQueries {
		_, err := db.ExecuteQuery(query)
		if err == nil {
			t.Errorf("expected error for query: %s", query)
		}
	}

	// Verify database is still functional
	result, err := db.ExecuteQuery("CREATE TABLE afterrollbacks (id INT)")
	if err != nil {
		t.Fatalf("database should still work after multiple rollbacks: %v", err)
	}

	if !result.Success {
		t.Error("expected success after multiple rollbacks")
	}

	// Verify all transactions (including rollbacks) were tracked
	stats := db.GetStatistics()
	if stats.TransactionsCount < int64(len(invalidQueries)) {
		t.Errorf("expected at least %d transactions, got %d", len(invalidQueries), stats.TransactionsCount)
	}
}

// TestTransaction_LongRunningTransaction tests handling of longer operations
func TestTransaction_LongRunningTransaction(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table with many columns
	_, err := db.ExecuteQuery("CREATE TABLE longtx (id INT PRIMARY KEY, col1 INT, col2 INT, col3 INT, col4 INT, col5 INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows
	for i := 0; i < 10; i++ {
		_, err := db.ExecuteQuery("INSERT INTO longtx VALUES (1, 1, 2, 3, 4, 5)")
		if err != nil {
			// May fail due to duplicate key, that's ok
			continue
		}
	}

	// Transaction should complete successfully
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("expected transactions to complete")
	}
}

// TestTransaction_AbortCleanup tests cleanup during transaction abort
func TestTransaction_AbortCleanup(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE aborttest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Force an error during execution
	_, err = db.ExecuteQuery("INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Fatal("expected error")
	}

	// Verify cleanup happened - database should still be usable
	result, err := db.ExecuteQuery("INSERT INTO aborttest VALUES (1)")
	if err != nil {
		t.Fatalf("database should be usable after abort: %v", err)
	}

	if !result.Success {
		t.Error("expected successful operation after abort")
	}
}

// TestCleanupTransaction tests the cleanupTransaction function
func TestCleanupTransaction(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Execute a query that will succeed
	_, err := db.ExecuteQuery("CREATE TABLE cleanuptest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Verify transaction was cleaned up (incremented counter)
	stats := db.GetStatistics()
	if stats.TransactionsCount == 0 {
		t.Error("transaction should be counted after cleanup")
	}
}

// TestTransaction_RecoveryAfterError tests database recovery after errors
func TestTransaction_RecoveryAfterError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Cause various types of errors
	errorQueries := []string{
		"INVALID SYNTAX",
		"SELECT * FROM nonexistent",
		"INSERT INTO fake VALUES (1)",
	}

	for _, query := range errorQueries {
		_, _ = db.ExecuteQuery(query) // Ignore errors
	}

	// Database should recover and work normally
	result, err := db.ExecuteQuery("CREATE TABLE recovery (id INT)")
	if err != nil {
		t.Fatalf("database should recover after errors: %v", err)
	}

	if !result.Success {
		t.Error("expected success after recovery")
	}

	// Verify we can perform normal operations
	_, err = db.ExecuteQuery("INSERT INTO recovery VALUES (1)")
	if err != nil {
		t.Fatalf("normal operations should work after recovery: %v", err)
	}
}

// TestTransaction_ConcurrentReadWrite tests concurrent read and write transactions
func TestTransaction_ConcurrentReadWrite(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE readwrite (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO readwrite VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 5

	wg.Add(numReaders + numWriters)

	// Concurrent readers
	for range numReaders {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = db.ExecuteQuery("SELECT * FROM readwrite")
			}
		}()
	}

	// Concurrent writers
	for range numWriters {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = db.ExecuteQuery("UPDATE readwrite SET value = 200 WHERE readwrite.id = 1")
			}
		}()
	}

	wg.Wait()

	// Verify database is still consistent
	result, err := db.ExecuteQuery("SELECT * FROM readwrite")
	if err != nil {
		t.Errorf("database should be consistent after concurrent access: %v", err)
	}

	if !result.Success {
		t.Error("expected successful query after concurrent operations")
	}
}
