package database

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrency_MultipleReaders tests concurrent read operations
func TestConcurrency_MultipleReaders(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup: Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE readers (id INT PRIMARY KEY, data STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO readers VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	numReaders := 50
	readsPerReader := 10

	var wg sync.WaitGroup
	wg.Add(numReaders)

	errors := int32(0)
	successes := int32(0)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < readsPerReader; j++ {
				result, err := db.ExecuteQuery("SELECT * FROM readers")
				if err != nil {
					atomic.AddInt32(&errors, 1)
				} else if result.Success {
					atomic.AddInt32(&successes, 1)
				}
			}
		}()
	}

	wg.Wait()

	if successes == 0 {
		t.Error("expected some successful reads")
	}

	t.Logf("Concurrent reads: %d successes, %d errors", successes, errors)
}

// TestConcurrency_MultipleWriters tests concurrent write operations
func TestConcurrency_MultipleWriters(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE writers (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numWriters := 10
	writesPerWriter := 5

	var wg sync.WaitGroup
	wg.Add(numWriters)

	successfulInserts := int32(0)
	totalAttempts := int32(0)

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < writesPerWriter; j++ {
				// Each writer tries to insert with unique IDs to avoid conflicts
				id := writerID*writesPerWriter + j
				result, err := db.ExecuteQuery(fmt.Sprintf("INSERT INTO writers VALUES (%d, %d)", id, writerID))
				atomic.AddInt32(&totalAttempts, 1)
				if err == nil && result.Success {
					atomic.AddInt32(&successfulInserts, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still functional
	result, err := db.ExecuteQuery("SELECT * FROM writers")
	if err != nil {
		t.Errorf("database should be functional after concurrent writes: %v", err)
	}

	if !result.Success {
		t.Error("expected successful query after concurrent writes")
	}

	// At least some inserts should have succeeded
	if successfulInserts == 0 {
		t.Error("expected some successful inserts")
	}

	t.Logf("Concurrent writes: %d successful inserts out of %d attempts", successfulInserts, totalAttempts)
}

// TestConcurrency_MixedReadWrite tests mixed read and write operations
func TestConcurrency_MixedReadWrite(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup
	_, err := db.ExecuteQuery("CREATE TABLE mixed (id INT PRIMARY KEY, counter INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO mixed VALUES (1, 0)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	numWorkers := 5
	opsPerWorker := 5

	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Readers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				_, _ = db.ExecuteQuery("SELECT * FROM mixed")
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Writers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				_, _ = db.ExecuteQuery("UPDATE mixed SET counter = 1 WHERE id = 1")
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// Verify final state
	result, err := db.ExecuteQuery("SELECT * FROM mixed")
	if err != nil {
		t.Errorf("final query should succeed: %v", err)
	}

	if !result.Success {
		t.Error("expected successful final query")
	}
}

// TestConcurrency_TableCreation tests concurrent table creation
func TestConcurrency_TableCreation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	numTables := 20

	var wg sync.WaitGroup
	wg.Add(numTables)

	created := int32(0)
	failed := int32(0)

	for i := 0; i < numTables; i++ {
		go func(tableNum int) {
			defer wg.Done()

			tableName := "table_" + string(rune(tableNum%10+'0'))
			query := "CREATE TABLE " + tableName + " (id INT PRIMARY KEY)"

			_, err := db.ExecuteQuery(query)
			if err != nil {
				atomic.AddInt32(&failed, 1)
			} else {
				atomic.AddInt32(&created, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still functional
	tables := db.GetTables()
	t.Logf("Created: %d, Failed: %d, Total tables: %d", created, failed, len(tables))

	// Database should still be accessible
	stats := db.GetStatistics()
	if stats.TableCount == 0 {
		t.Error("expected at least some tables to be created")
	}
}

// TestConcurrency_StatisticsUpdates tests concurrent statistics updates
func TestConcurrency_StatisticsUpdates(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE stattest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numWorkers := 50
	opsPerWorker := 20

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < opsPerWorker; j++ {
				// Mix of successful and failing operations
				_, _ = db.ExecuteQuery("INSERT INTO stattest VALUES (1)")
				_, _ = db.ExecuteQuery("INVALID QUERY")
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify statistics
	stats := db.GetStatistics()

	if stats.QueriesExecuted == 0 && stats.ErrorCount == 0 {
		t.Error("expected some operations to be recorded")
	}

	t.Logf("Stats: Queries=%d, Errors=%d, Transactions=%d",
		stats.QueriesExecuted, stats.ErrorCount, stats.TransactionsCount)
}

// TestConcurrency_GetTables tests concurrent GetTables calls
func TestConcurrency_GetTables(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create some tables
	for i := 0; i < 5; i++ {
		tableName := "concurrent_" + string(rune(i+'0'))
		_, _ = db.ExecuteQuery("CREATE TABLE " + tableName + " (id INT)")
	}

	numWorkers := 30
	callsPerWorker := 100

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < callsPerWorker; j++ {
				tables := db.GetTables()
				if tables == nil {
					t.Error("GetTables should not return nil")
				}
			}
		}()
	}

	wg.Wait()
}

// TestConcurrency_GetStatistics tests concurrent GetStatistics calls
func TestConcurrency_GetStatistics(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	numWorkers := 50
	callsPerWorker := 100

	var wg sync.WaitGroup
	wg.Add(numWorkers * 2)

	// Workers reading statistics
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < callsPerWorker; j++ {
				stats := db.GetStatistics()
				if stats.Name != "testdb" {
					t.Errorf("expected Name='testdb', got '%s'", stats.Name)
				}
			}
		}()
	}

	// Workers modifying state
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < callsPerWorker/10; j++ {
				db.recordSuccess()
				db.recordError()
			}
		}()
	}

	wg.Wait()

	// Final statistics should be consistent
	stats := db.GetStatistics()
	if stats.QueriesExecuted < 0 || stats.ErrorCount < 0 {
		t.Error("statistics should not be negative")
	}
}

// TestConcurrency_DatabaseClose tests concurrent close operations
func TestConcurrency_DatabaseClose(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create and populate table
	_, err := db.ExecuteQuery("CREATE TABLE closetest (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numWorkers := 10

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Start workers that will be interrupted by close
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				_, _ = db.ExecuteQuery("SELECT * FROM closetest")
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Let workers run for a bit
	time.Sleep(10 * time.Millisecond)

	// Close should not panic
	err = db.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	wg.Wait()
}

// TestConcurrency_TransactionIsolation tests transaction isolation
func TestConcurrency_TransactionIsolation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE isolation (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.ExecuteQuery("INSERT INTO isolation VALUES (1, 0)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	numWorkers := 10
	updatesPerWorker := 10

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < updatesPerWorker; j++ {
				// Each update is its own transaction
				_, _ = db.ExecuteQuery("UPDATE isolation SET value = value + 1 WHERE id = 1")
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	// Verify database is consistent
	result, err := db.ExecuteQuery("SELECT * FROM isolation")
	if err != nil {
		t.Errorf("final SELECT should succeed: %v", err)
	}

	if !result.Success {
		t.Error("expected successful SELECT after concurrent updates")
	}
}

// TestConcurrency_RapidFireQueries tests rapid sequential queries
func TestConcurrency_RapidFireQueries(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE rapidfire (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numQueries := 1000
	successCount := 0

	for i := 0; i < numQueries; i++ {
		result, err := db.ExecuteQuery("SELECT * FROM rapidfire")
		if err == nil && result.Success {
			successCount++
		}
	}

	if successCount < numQueries/2 {
		t.Errorf("expected at least %d successful queries, got %d", numQueries/2, successCount)
	}

	t.Logf("Rapid fire: %d/%d queries succeeded", successCount, numQueries)
}

// TestConcurrency_MemoryConsistency tests memory consistency under concurrent load
func TestConcurrency_MemoryConsistency(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE memtest (id INT PRIMARY KEY, data STRING)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numWorkers := 20
	duration := 500 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	stop := make(chan struct{})

	go func() {
		time.Sleep(duration)
		close(stop)
	}()

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
					// Mix of operations
					_, _ = db.ExecuteQuery("INSERT INTO memtest VALUES (1, 'data')")
					_, _ = db.ExecuteQuery("SELECT * FROM memtest")
					_, _ = db.ExecuteQuery("UPDATE memtest SET data = 'updated'")
					_, _ = db.ExecuteQuery("DELETE FROM memtest WHERE id = 1")
					time.Sleep(time.Microsecond * 10)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still consistent
	stats := db.GetStatistics()
	if stats.QueriesExecuted < 0 {
		t.Error("QueriesExecuted should not be negative")
	}

	if stats.TransactionsCount < 0 {
		t.Error("TransactionsCount should not be negative")
	}

	// Database should still be functional
	result, err := db.ExecuteQuery("SELECT * FROM memtest")
	if err != nil {
		t.Errorf("database should be functional after concurrent load: %v", err)
	}

	if !result.Success {
		t.Error("expected successful query after concurrent load")
	}
}

// TestConcurrency_StressTest tests database under heavy concurrent load
func TestConcurrency_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create multiple tables
	for i := 0; i < 5; i++ {
		tableName := "stress_" + string(rune(i+'0'))
		_, _ = db.ExecuteQuery("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, value INT)")
	}

	numWorkers := 50
	duration := 2 * time.Second

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	stop := make(chan struct{})
	totalOps := int64(0)

	go func() {
		time.Sleep(duration)
		close(stop)
	}()

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			operations := []string{
				"INSERT INTO stress_0 VALUES (1, 100)",
				"SELECT * FROM stress_1",
				"UPDATE stress_2 SET value = 200",
				"DELETE FROM stress_3 WHERE id = 1",
				"SELECT * FROM stress_4",
			}

			for {
				select {
				case <-stop:
					return
				default:
					for _, op := range operations {
						_, _ = db.ExecuteQuery(op)
						atomic.AddInt64(&totalOps, 1)
					}
					time.Sleep(time.Microsecond * 100)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify database survived stress test
	for i := 0; i < 5; i++ {
		tableName := "stress_" + string(rune(i+'0'))
		result, err := db.ExecuteQuery("SELECT * FROM " + tableName)
		if err != nil {
			t.Errorf("table %s should be accessible after stress test: %v", tableName, err)
		}
		if !result.Success {
			t.Errorf("SELECT from %s should succeed after stress test", tableName)
		}
	}

	stats := db.GetStatistics()
	t.Logf("Stress test completed: %d operations, %d queries executed, %d errors",
		totalOps, stats.QueriesExecuted, stats.ErrorCount)
}

// TestConcurrency_ErrorHandling tests concurrent error handling
func TestConcurrency_ErrorHandling(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	numWorkers := 20
	errorsPerWorker := 10

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < errorsPerWorker; j++ {
				// Intentionally invalid queries
				_, _ = db.ExecuteQuery("INVALID QUERY")
				_, _ = db.ExecuteQuery("SELECT * FROM nonexistent")
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify error counting
	stats := db.GetStatistics()
	if stats.ErrorCount == 0 {
		t.Error("expected errors to be recorded")
	}

	// Database should still work after many errors
	_, err := db.ExecuteQuery("CREATE TABLE aftererrors (id INT)")
	if err != nil {
		t.Errorf("database should recover from concurrent errors: %v", err)
	}
}

// TestConcurrency_DataRaces tests for potential data races
func TestConcurrency_DataRaces(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	_, err := db.ExecuteQuery("CREATE TABLE racetest (id INT PRIMARY KEY, counter INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	numWorkers := 30

	var wg sync.WaitGroup
	wg.Add(numWorkers * 3)

	// Concurrent readers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = db.GetTables()
				_ = db.GetStatistics()
			}
		}()
	}

	// Concurrent writers
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = db.ExecuteQuery("INSERT INTO racetest VALUES (1, 0)")
				_, _ = db.ExecuteQuery("UPDATE racetest SET counter = 1")
			}
		}()
	}

	// Concurrent statistics updates
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				db.recordSuccess()
				db.recordError()
			}
		}()
	}

	wg.Wait()

	// If we get here without panicking or race detector errors, test passes
	t.Log("Data race test completed successfully")
}
