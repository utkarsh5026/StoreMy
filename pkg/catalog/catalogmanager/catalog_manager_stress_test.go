package catalogmanager

import (
	"fmt"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"sync"
	"testing"
)

// ========================================
// Concurrent Operation Tests
// ========================================

// TestCatalogManager_ConcurrentTableCreation tests creating tables concurrently
func TestCatalogManager_ConcurrentTableCreation(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create multiple tables concurrently
	var wg sync.WaitGroup
	numTables := 10
	errors := make(chan error, numTables)
	successes := make(chan string, numTables)

	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errors <- fmt.Errorf("table concurrent_table_%d: panic: %v", index, r)
				}
			}()

			tableName := fmt.Sprintf("concurrent_table_%d", index)
			fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
			tableSchema := createTestSchema(tableName, "id", fields)

			// Begin transaction and handle errors properly (t.Fatalf is not safe in goroutines)
			tx, err := setup.txRegistry.Begin()
			if err != nil {
				errors <- fmt.Errorf("table %s: begin transaction failed: %w", tableName, err)
				return
			}

			tableID, err := setup.catalogMgr.CreateTable(tx, tableSchema)
			if err != nil {
				errors <- fmt.Errorf("table %s: creation failed: %w", tableName, err)
				return
			}

			// Commit and handle errors properly
			if err := setup.store.CommitTransaction(tx); err != nil {
				errors <- fmt.Errorf("table %s (ID=%d): commit failed: %w", tableName, tableID, err)
			} else {
				successes <- tableName
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	close(successes)

	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Logf("Error during concurrent table creation: %v", err)
	}

	// Count successes
	successCount := 0
	successfulTables := []string{}
	for tableName := range successes {
		successCount++
		successfulTables = append(successfulTables, tableName)
	}

	t.Logf("Successfully created and committed %d tables: %v", successCount, successfulTables)

	if errorCount > 0 {
		t.Errorf("Encountered %d errors during concurrent table creation", errorCount)
	}

	// Flush all pages to ensure writes are visible
	if err := setup.store.FlushAllPages(); err != nil {
		t.Fatalf("FlushAllPages failed: %v", err)
	}

	// Verify all tables were created
	tx2 := setup.beginTx()
	allTables, err := setup.catalogMgr.ListAllTables(tx2, true)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("ListAllTables failed: %v", err)
	}

	createdCount := 0
	createdTables := []string{}
	for _, name := range allTables {
		if len(name) > 17 && name[:17] == "concurrent_table_" {
			createdCount++
			createdTables = append(createdTables, name)
		}
	}

	if createdCount != numTables {
		t.Errorf("Expected %d concurrent tables, got %d. Created tables: %v", numTables, createdCount, createdTables)
	}
}

// TestCatalogManager_ConcurrentIndexCreation tests creating indexes concurrently
func TestCatalogManager_ConcurrentIndexCreation(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table with multiple columns
	fields := []FieldMetadata{
		{Name: "col1", Type: types.IntType},
		{Name: "col2", Type: types.IntType},
		{Name: "col3", Type: types.IntType},
		{Name: "col4", Type: types.IntType},
		{Name: "col5", Type: types.IntType},
	}
	tableSchema := createTestSchema("concurrent_idx_table", "col1", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create multiple indexes concurrently
	var wg sync.WaitGroup
	numIndexes := 5
	errors := make(chan error, numIndexes)

	for i := 0; i < numIndexes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			indexName := fmt.Sprintf("idx_col%d", idx+1)
			columnName := fmt.Sprintf("col%d", idx+1)

			tx := setup.beginTx()
			_, _, err := setup.catalogMgr.CreateIndex(tx, indexName, "concurrent_idx_table", columnName, index.BTreeIndex)
			if err != nil {
				errors <- fmt.Errorf("index %s creation failed: %w", indexName, err)
				return
			}

			// Commit and handle errors properly (t.Fatalf is not safe in goroutines)
			if err := setup.store.CommitTransaction(tx); err != nil {
				errors <- fmt.Errorf("index %s commit failed: %w", indexName, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Logf("Error during concurrent index creation: %v", err)
	}

	if errorCount > 0 {
		t.Errorf("Encountered %d errors during concurrent index creation", errorCount)
	}

	// Verify all indexes were created
	tx3 := setup.beginTx()
	indexes, err := setup.catalogMgr.GetIndexesByTable(tx3, tableID)
	setup.commitTx(tx3)
	if err != nil {
		t.Fatalf("GetIndexesByTable failed: %v", err)
	}

	if len(indexes) != numIndexes {
		t.Errorf("Expected %d indexes, got %d", numIndexes, len(indexes))
	}
}

// TestCatalogManager_ConcurrentReadOperations tests concurrent read operations
func TestCatalogManager_ConcurrentReadOperations(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableSchema := createTestSchema("concurrent_read_table", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Perform concurrent reads
	var wg sync.WaitGroup
	numReads := 50
	errors := make(chan error, numReads)

	for i := 0; i < numReads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx := setup.beginTx()

			// Test various read operations
			_, err := setup.catalogMgr.GetTableID(tx, "concurrent_read_table")
			if err != nil {
				errors <- fmt.Errorf("GetTableID failed: %w", err)
				return
			}

			_, err = setup.catalogMgr.GetTableName(tx, tableID)
			if err != nil {
				errors <- fmt.Errorf("GetTableName failed: %w", err)
				return
			}

			_, err = setup.catalogMgr.GetTableSchema(tx, tableID)
			if err != nil {
				errors <- fmt.Errorf("GetTableSchema failed: %w", err)
				return
			}

			exists := setup.catalogMgr.TableExists(tx, "concurrent_read_table")
			if !exists {
				errors <- fmt.Errorf("TableExists returned false")
				return
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestCatalogManager_ConcurrentStatisticsUpdate tests concurrent statistics updates
func TestCatalogManager_ConcurrentStatisticsUpdate(t *testing.T) {
	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create multiple tables
	numTables := 5
	tableIDs := make([]int, numTables)

	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("stats_concurrent_%d", i)
		fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
		tableSchema := createTestSchema(tableName, "id", fields)

		tx := setup.beginTx()
		tableID, err := setup.catalogMgr.CreateTable(tx, tableSchema)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateTable %d failed: %v", i, err)
		}
		tableIDs[i] = tableID
	}

	// Update statistics concurrently
	var wg sync.WaitGroup
	errors := make(chan error, numTables)

	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func(tableID int) {
			defer wg.Done()

			tx := setup.beginTx()
			err := setup.catalogMgr.UpdateTableStatistics(tx, tableID)
			setup.commitTx(tx)
			if err != nil {
				errors <- fmt.Errorf("UpdateTableStatistics for table %d failed: %w", tableID, err)
			}
		}(tableIDs[i])
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all statistics were created
	for i, tableID := range tableIDs {
		tx := setup.beginTx()
		stats, err := setup.catalogMgr.GetTableStatistics(tx, tableID)
		if err != nil {
			t.Errorf("GetTableStatistics for table %d failed: %v", i, err)
		}
		if stats == nil {
			t.Errorf("Expected statistics for table %d, got nil", i)
		}
	}
}

// ========================================
// Stress Tests
// ========================================

// TestCatalogManager_StressTest_ManyTables tests creating a large number of tables
func TestCatalogManager_StressTest_ManyTables(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create 100 tables
	numTables := 100
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}

	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("stress_table_%d", i)
		tableSchema := createTestSchema(tableName, "id", fields)

		tx := setup.beginTx()
		_, err := setup.catalogMgr.CreateTable(tx, tableSchema)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateTable %d failed: %v", i, err)
		}

		// Periodically check progress
		if (i+1)%20 == 0 {
			t.Logf("Created %d/%d tables", i+1, numTables)
		}
	}

	// Verify all tables exist
	tx2 := setup.beginTx()
	allTables, err := setup.catalogMgr.ListAllTables(tx2, true)
	if err != nil {
		t.Fatalf("ListAllTables failed: %v", err)
	}

	createdCount := 0
	for _, name := range allTables {
		if len(name) > 13 && name[:13] == "stress_table_" {
			createdCount++
		}
	}

	if createdCount != numTables {
		t.Errorf("Expected %d stress tables, got %d", numTables, createdCount)
	}

	t.Logf("Successfully created and verified %d tables", numTables)
}

// TestCatalogManager_StressTest_ManyIndexes tests creating many indexes on a single table
func TestCatalogManager_StressTest_ManyIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create a table with 50 columns
	numColumns := 50
	fields := make([]FieldMetadata, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = FieldMetadata{
			Name: fmt.Sprintf("col%d", i),
			Type: types.IntType,
		}
	}
	fields[0].Name = "id" // First column is primary key

	tableSchema := createTestSchema("many_idx_table", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create an index on each column (except primary key)
	numIndexes := numColumns - 1
	for i := 1; i < numColumns; i++ {
		indexName := fmt.Sprintf("idx_col%d", i)
		columnName := fmt.Sprintf("col%d", i)

		tx := setup.beginTx()
		_, _, err := setup.catalogMgr.CreateIndex(tx, indexName, "many_idx_table", columnName, index.BTreeIndex)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateIndex %d failed: %v", i, err)
		}

		if (i+1)%10 == 0 {
			t.Logf("Created %d/%d indexes", i, numIndexes)
		}
	}

	// Verify all indexes were created
	tx3 := setup.beginTx()
	indexes, err := setup.catalogMgr.GetIndexesByTable(tx3, tableID)
	if err != nil {
		t.Fatalf("GetIndexesByTable failed: %v", err)
	}

	if len(indexes) != numIndexes {
		t.Errorf("Expected %d indexes, got %d", numIndexes, len(indexes))
	}

	t.Logf("Successfully created and verified %d indexes", numIndexes)
}

// TestCatalogManager_StressTest_RepeatedOperations tests repeated create/drop cycles
func TestCatalogManager_StressTest_RepeatedOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Perform 50 create/drop cycles
	cycles := 50
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}

	for i := 0; i < cycles; i++ {
		tableName := "cycle_table"
		tableSchema := createTestSchema(tableName, "id", fields)

		// Create table
		tx := setup.beginTx()
		_, err := setup.catalogMgr.CreateTable(tx, tableSchema)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateTable in cycle %d failed: %v", i, err)
		}

		// Verify table exists
		tx2 := setup.beginTx()
		if !setup.catalogMgr.TableExists(tx2, tableName) {
			t.Fatalf("Table should exist in cycle %d", i)
		}

		// Drop table
		tx3 := setup.beginTx()
		err = setup.catalogMgr.DropTable(tx3, tableName)
		setup.commitTx(tx3)
		if err != nil {
			t.Fatalf("DropTable in cycle %d failed: %v", i, err)
		}

		// Verify table is gone
		if setup.catalogMgr.tableCache.TableExists(tableName) {
			t.Fatalf("Table should not exist after drop in cycle %d", i)
		}

		if (i+1)%10 == 0 {
			t.Logf("Completed %d/%d cycles", i+1, cycles)
		}
	}

	t.Logf("Successfully completed %d create/drop cycles", cycles)
}

// TestCatalogManager_StressTest_ComplexSchema tests tables with very complex schemas
func TestCatalogManager_StressTest_ComplexSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table with 100 columns of various types
	numColumns := 100
	fields := make([]FieldMetadata, numColumns)

	for i := 0; i < numColumns; i++ {
		// Alternate between int and string types
		var fieldType types.Type
		if i%2 == 0 {
			fieldType = types.IntType
		} else {
			fieldType = types.StringType
		}

		fields[i] = FieldMetadata{
			Name:      fmt.Sprintf("field_%d", i),
			Type:      fieldType,
			IsAutoInc: i == 0, // First field is auto-increment
		}
	}

	tableSchema := createTestSchema("complex_schema_table", "field_0", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify schema
	tx3 := setup.beginTx()
	schema, err := setup.catalogMgr.GetTableSchema(tx3, tableID)
	if err != nil {
		t.Fatalf("GetTableSchema failed: %v", err)
	}

	if schema.NumFields() != numColumns {
		t.Errorf("Expected %d fields, got %d", numColumns, schema.NumFields())
	}

	// Verify each column type
	for i := 0; i < numColumns; i++ {
		expectedType := types.IntType
		if i%2 == 1 {
			expectedType = types.StringType
		}

		if schema.Columns[i].FieldType != expectedType {
			t.Errorf("Column %d: expected type %v, got %v", i, expectedType, schema.Columns[i].FieldType)
		}
	}

	t.Logf("Successfully created table with %d columns", numColumns)
}

// TestCatalogManager_StressTest_LoadUnloadCycles tests repeated cache clear and reload
func TestCatalogManager_StressTest_LoadUnloadCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create 10 tables
	numTables := 10
	fields := []FieldMetadata{{Name: "id", Type: types.IntType}}
	tableNames := make([]string, numTables)

	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("load_unload_table_%d", i)
		tableNames[i] = tableName
		tableSchema := createTestSchema(tableName, "id", fields)

		tx := setup.beginTx()
		_, err := setup.catalogMgr.CreateTable(tx, tableSchema)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("CreateTable %d failed: %v", i, err)
		}
	}

	// Perform 20 load/unload cycles
	cycles := 20
	for i := 0; i < cycles; i++ {
		// Clear cache
		setup.catalogMgr.ClearCache()

		// Verify cache is empty
		for _, name := range tableNames {
			if setup.catalogMgr.tableCache.TableExists(name) {
				t.Fatalf("Table %s should not be in cache after clear", name)
			}
		}

		// Reload all tables
		for _, name := range tableNames {
			tx := setup.beginTx()
			err := setup.catalogMgr.LoadTable(tx, name)
			if err != nil {
				t.Fatalf("LoadTable %s in cycle %d failed: %v", name, i, err)
			}
		}

		// Verify all tables are loaded
		for _, name := range tableNames {
			if !setup.catalogMgr.tableCache.TableExists(name) {
				t.Fatalf("Table %s should be in cache after load", name)
			}
		}

		if (i+1)%5 == 0 {
			t.Logf("Completed %d/%d load/unload cycles", i+1, cycles)
		}
	}

	t.Logf("Successfully completed %d load/unload cycles", cycles)
}

// TestCatalogManager_StressTest_AutoIncrementValues tests many auto-increment operations
func TestCatalogManager_StressTest_AutoIncrementValues(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	setup := setupTest(t)
	defer setup.cleanup()

	tx := setup.beginTx()
	if err := setup.catalogMgr.Initialize(tx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Create table with auto-increment
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType, IsAutoInc: true},
		{Name: "data", Type: types.StringType},
	}
	tableSchema := createTestSchema("autoinc_stress", "id", fields)

	tx2 := setup.beginTx()
	tableID, err := setup.catalogMgr.CreateTable(tx2, tableSchema)
	setup.commitTx(tx2)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Increment 1000 times
	increments := 1000
	for i := 1; i <= increments; i++ {
		tx := setup.beginTx()
		err := setup.catalogMgr.IncrementAutoIncrementValue(tx, tableID, "id", i+1)
		setup.commitTx(tx)
		if err != nil {
			t.Fatalf("IncrementAutoIncrementValue at %d failed: %v", i, err)
		}

		if (i+1)%200 == 0 {
			t.Logf("Completed %d/%d increments", i+1, increments)
		}
	}

	// Verify final value
	tx3 := setup.beginTx()
	autoInc, err := setup.catalogMgr.GetAutoIncrementColumn(tx3, tableID)
	if err != nil {
		t.Fatalf("GetAutoIncrementColumn failed: %v", err)
	}

	if autoInc == nil {
		t.Fatal("Expected auto-increment info, got nil")
	}

	if autoInc.NextValue != increments+1 {
		t.Errorf("Expected auto-increment value %d, got %d", increments+1, autoInc.NextValue)
	}

	t.Logf("Successfully completed %d auto-increment operations", increments)
}
