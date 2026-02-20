package tablecache

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
	"time"
)

// mockDbFile implements page.DbFile for testing
type mockDbFile struct {
	id        primitives.FileID
	tupleDesc *tuple.TupleDescription
	closed    bool
	mutex     sync.Mutex
}

func newMockDbFile(id primitives.FileID, fieldTypes []types.Type, fieldNames []string) *mockDbFile {
	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		panic("Failed to create TupleDescription: " + err.Error())
	}
	return &mockDbFile{
		id:        id,
		tupleDesc: td,
		closed:    false,
	}
}

func (m *mockDbFile) ReadPage(pid *page.PageDescriptor) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) WritePage(p page.Page) error {
	return nil
}

func (m *mockDbFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return nil
}

func (m *mockDbFile) GetID() primitives.FileID {
	return m.id
}

func (m *mockDbFile) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

func (m *mockDbFile) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.closed = true
	return nil
}

func (m *mockDbFile) IsClosed() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.closed
}

// Helper function to create a test schema
func createTestSchema(tableName string, tableID primitives.FileID, fields []string) *schema.Schema {
	columns := make([]schema.ColumnMetadata, len(fields))
	for i, fieldName := range fields {
		col, err := schema.NewColumnMetadata(fieldName, types.IntType, primitives.ColumnID(i), tableID, i == 0, false)
		if err != nil {
			panic(fmt.Sprintf("Failed to create column metadata: %v", err))
		}
		columns[i] = *col
	}

	s, err := schema.NewSchema(tableID, tableName, columns)
	if err != nil {
		panic(fmt.Sprintf("Failed to create schema: %v", err))
	}
	return s
}

// TestNewTableCache tests cache creation
func TestNewTableCache(t *testing.T) {
	cache := NewTableCache()

	if cache == nil {
		t.Fatal("NewTableCache returned nil")
	}

	if cache.nameToTable == nil {
		t.Error("nameToTable map not initialized")
	}

	if cache.idToTable == nil {
		t.Error("idToTable map not initialized")
	}

	if cache.lruList == nil {
		t.Error("lruList not initialized")
	}

	if cache.maxSize != 0 {
		t.Errorf("Expected maxSize 0 (unlimited), got %d", cache.maxSize)
	}
}

// TestAddTable_Basic tests basic table addition
func TestAddTable_Basic(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("users", 1, []string{"id", "name"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	err := cache.AddTable(mockFile, testSchema)
	if err != nil {
		t.Fatalf("AddTable failed: %v", err)
	}

	// Verify table exists
	if !cache.TableExists("users") {
		t.Error("Table 'users' should exist in cache")
	}

	// Verify we can retrieve table ID
	tableID, err := cache.GetTableID("users")
	if err != nil {
		t.Errorf("GetTableID failed: %v", err)
	}
	if tableID != primitives.FileID(1) {
		t.Errorf("Expected table ID 1, got %d", tableID)
	}

	// Verify we can retrieve DbFile
	retrievedFile, err := cache.GetDbFile(primitives.FileID(1))
	if err != nil {
		t.Errorf("GetDbFile failed: %v", err)
	}
	if retrievedFile.GetID() != mockFile.GetID() {
		t.Error("Retrieved file does not match original")
	}
}

// TestAddTable_NilFile tests that nil file is rejected
func TestAddTable_NilFile(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("users", 1, []string{"id"})

	err := cache.AddTable(nil, testSchema)
	if err == nil {
		t.Error("AddTable should reject nil file")
	}
}

// TestAddTable_NilSchema tests that nil schema is rejected
func TestAddTable_NilSchema(t *testing.T) {
	cache := NewTableCache()
	mockFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})

	err := cache.AddTable(mockFile, nil)
	if err == nil {
		t.Error("AddTable should reject nil schema")
	}
}

// TestAddTable_EmptyTableName tests that empty table name is rejected
func TestAddTable_EmptyTableName(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	err := cache.AddTable(mockFile, testSchema)
	if err == nil {
		t.Error("AddTable should reject empty table name")
	}
}

// TestAddTable_Replacement tests replacing an existing table
func TestAddTable_Replacement(t *testing.T) {
	cache := NewTableCache()

	// Add first table
	testSchema1 := createTestSchema("users", 1, []string{"id"})
	mockFile1 := newMockDbFile(1, testSchema1.FieldTypes(), testSchema1.FieldNames())
	cache.AddTable(mockFile1, testSchema1)

	// Replace with new table (same name, different ID)
	testSchema2 := createTestSchema("users", 2, []string{"id", "name"})
	mockFile2 := newMockDbFile(2, testSchema2.FieldTypes(), testSchema2.FieldNames())
	cache.AddTable(mockFile2, testSchema2)

	// Verify new table replaced old one
	tableID, err := cache.GetTableID("users")
	if err != nil {
		t.Fatalf("GetTableID failed: %v", err)
	}
	if tableID != primitives.FileID(2) {
		t.Errorf("Expected new table ID 2, got %d", tableID)
	}

	// Verify old ID no longer exists
	_, err = cache.GetDbFile(primitives.FileID(1))
	if err == nil {
		t.Error("Old table ID should not exist in cache")
	}
}

// TestGetTableID_NotFound tests retrieving non-existent table
func TestGetTableID_NotFound(t *testing.T) {
	cache := NewTableCache()

	_, err := cache.GetTableID("nonexistent")
	if err == nil {
		t.Error("GetTableID should return error for non-existent table")
	}
}

// TestGetDbFile_NotFound tests retrieving non-existent file
func TestGetDbFile_NotFound(t *testing.T) {
	cache := NewTableCache()

	_, err := cache.GetDbFile(primitives.FileID(999))
	if err == nil {
		t.Error("GetDbFile should return error for non-existent table ID")
	}
}

// TestRemoveTable tests table removal
func TestRemoveTable(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("temp_table", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	cache.AddTable(mockFile, testSchema)

	// Verify table exists
	if !cache.TableExists("temp_table") {
		t.Fatal("Table should exist before removal")
	}

	// Remove table
	err := cache.RemoveTable("temp_table")
	if err != nil {
		t.Fatalf("RemoveTable failed: %v", err)
	}

	// Verify table no longer exists
	if cache.TableExists("temp_table") {
		t.Error("Table should not exist after removal")
	}

	// Verify file was closed
	if !mockFile.IsClosed() {
		t.Error("File should be closed after table removal")
	}
}

// TestRemoveTable_NotFound tests removing non-existent table
func TestRemoveTable_NotFound(t *testing.T) {
	cache := NewTableCache()

	err := cache.RemoveTable("nonexistent")
	if err == nil {
		t.Error("RemoveTable should return error for non-existent table")
	}
}

// TestClear tests clearing all tables
func TestClear(t *testing.T) {
	cache := NewTableCache()

	// Add multiple tables
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("table%d", i)
		testSchema := createTestSchema(tableName, primitives.FileID(i), []string{"id"})
		mockFile := newMockDbFile(primitives.FileID(i), testSchema.FieldTypes(), testSchema.FieldNames())
		cache.AddTable(mockFile, testSchema)
	}

	// Verify tables exist
	tables := cache.GetAllTableNames()
	if len(tables) != 3 {
		t.Fatalf("Expected 3 tables, got %d", len(tables))
	}

	// Clear cache
	cache.Clear()

	// Verify cache is empty
	tables = cache.GetAllTableNames()
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables after clear, got %d", len(tables))
	}
}

// TestGetAllTableNames tests retrieving all table names
func TestGetAllTableNames(t *testing.T) {
	cache := NewTableCache()

	// Add tables
	expectedTables := map[string]bool{
		"users":    true,
		"products": true,
		"orders":   true,
	}

	i := 1
	for tableName := range expectedTables {
		testSchema := createTestSchema(tableName, primitives.FileID(i), []string{"id"})
		mockFile := newMockDbFile(primitives.FileID(i), testSchema.FieldTypes(), testSchema.FieldNames())
		cache.AddTable(mockFile, testSchema)
		i++
	}

	// Get all table names
	tables := cache.GetAllTableNames()
	if len(tables) != len(expectedTables) {
		t.Errorf("Expected %d tables, got %d", len(expectedTables), len(tables))
	}

	// Verify all expected tables are present
	for _, tableName := range tables {
		if !expectedTables[tableName] {
			t.Errorf("Unexpected table: %s", tableName)
		}
	}
}

// TestValidateIntegrity tests integrity validation
func TestValidateIntegrity(t *testing.T) {
	cache := NewTableCache()

	// Add tables
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("table%d", i)
		testSchema := createTestSchema(tableName, primitives.FileID(i), []string{"id"})
		mockFile := newMockDbFile(primitives.FileID(i), testSchema.FieldTypes(), testSchema.FieldNames())
		cache.AddTable(mockFile, testSchema)
	}

	// Validate integrity
	err := cache.ValidateIntegrity()
	if err != nil {
		t.Errorf("ValidateIntegrity failed: %v", err)
	}
}

// TestValidateIntegrity_Corrupted tests integrity validation with corrupted state
func TestValidateIntegrity_Corrupted(t *testing.T) {
	cache := NewTableCache()

	// Add a table normally
	testSchema := createTestSchema("users", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())
	cache.AddTable(mockFile, testSchema)

	// Corrupt the cache by manually removing from one map
	cache.mutex.Lock()
	delete(cache.idToTable, primitives.FileID(1))
	cache.mutex.Unlock()

	// Validate should fail
	err := cache.ValidateIntegrity()
	if err == nil {
		t.Error("ValidateIntegrity should fail with corrupted cache")
	}
}

// TestRenameTable tests table renaming
func TestRenameTable(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("old_name", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	cache.AddTable(mockFile, testSchema)

	// Rename table
	err := cache.RenameTable("old_name", "new_name")
	if err != nil {
		t.Fatalf("RenameTable failed: %v", err)
	}

	// Verify old name doesn't exist
	if cache.TableExists("old_name") {
		t.Error("Old name should not exist after rename")
	}

	// Verify new name exists
	if !cache.TableExists("new_name") {
		t.Error("New name should exist after rename")
	}

	// Verify table ID unchanged
	tableID, err := cache.GetTableID("new_name")
	if err != nil {
		t.Fatalf("GetTableID failed: %v", err)
	}
	if tableID != primitives.FileID(1) {
		t.Errorf("Table ID should be unchanged, expected 1, got %d", tableID)
	}
}

// TestRenameTable_NotFound tests renaming non-existent table
func TestRenameTable_NotFound(t *testing.T) {
	cache := NewTableCache()

	err := cache.RenameTable("nonexistent", "new_name")
	if err == nil {
		t.Error("RenameTable should return error for non-existent table")
	}
}

// TestRenameTable_DuplicateName tests renaming to existing name
func TestRenameTable_DuplicateName(t *testing.T) {
	cache := NewTableCache()

	// Add two tables
	testSchema1 := createTestSchema("table1", 1, []string{"id"})
	mockFile1 := newMockDbFile(1, testSchema1.FieldTypes(), testSchema1.FieldNames())
	cache.AddTable(mockFile1, testSchema1)

	testSchema2 := createTestSchema("table2", 2, []string{"id"})
	mockFile2 := newMockDbFile(2, testSchema2.FieldTypes(), testSchema2.FieldNames())
	cache.AddTable(mockFile2, testSchema2)

	// Try to rename table1 to table2 (should fail)
	err := cache.RenameTable("table1", "table2")
	if err == nil {
		t.Error("RenameTable should return error when new name already exists")
	}
}

// TestRenameTable_EmptyNames tests renaming with empty names
func TestRenameTable_EmptyNames(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("test", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())
	cache.AddTable(mockFile, testSchema)

	// Empty old name
	err := cache.RenameTable("", "new_name")
	if err == nil {
		t.Error("RenameTable should reject empty old name")
	}

	// Empty new name
	err = cache.RenameTable("test", "")
	if err == nil {
		t.Error("RenameTable should reject empty new name")
	}
}

// TestGetTableInfo tests retrieving full table info
func TestGetTableInfo(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("users", 1, []string{"id", "name"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	cache.AddTable(mockFile, testSchema)

	// Get table info
	info, err := cache.GetTableInfo(primitives.FileID(1))
	if err != nil {
		t.Fatalf("GetTableInfo failed: %v", err)
	}

	if info == nil {
		t.Fatal("GetTableInfo returned nil")
	}

	if info.File.GetID() != mockFile.GetID() {
		t.Error("TableInfo file does not match original")
	}

	if info.Schema.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", info.Schema.TableName)
	}

	if info.GetFileID() != primitives.FileID(1) {
		t.Errorf("Expected file ID 1, got %d", info.GetFileID())
	}
}

// TestLRU_Eviction tests LRU eviction when cache is full
func TestLRU_Eviction(t *testing.T) {
	cache := NewTableCache()
	cache.maxSize = 3 // Set max size to 3 tables

	// Add 3 tables (fill cache)
	for i := 1; i <= 3; i++ {
		tableName := fmt.Sprintf("table%d", i)
		testSchema := createTestSchema(tableName, primitives.FileID(i), []string{"id"})
		mockFile := newMockDbFile(primitives.FileID(i), testSchema.FieldTypes(), testSchema.FieldNames())
		cache.AddTable(mockFile, testSchema)
	}

	// All 3 tables should exist
	if len(cache.idToTable) != 3 {
		t.Fatalf("Expected 3 tables, got %d", len(cache.idToTable))
	}

	// Add 4th table (should evict table1)
	testSchema4 := createTestSchema("table4", 4, []string{"id"})
	mockFile4 := newMockDbFile(4, testSchema4.FieldTypes(), testSchema4.FieldNames())
	cache.AddTable(mockFile4, testSchema4)

	// Should still have 3 tables
	if len(cache.idToTable) != 3 {
		t.Errorf("Expected 3 tables after eviction, got %d", len(cache.idToTable))
	}

	// table1 should be evicted (it was added first and never accessed)
	if cache.TableExists("table1") {
		t.Error("table1 should have been evicted")
	}

	// table4 should exist
	if !cache.TableExists("table4") {
		t.Error("table4 should exist")
	}

	// Eviction counter should be 1
	evictions := cache.metrics.evictions.Load()
	if evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", evictions)
	}
}

// TestLRU_AccessUpdatesPosition tests that accessing a table updates its LRU position
func TestLRU_AccessUpdatesPosition(t *testing.T) {
	cache := NewTableCache()
	cache.maxSize = 2

	// Add 2 tables
	testSchema1 := createTestSchema("table1", 1, []string{"id"})
	mockFile1 := newMockDbFile(1, testSchema1.FieldTypes(), testSchema1.FieldNames())
	cache.AddTable(mockFile1, testSchema1)

	testSchema2 := createTestSchema("table2", 2, []string{"id"})
	mockFile2 := newMockDbFile(2, testSchema2.FieldTypes(), testSchema2.FieldNames())
	cache.AddTable(mockFile2, testSchema2)

	// Access table1 (should move it to front of LRU)
	cache.GetTableID("table1")

	// Add table3 (should evict table2, not table1)
	testSchema3 := createTestSchema("table3", 3, []string{"id"})
	mockFile3 := newMockDbFile(3, testSchema3.FieldTypes(), testSchema3.FieldNames())
	cache.AddTable(mockFile3, testSchema3)

	// table1 should still exist (was recently accessed)
	if !cache.TableExists("table1") {
		t.Error("table1 should still exist (was recently accessed)")
	}

	// table2 should be evicted (was not accessed)
	if cache.TableExists("table2") {
		t.Error("table2 should have been evicted")
	}

	// table3 should exist
	if !cache.TableExists("table3") {
		t.Error("table3 should exist")
	}
}

// TestMetrics tests cache metrics tracking
func TestMetrics(t *testing.T) {
	cache := NewTableCache()
	testSchema := createTestSchema("users", 1, []string{"id"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())
	cache.AddTable(mockFile, testSchema)

	// Initial metrics should be zero
	if cache.metrics.hits.Load() != 0 {
		t.Errorf("Expected 0 hits initially, got %d", cache.metrics.hits.Load())
	}
	if cache.metrics.misses.Load() != 0 {
		t.Errorf("Expected 0 misses initially, got %d", cache.metrics.misses.Load())
	}

	// Access existing table (should increment hits)
	cache.GetTableID("users")
	if cache.metrics.hits.Load() != 1 {
		t.Errorf("Expected 1 hit, got %d", cache.metrics.hits.Load())
	}

	// Access non-existent table (should increment misses)
	cache.GetTableID("nonexistent")
	if cache.metrics.misses.Load() != 1 {
		t.Errorf("Expected 1 miss, got %d", cache.metrics.misses.Load())
	}

	// Access via GetDbFile (should increment hits)
	cache.GetDbFile(1)
	if cache.metrics.hits.Load() != 2 {
		t.Errorf("Expected 2 hits, got %d", cache.metrics.hits.Load())
	}
}

// TestConcurrency tests concurrent access to cache
func TestConcurrency(t *testing.T) {
	cache := NewTableCache()

	var i primitives.FileID
	for i = 1; i <= 10; i++ {
		tableName := fmt.Sprintf("table%d", i)
		testSchema := createTestSchema(tableName, i, []string{"id"})
		mockFile := newMockDbFile(i, testSchema.FieldTypes(), testSchema.FieldNames())
		cache.AddTable(mockFile, testSchema)
	}

	var wg sync.WaitGroup
	numGoroutines := 50
	operationsPerGoroutine := 100

	// Spawn multiple goroutines performing concurrent operations
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < operationsPerGoroutine; i++ {
				tableID := primitives.FileID((i % 10) + 1)
				tableName := fmt.Sprintf("table%d", tableID)

				// Random operations
				switch i % 4 {
				case 0:
					cache.GetTableID(tableName)
				case 1:
					cache.GetDbFile(tableID)
				case 2:
					cache.TableExists(tableName)
				case 3:
					cache.GetTableInfo(tableID)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify integrity after concurrent access
	err := cache.ValidateIntegrity()
	if err != nil {
		t.Errorf("Cache integrity violated after concurrent access: %v", err)
	}
}

// TestConcurrency_WithModifications tests concurrent reads and writes
func TestConcurrency_WithModifications(t *testing.T) {
	cache := NewTableCache()

	var wg sync.WaitGroup
	numGoroutines := 20

	// Some goroutines add tables
	for g := 0; g < numGoroutines/2; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				tableID := primitives.FileID(goroutineID*50 + i)
				tableName := fmt.Sprintf("table_%d_%d", goroutineID, i)
				testSchema := createTestSchema(tableName, tableID, []string{"id"})
				mockFile := newMockDbFile(tableID, testSchema.FieldTypes(), testSchema.FieldNames())
				cache.AddTable(mockFile, testSchema)
			}
		}(g)
	}

	// Other goroutines read tables
	for g := 0; g < numGoroutines/2; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < 100; i++ {
				tableID := primitives.FileID(i % 100)
				cache.GetDbFile(tableID)
				cache.GetTableInfo(tableID)
			}
		}(g)
	}

	wg.Wait()

	// Verify integrity after concurrent modifications
	err := cache.ValidateIntegrity()
	if err != nil {
		t.Errorf("Cache integrity violated after concurrent modifications: %v", err)
	}
}

// TestTableInfo_GetFileID tests TableInfo GetFileID method
func TestTableInfo_GetFileID(t *testing.T) {
	testSchema := createTestSchema("users", 42, []string{"id"})
	mockFile := newMockDbFile(42, testSchema.FieldTypes(), testSchema.FieldNames())

	info := newTableInfo(mockFile, testSchema)

	if info.GetFileID() != 42 {
		t.Errorf("Expected file ID 42, got %d", info.GetFileID())
	}
}

// TestDefaultCacheTTL tests default TTL configuration
func TestDefaultCacheTTL(t *testing.T) {
	config := DefaultCacheTTL()

	if config.StatsTTL != 5*time.Minute {
		t.Errorf("Expected StatsTTL of 5 minutes, got %v", config.StatsTTL)
	}
}

// TestNewTableInfo tests TableInfo creation
func TestNewTableInfo(t *testing.T) {
	testSchema := createTestSchema("users", 1, []string{"id", "name"})
	mockFile := newMockDbFile(1, testSchema.FieldTypes(), testSchema.FieldNames())

	info := newTableInfo(mockFile, testSchema)

	if info == nil {
		t.Fatal("newTableInfo returned nil")
	}

	if info.File != mockFile {
		t.Error("File not set correctly")
	}

	if info.Schema != testSchema {
		t.Error("Schema not set correctly")
	}

	if info.LastAccessed.IsZero() {
		t.Error("LastAccessed should be set")
	}
}
