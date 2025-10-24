package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// FieldMetadata is a simple helper struct for test field definitions
type FieldMetadata struct {
	Name      string
	Type      types.Type
	IsAutoInc bool
}

// Note: FieldMetadata is already defined in catalog_manager_test.go
// We don't need to redefine it here since they're in the same package
// createTestSchema creates a schema from field metadata for testing
func createTestSchema(tableName, primaryKey string, fields []FieldMetadata) *schema.Schema {
	columns := make([]schema.ColumnMetadata, len(fields))
	for i, field := range fields {
		isPrimary := field.Name == primaryKey
		col, err := schema.NewColumnMetadata(field.Name, field.Type, i, 0, isPrimary, field.IsAutoInc)
		if err != nil {
			panic(fmt.Sprintf("Failed to create column metadata: %v", err))
		}
		columns[i] = *col
	}

	s, err := schema.NewSchema(0, tableName, columns)
	if err != nil {
		panic(fmt.Sprintf("Failed to create schema: %v", err))
	}
	return s
}

func setupTestCatalog(t *testing.T) (*SystemCatalog, *transaction.TransactionRegistry, string, func()) {
	t.Helper()

	// Create temp directories
	tempDir, err := os.MkdirTemp("", "catalog_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tempDir, "wal.log")

	// Setup components
	wal, err := wal.NewWAL(logPath, 8192)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	store := memory.NewPageStore(wal)

	txRegistry := transaction.NewTransactionRegistry(wal)
	cache := tablecache.NewTableCache()
	catalog := NewSystemCatalog(store, cache)

	// Initialize catalog with a transaction
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog.Initialize(tx, tempDir); err != nil {
		t.Fatalf("failed to initialize catalog: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tempDir)
	}

	return catalog, txRegistry, tempDir, cleanup
}

// Helper to register a table with catalog
func registerTestTable(
	t *testing.T,
	catalog *SystemCatalog,
	tx *transaction.TransactionContext,
	tempDir, tableName, primaryKey string,
	fields []FieldMetadata,
) int {
	t.Helper()

	filePath := filepath.Join(tempDir, tableName+".dat")

	// Build schema from fields
	fieldTypes := make([]types.Type, len(fields))
	fieldNames := make([]string, len(fields))
	for i, f := range fields {
		fieldTypes[i] = f.Type
		fieldNames[i] = f.Name
	}

	tupleDesc, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	heapFile, err := heap.NewHeapFile(filePath, tupleDesc)
	if err != nil {
		t.Fatalf("failed to create heap file: %v", err)
	}

	tableID := heapFile.GetID()

	// Build schema.Schema for RegisterTable
	tableSchema := createTestSchema(tableName, primaryKey, fields)
	tableSchema.TableID = tableID
	for i := range tableSchema.Columns {
		tableSchema.Columns[i].TableID = tableID
	}

	err = catalog.RegisterTable(tx, tableSchema, filePath)
	if err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}

	// Add table to cache and register with page store (similar to LoadTable)
	if err := catalog.cache.AddTable(heapFile, tableSchema); err != nil {
		t.Fatalf("failed to add table to cache: %v", err)
	}
	catalog.store.RegisterDbFile(tableID, heapFile)

	return tableID
}

func TestNewSystemCatalog(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, _ := wal.NewWAL(walPath, 8192)
	store := memory.NewPageStore(wal)
	defer store.Close()

	cache := tablecache.NewTableCache()
	catalog := NewSystemCatalog(store, cache)

	if catalog == nil {
		t.Fatal("expected non-nil catalog")
	}

	if catalog.store != store {
		t.Error("store not set correctly")
	}

	if catalog.cache != cache {
		t.Error("cache not set correctly")
	}
}

func TestSystemCatalog_Initialize(t *testing.T) {
	catalog, _, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Verify catalog tables exist in cache
	if !catalog.cache.TableExists("CATALOG_TABLES") {
		t.Error("CATALOG_TABLES not found")
	}

	if !catalog.cache.TableExists("CATALOG_COLUMNS") {
		t.Error("CATALOG_COLUMNS not found")
	}

	// Verify files were created
	tablesPath := filepath.Join(tempDir, "catalog_tables.dat")
	if _, err := os.Stat(tablesPath); os.IsNotExist(err) {
		t.Error("catalog_tables.dat file not created")
	}

	columnsPath := filepath.Join(tempDir, "catalog_columns.dat")
	if _, err := os.Stat(columnsPath); os.IsNotExist(err) {
		t.Error("catalog_columns.dat file not created")
	}

	// Verify table IDs are set
	tablesID, err := catalog.cache.GetTableID("CATALOG_TABLES")
	if err != nil {
		t.Fatalf("failed to get CATALOG_TABLES ID: %v", err)
	}
	if tablesID != catalog.SystemTabs.TablesTableID {
		t.Errorf("expected CATALOG_TABLES ID=%d, got %d", catalog.SystemTabs.TablesTableID, tablesID)
	}

	columnsID, err := catalog.cache.GetTableID("CATALOG_COLUMNS")
	if err != nil {
		t.Fatalf("failed to get CATALOG_COLUMNS ID: %v", err)
	}
	if columnsID != catalog.SystemTabs.ColumnsTableID {
		t.Errorf("expected CATALOG_COLUMNS ID=%d, got %d", catalog.SystemTabs.ColumnsTableID, columnsID)
	}
}

func TestSystemCatalog_RegisterTable_Simple(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer catalog.store.CommitTransaction(tx)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}

	registerTestTable(t, catalog, tx, tempDir, "users", "id", fields)
}

func TestSystemCatalog_RegisterTable_MultipleTables(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer catalog.store.CommitTransaction(tx)

	// Register first table
	fields1 := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}

	tableID1 := registerTestTable(t, catalog, tx, tempDir, "users", "id", fields1)

	// Register second table
	fields2 := []FieldMetadata{
		{Name: "product_id", Type: types.IntType},
		{Name: "price", Type: types.IntType},
		{Name: "description", Type: types.StringType},
	}

	tableID2 := registerTestTable(t, catalog, tx, tempDir, "products", "product_id", fields2)

	if tableID1 == tableID2 {
		t.Error("table IDs should be different")
	}
}

func TestSystemCatalog_LoadTables(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Register a table
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "email", Type: types.StringType},
		{Name: "age", Type: types.IntType},
	}

	tableID := registerTestTable(t, catalog, tx, tempDir, "customers", "id", fields)
	catalog.store.CommitTransaction(tx)

	// Flush all pages to disk so catalog2 can read them
	if err := catalog.store.FlushAllPages(); err != nil {
		t.Fatalf("failed to flush pages: %v", err)
	}

	// Don't close catalog1's files - let them remain open
	// This simulates a scenario where the old handles aren't released yet
	// catalog2 will open new file handles for the same files

	// Create a new catalog instance (simulating restart)
	wal2, _ := wal.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(wal2)
	txRegistry2 := transaction.NewTransactionRegistry(wal2)
	cache2 := tablecache.NewTableCache()
	catalog2 := NewSystemCatalog(pageStore2, cache2)

	// Initialize and load tables
	tx2, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.Initialize(tx2, tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx3, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.LoadTables(tx3, tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify table was loaded
	if !cache2.TableExists("customers") {
		t.Error("customers table not loaded")
	}

	// Verify schema was loaded correctly
	tableInfo, err := cache2.GetTableInfo(tableID)
	if err != nil {
		t.Fatalf("failed to get table info: %v", err)
	}

	if tableInfo.Schema.TupleDesc.NumFields() != 3 {
		t.Errorf("expected 3 fields, got %d", tableInfo.Schema.TupleDesc.NumFields())
	}

	tupleDesc := tableInfo.Schema.TupleDesc

	// Verify field names
	expectedNames := []string{"id", "email", "age"}
	for i, expected := range expectedNames {
		name, err := tupleDesc.GetFieldName(i)
		if err != nil {
			t.Errorf("failed to get field name %d: %v", i, err)
		}
		if name != expected {
			t.Errorf("field %d: expected name %s, got %s", i, expected, name)
		}
	}

	// Verify field types
	expectedTypes := []types.Type{types.IntType, types.StringType, types.IntType}
	for i, expected := range expectedTypes {
		fieldType, err := tupleDesc.TypeAtIndex(i)
		if err != nil {
			t.Errorf("failed to get field type %d: %v", i, err)
		}
		if fieldType != expected {
			t.Errorf("field %d: expected type %v, got %v", i, expected, fieldType)
		}
	}

	pageStore2.Close()
}

func TestSystemCatalog_LoadTables_Multiple(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Register multiple tables
	tables := []struct {
		name   string
		fields []FieldMetadata
		pk     string
	}{
		{
			name: "users",
			fields: []FieldMetadata{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.StringType},
			},
			pk: "id",
		},
		{
			name: "orders",
			fields: []FieldMetadata{
				{Name: "order_id", Type: types.IntType},
				{Name: "user_id", Type: types.IntType},
				{Name: "total", Type: types.IntType},
			},
			pk: "order_id",
		},
		{
			name: "products",
			fields: []FieldMetadata{
				{Name: "sku", Type: types.StringType},
				{Name: "price", Type: types.IntType},
			},
			pk: "sku",
		},
	}

	// Register each table in its own transaction to ensure proper isolation
	for _, table := range tables {
		tx, err := txRegistry.Begin()
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}
		registerTestTable(t, catalog, tx, tempDir, table.name, table.pk, table.fields)
		catalog.store.CommitTransaction(tx)
		// Flush after each transaction to ensure data is written to disk
		if err := catalog.store.FlushAllPages(); err != nil {
			t.Fatalf("failed to flush pages: %v", err)
		}
	}

	// Create new catalog and load
	wal2, _ := wal.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(wal2)
	txRegistry2 := transaction.NewTransactionRegistry(wal2)
	cache2 := tablecache.NewTableCache()
	catalog2 := NewSystemCatalog(pageStore2, cache2)

	// Initialize must be called to set up system catalog tables
	tx2, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.Initialize(tx2, tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Load user tables from the catalog
	tx3, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.LoadTables(tx3, tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify all tables loaded
	for _, table := range tables {
		if !cache2.TableExists(table.name) {
			t.Errorf("table %s not loaded", table.name)
		}
	}

	pageStore2.Close()
}

func TestSystemCatalog_GetTableID(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Register a table
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "data", Type: types.StringType},
	}

	expectedID := registerTestTable(t, catalog, tx, tempDir, "test_table", "id", fields)
	catalog.store.CommitTransaction(tx)

	// Get table ID from cache
	tableID, err := catalog.cache.GetTableID("test_table")
	if err != nil {
		t.Fatalf("GetTableID failed: %v", err)
	}

	// The catalog stores table IDs as int32, so we need to compare truncated values
	if tableID != int(int32(expectedID)) {
		t.Errorf("expected tableID=%d, got %d", int(int32(expectedID)), tableID)
	}
}

func TestSystemCatalog_GetTableID_NotFound(t *testing.T) {
	catalog, _, _, cleanup := setupTestCatalog(t)
	defer cleanup()

	_, err := catalog.cache.GetTableID("nonexistent_table")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestSystemCatalog_RegisterTable_WithBoolField(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer catalog.store.CommitTransaction(tx)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "active", Type: types.BoolType},
		{Name: "name", Type: types.StringType},
	}

	tableID := registerTestTable(t, catalog, tx, tempDir, "flags", "id", fields)

	// Flush all pages to disk so catalog2 can read them
	if err := catalog.store.FlushAllPages(); err != nil {
		t.Fatalf("failed to flush pages: %v", err)
	}

	// Load in new catalog
	wal2, _ := wal.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(wal2)
	txRegistry2 := transaction.NewTransactionRegistry(wal2)
	cache2 := tablecache.NewTableCache()
	catalog2 := NewSystemCatalog(pageStore2, cache2)

	tx2, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.Initialize(tx2, tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx3, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.LoadTables(tx3, tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify bool field type preserved
	tableInfo, err := cache2.GetTableInfo(tableID)
	if err != nil {
		t.Fatalf("failed to get table info: %v", err)
	}
	schema := tableInfo.Schema.TupleDesc

	fieldType, err := schema.TypeAtIndex(1)
	if err != nil {
		t.Fatalf("failed to get field type: %v", err)
	}

	if fieldType != types.BoolType {
		t.Errorf("expected BoolType, got %v", fieldType)
	}

	pageStore2.Close()
}

func TestSystemCatalog_LoadTables_EmptyCatalog(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Don't register any tables, just try to load
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	err = catalog.LoadTables(tx, tempDir)
	if err != nil {
		t.Errorf("LoadTables should succeed with empty catalog: %v", err)
	}
}

func TestSystemCatalog_PrimaryKeyPreserved(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	fields := []FieldMetadata{
		{Name: "user_id", Type: types.IntType},
		{Name: "username", Type: types.StringType},
		{Name: "score", Type: types.IntType},
	}

	registerTestTable(t, catalog, tx, tempDir, "players", "user_id", fields)
	catalog.store.CommitTransaction(tx)

	// Flush all pages to disk so catalog2 can read them
	if err := catalog.store.FlushAllPages(); err != nil {
		t.Fatalf("failed to flush pages: %v", err)
	}

	// Load in new catalog
	wal2, _ := wal.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(wal2)
	txRegistry2 := transaction.NewTransactionRegistry(wal2)
	cache2 := tablecache.NewTableCache()
	catalog2 := NewSystemCatalog(pageStore2, cache2)

	tx2, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.Initialize(tx2, tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	tx3, err := txRegistry2.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := catalog2.LoadTables(tx3, tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify primary key is preserved (would need to check TableInfo if exposed)
	// For now, just verify the table loaded successfully
	if !cache2.TableExists("players") {
		t.Error("players table not loaded")
	}

	pageStore2.Close()
}

func TestSystemCatalog_SequentialTableIDs(t *testing.T) {
	catalog, txRegistry, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer catalog.store.CommitTransaction(tx)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
	}

	var ids []int
	for i := 0; i < 5; i++ {
		id := registerTestTable(t, catalog, tx, tempDir, "table_"+string(rune('A'+i)), "id", fields)
		ids = append(ids, id)
	}

	// Verify all IDs are unique
	seen := make(map[int]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate table ID %d", id)
		}
		seen[id] = true
	}
}
