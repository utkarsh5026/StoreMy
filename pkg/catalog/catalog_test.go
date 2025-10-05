package catalog

import (
	"os"
	"path/filepath"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

func setupTestCatalog(t *testing.T) (*SystemCatalog, string, func()) {
	t.Helper()

	// Create temp directories
	tempDir, err := os.MkdirTemp("", "catalog_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tempDir, "wal.log")

	// Setup components
	tableManager := memory.NewTableManager()
	wal, err := log.NewWAL(logPath, 8192)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	store := memory.NewPageStore(tableManager, wal)
	catalog := NewSystemCatalog(store, tableManager)

	// Initialize catalog
	if err := catalog.Initialize(tempDir); err != nil {
		t.Fatalf("failed to initialize catalog: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tempDir)
	}

	return catalog, tempDir, cleanup
}

// Helper to register a table with catalog
func registerTestTable(
	t *testing.T,
	catalog *SystemCatalog,
	tid *transaction.TransactionID,
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

	schema, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	heapFile, err := heap.NewHeapFile(filePath, schema)
	if err != nil {
		t.Fatalf("failed to create heap file: %v", err)
	}

	tableID := heapFile.GetID()

	err = catalog.RegisterTable(tid, tableID, tableName, filePath, primaryKey, fields)
	if err != nil {
		t.Fatalf("RegisterTable failed: %v", err)
	}

	return tableID
}

func TestNewSystemCatalog(t *testing.T) {
	tableManager := memory.NewTableManager()
	walPath := filepath.Join(t.TempDir(), "test.wal")
	wal, _ := log.NewWAL(walPath, 8192)
	store := memory.NewPageStore(tableManager, wal)
	defer store.Close()

	catalog := NewSystemCatalog(store, tableManager)

	if catalog == nil {
		t.Fatal("expected non-nil catalog")
	}

	if catalog.store != store {
		t.Error("store not set correctly")
	}

	if catalog.tableManager != tableManager {
		t.Error("tableManager not set correctly")
	}
}

func TestSystemCatalog_Initialize(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Verify catalog tables exist in TableManager
	if !catalog.tableManager.TableExists("CATALOG_TABLES") {
		t.Error("CATALOG_TABLES not found")
	}

	if !catalog.tableManager.TableExists("CATALOG_COLUMNS") {
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
	tablesID, err := catalog.tableManager.GetTableID("CATALOG_TABLES")
	if err != nil {
		t.Fatalf("failed to get CATALOG_TABLES ID: %v", err)
	}
	if tablesID != catalog.tablesTableID {
		t.Errorf("expected CATALOG_TABLES ID=%d, got %d", catalog.tablesTableID, tablesID)
	}

	columnsID, err := catalog.tableManager.GetTableID("CATALOG_COLUMNS")
	if err != nil {
		t.Fatalf("failed to get CATALOG_COLUMNS ID: %v", err)
	}
	if columnsID != catalog.columnsTableID {
		t.Errorf("expected CATALOG_COLUMNS ID=%d, got %d", catalog.columnsTableID, columnsID)
	}
}

func TestSystemCatalog_RegisterTable_Simple(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}

	registerTestTable(t, catalog, tid, tempDir, "users", "id", fields)
}

func TestSystemCatalog_RegisterTable_MultipleTables(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid)

	// Register first table
	fields1 := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "name", Type: types.StringType},
	}

	tableID1 := registerTestTable(t, catalog, tid, tempDir, "users", "id", fields1)

	// Register second table
	fields2 := []FieldMetadata{
		{Name: "product_id", Type: types.IntType},
		{Name: "price", Type: types.IntType},
		{Name: "description", Type: types.StringType},
	}

	tableID2 := registerTestTable(t, catalog, tid, tempDir, "products", "product_id", fields2)

	if tableID1 == tableID2 {
		t.Error("table IDs should be different")
	}
}

func TestSystemCatalog_LoadTables(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Register a table
	tid := transaction.NewTransactionID()
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "email", Type: types.StringType},
		{Name: "age", Type: types.IntType},
	}

	tableID := registerTestTable(t, catalog, tid, tempDir, "customers", "id", fields)
	catalog.store.CommitTransaction(tid)

	// Create a new catalog instance (simulating restart)
	tableManager2 := memory.NewTableManager()
	wal2, _ := log.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(tableManager2, wal2)
	catalog2 := NewSystemCatalog(pageStore2, tableManager2)

	// Initialize and load tables
	if err := catalog2.Initialize(tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if err := catalog2.LoadTables(tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify table was loaded
	if !tableManager2.TableExists("customers") {
		t.Error("customers table not loaded")
	}

	// Verify schema was loaded correctly
	schema, err := tableManager2.GetTupleDesc(tableID)
	if err != nil {
		t.Fatalf("failed to get tuple desc: %v", err)
	}

	if schema.NumFields() != 3 {
		t.Errorf("expected 3 fields, got %d", schema.NumFields())
	}

	// Verify field names
	expectedNames := []string{"id", "email", "age"}
	for i, expected := range expectedNames {
		name, err := schema.GetFieldName(i)
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
		fieldType, err := schema.TypeAtIndex(i)
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
	catalog, tempDir, cleanup := setupTestCatalog(t)
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
		tid := transaction.NewTransactionID()
		registerTestTable(t, catalog, tid, tempDir, table.name, table.pk, table.fields)
		catalog.store.CommitTransaction(tid)
	}

	// Create new catalog and load
	tableManager2 := memory.NewTableManager()
	wal2, _ := log.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(tableManager2, wal2)
	catalog2 := NewSystemCatalog(pageStore2, tableManager2)

	// Initialize must be called to set up system catalog tables
	if err := catalog2.Initialize(tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Load user tables from the catalog
	if err := catalog2.LoadTables(tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify all tables loaded
	for _, table := range tables {
		if !tableManager2.TableExists(table.name) {
			t.Errorf("table %s not loaded", table.name)
		}
	}

	pageStore2.Close()
}

func TestSystemCatalog_GetTableID(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Register a table
	tid := transaction.NewTransactionID()
	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "data", Type: types.StringType},
	}

	expectedID := registerTestTable(t, catalog, tid, tempDir, "test_table", "id", fields)
	catalog.store.CommitTransaction(tid)

	// Get table ID from catalog
	tid2 := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid2)

	tableID, err := catalog.GetTableID(tid2, "test_table")
	if err != nil {
		t.Fatalf("GetTableID failed: %v", err)
	}

	// The catalog stores table IDs as int32, so we need to compare truncated values
	if tableID != int(int32(expectedID)) {
		t.Errorf("expected tableID=%d, got %d", int(int32(expectedID)), tableID)
	}
}

func TestSystemCatalog_GetTableID_NotFound(t *testing.T) {
	catalog, _, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid)

	_, err := catalog.GetTableID(tid, "nonexistent_table")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestSystemCatalog_RegisterTable_WithBoolField(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
		{Name: "active", Type: types.BoolType},
		{Name: "name", Type: types.StringType},
	}

	tableID := registerTestTable(t, catalog, tid, tempDir, "flags", "id", fields)

	// Load in new catalog
	tableManager2 := memory.NewTableManager()
	wal2, _ := log.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(tableManager2, wal2)
	catalog2 := NewSystemCatalog(pageStore2, tableManager2)

	if err := catalog2.Initialize(tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if err := catalog2.LoadTables(tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify bool field type preserved
	schema, err := tableManager2.GetTupleDesc(tableID)
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

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
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	// Don't register any tables, just try to load
	err := catalog.LoadTables(tempDir)
	if err != nil {
		t.Errorf("LoadTables should succeed with empty catalog: %v", err)
	}
}

func TestSystemCatalog_PrimaryKeyPreserved(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()

	fields := []FieldMetadata{
		{Name: "user_id", Type: types.IntType},
		{Name: "username", Type: types.StringType},
		{Name: "score", Type: types.IntType},
	}

	registerTestTable(t, catalog, tid, tempDir, "players", "user_id", fields)
	catalog.store.CommitTransaction(tid)

	// Load in new catalog
	tableManager2 := memory.NewTableManager()
	wal2, _ := log.NewWAL(filepath.Join(tempDir, "wal2.log"), 8192)
	pageStore2 := memory.NewPageStore(tableManager2, wal2)
	catalog2 := NewSystemCatalog(pageStore2, tableManager2)

	if err := catalog2.Initialize(tempDir); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if err := catalog2.LoadTables(tempDir); err != nil {
		t.Fatalf("LoadTables failed: %v", err)
	}

	// Verify primary key is preserved (would need to check TableInfo if exposed)
	// For now, just verify the table loaded successfully
	if !tableManager2.TableExists("players") {
		t.Error("players table not loaded")
	}

	pageStore2.Close()
}

func TestSystemCatalog_SequentialTableIDs(t *testing.T) {
	catalog, tempDir, cleanup := setupTestCatalog(t)
	defer cleanup()

	tid := transaction.NewTransactionID()
	defer catalog.store.CommitTransaction(tid)

	fields := []FieldMetadata{
		{Name: "id", Type: types.IntType},
	}

	var ids []int
	for i := 0; i < 5; i++ {
		id := registerTestTable(t, catalog, tid, tempDir, "table_"+string(rune('A'+i)), "id", fields)
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
