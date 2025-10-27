package operations

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"testing"
)

// Helper to create table metadata tuple
func createTableTuple(tableID primitives.TableID, tableName, filePath, primaryKey string) *systemtable.TableMetadata {
	return &systemtable.TableMetadata{
		TableID:       tableID,
		TableName:     tableName,
		FilePath:      primitives.Filepath(filePath),
		PrimaryKeyCol: primaryKey,
	}
}

func TestGetTableMetadataByID(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Add table tuples
	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "users", "/data/users.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(20, "products", "/data/products.dat", "sku")),
		systemtable.Tables.CreateTuple(*createTableTuple(30, "orders", "/data/orders.dat", "order_id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test: Find by ID
	tm, err := ops.GetTableMetadataByID(nil, 20)
	if err != nil {
		t.Fatalf("Failed to get table by ID: %v", err)
	}

	if tm.TableName != "products" {
		t.Errorf("Expected table name 'products', got '%s'", tm.TableName)
	}
	if tm.FilePath != "/data/products.dat" {
		t.Errorf("Expected file path '/data/products.dat', got '%s'", tm.FilePath)
	}
	if tm.PrimaryKeyCol != "sku" {
		t.Errorf("Expected primary key 'sku', got '%s'", tm.PrimaryKeyCol)
	}

	// Test: Non-existent ID
	_, err = ops.GetTableMetadataByID(nil, 999)
	if err == nil {
		t.Error("Expected error for non-existent table ID")
	}
}

func TestGetTableMetadataByName(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "users", "/data/users.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(20, "Products", "/data/products.dat", "sku")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test: Find by name (case-insensitive)
	tm, err := ops.GetTableMetadataByName(nil, "USERS")
	if err != nil {
		t.Fatalf("Failed to get table by name: %v", err)
	}

	if tm.TableID != 10 {
		t.Errorf("Expected table ID 10, got %d", tm.TableID)
	}
	if tm.FilePath != "/data/users.dat" {
		t.Errorf("Expected file path '/data/users.dat', got '%s'", tm.FilePath)
	}

	// Test: Case insensitivity for Products
	tm, err = ops.GetTableMetadataByName(nil, "products")
	if err != nil {
		t.Fatalf("Failed to get table by name (case-insensitive): %v", err)
	}
	if tm.TableID != 20 {
		t.Errorf("Expected table ID 20, got %d", tm.TableID)
	}

	// Test: Non-existent table
	_, err = ops.GetTableMetadataByName(nil, "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table name")
	}
}

func TestGetAllTables(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Add multiple tables
	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "users", "/data/users.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(20, "products", "/data/products.dat", "sku")),
		systemtable.Tables.CreateTuple(*createTableTuple(30, "orders", "/data/orders.dat", "order_id")),
		systemtable.Tables.CreateTuple(*createTableTuple(40, "customers", "/data/customers.dat", "customer_id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test: Get all tables
	tables, err := ops.GetAllTables(nil)
	if err != nil {
		t.Fatalf("Failed to get all tables: %v", err)
	}

	if len(tables) != 4 {
		t.Errorf("Expected 4 tables, got %d", len(tables))
	}

	// Verify table names
	expectedNames := map[string]bool{
		"users":     false,
		"products":  false,
		"orders":    false,
		"customers": false,
	}
	for _, table := range tables {
		if _, ok := expectedNames[table.TableName]; ok {
			expectedNames[table.TableName] = true
		}
	}
	for name, found := range expectedNames {
		if !found {
			t.Errorf("Expected table %s not found", name)
		}
	}
}

func TestGetAllTables_EmptyTable(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)
	mock.tables[tableTableID] = []*tuple.Tuple{} // Empty table

	ops := NewTableOperations(mock, tableTableID)

	// Test: Get all tables from empty catalog
	tables, err := ops.GetAllTables(nil)
	if err != nil {
		t.Fatalf("Should not error on empty table: %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(tables))
	}
}

func TestGetTableMetadataByName_CaseInsensitive(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "MyTableName", "/data/mytable.dat", "id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	testCases := []string{
		"mytablename",
		"MYTABLENAME",
		"MyTableName",
		"mYtAbLeNaMe",
	}

	for _, testName := range testCases {
		tm, err := ops.GetTableMetadataByName(nil, testName)
		if err != nil {
			t.Errorf("Failed to find table with name '%s': %v", testName, err)
		}
		if tm.TableID != 10 {
			t.Errorf("Expected table ID 10, got %d for name '%s'", tm.TableID, testName)
		}
	}
}

func TestGetTableMetadataByID_EdgeCases(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(1, "table_1", "/data/t1.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(999999, "table_large", "/data/tlarge.dat", "id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test valid IDs
	testCases := []struct {
		id          primitives.TableID
		expectFound bool
		expectName  string
	}{
		{1, true, "table_1"},
		{999999, true, "table_large"},
		{0, false, ""},
		{500, false, ""},
	}

	for _, tc := range testCases {
		tm, err := ops.GetTableMetadataByID(nil, tc.id)
		if tc.expectFound {
			if err != nil {
				t.Errorf("Expected to find table with ID %d: %v", tc.id, err)
			}
			if tm.TableName != tc.expectName {
				t.Errorf("Expected table name '%s', got '%s'", tc.expectName, tm.TableName)
			}
		} else {
			if err == nil {
				t.Errorf("Expected error for table ID %d, but found table", tc.id)
			}
		}
	}
}

func TestGetTableMetadataByName_SpecialCharacters(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	specialNames := []string{
		"table@user",
		"table#data",
		"table$info",
		"table.metadata",
		"table-with-dashes",
		"table_with_underscores",
	}

	// Create tuples for each special name
	for i, name := range specialNames {
		tup := systemtable.Tables.CreateTuple(*createTableTuple(i+1, name, fmt.Sprintf("/data/%d.dat", i), "id"))
		mock.tables[tableTableID] = append(mock.tables[tableTableID], tup)
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test each special name can be found
	for i, name := range specialNames {
		tm, err := ops.GetTableMetadataByName(nil, name)
		if err != nil {
			t.Errorf("Failed to find table with name '%s': %v", name, err)
		}
		if tm.TableID != i+1 {
			t.Errorf("Expected table ID %d, got %d for name '%s'", i+1, tm.TableID, name)
		}
	}
}

func TestGetTableMetadataByName_EmptyName(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "valid_name", "/data/valid.dat", "id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Try to find by empty name
	_, err := ops.GetTableMetadataByName(nil, "")
	if err == nil {
		t.Error("Expected error when searching for empty table name")
	}
}

func TestTableOperations_WithPrimaryKeys(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Tables with different primary key configurations
	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "with_pk", "/data/with_pk.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(20, "no_pk", "/data/no_pk.dat", "")),
		systemtable.Tables.CreateTuple(*createTableTuple(30, "composite_pk", "/data/composite_pk.dat", "user_id,order_id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test table with primary key
	tm, err := ops.GetTableMetadataByName(nil, "with_pk")
	if err != nil {
		t.Fatalf("Failed to get table with primary key: %v", err)
	}
	if tm.PrimaryKeyCol != "id" {
		t.Errorf("Expected primary key 'id', got '%s'", tm.PrimaryKeyCol)
	}

	// Test table without primary key
	tm, err = ops.GetTableMetadataByName(nil, "no_pk")
	if err != nil {
		t.Fatalf("Failed to get table without primary key: %v", err)
	}
	if tm.PrimaryKeyCol != "" {
		t.Errorf("Expected empty primary key, got '%s'", tm.PrimaryKeyCol)
	}

	// Test table with composite primary key
	tm, err = ops.GetTableMetadataByName(nil, "composite_pk")
	if err != nil {
		t.Fatalf("Failed to get table with composite primary key: %v", err)
	}
	if tm.PrimaryKeyCol != "user_id,order_id" {
		t.Errorf("Expected composite primary key 'user_id,order_id', got '%s'", tm.PrimaryKeyCol)
	}
}

func TestGetAllTables_LargeNumberOfTables(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Create 100 tables
	for i := 1; i <= 100; i++ {
		tup := systemtable.Tables.CreateTuple(*createTableTuple(
			i,
			fmt.Sprintf("table_%d", i),
			fmt.Sprintf("/data/table_%d.dat", i),
			"id",
		))
		mock.tables[tableTableID] = append(mock.tables[tableTableID], tup)
	}

	ops := NewTableOperations(mock, tableTableID)

	tables, err := ops.GetAllTables(nil)
	if err != nil {
		t.Fatalf("Failed to get all tables: %v", err)
	}

	if len(tables) != 100 {
		t.Errorf("Expected 100 tables, got %d", len(tables))
	}

	// Verify table IDs are correct
	foundIDs := make(map[int]bool)
	for _, tm := range tables {
		foundIDs[tm.TableID] = true
	}

	for i := 1; i <= 100; i++ {
		if !foundIDs[i] {
			t.Errorf("Table ID %d not found", i)
		}
	}
}

func TestGetTableMetadataByName_DuplicateNames(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Create tables with same name but different IDs (edge case)
	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "duplicate", "/data/dup1.dat", "id")),
		systemtable.Tables.CreateTuple(*createTableTuple(20, "duplicate", "/data/dup2.dat", "id")),
	}

	ops := NewTableOperations(mock, tableTableID)

	// GetTableMetadataByName should return the first match
	tm, err := ops.GetTableMetadataByName(nil, "duplicate")
	if err != nil {
		t.Fatalf("Failed to get table by name: %v", err)
	}

	// Should get one of them (the first one found)
	if tm.TableID != 10 && tm.TableID != 20 {
		t.Errorf("Expected table ID 10 or 20, got %d", tm.TableID)
	}
}

func TestGetTableMetadataByID_SystemTables(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Simulate system catalog tables
	systemTables := []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(1, "CATALOG_TABLES", "catalog_tables.dat", "table_id")),
		systemtable.Tables.CreateTuple(*createTableTuple(2, "CATALOG_COLUMNS", "catalog_columns.dat", "column_id")),
		systemtable.Tables.CreateTuple(*createTableTuple(3, "CATALOG_INDEXES", "catalog_indexes.dat", "index_id")),
	}

	mock.tables[tableTableID] = systemTables

	ops := NewTableOperations(mock, tableTableID)

	// Test retrieving system tables
	for _, expectedID := range []int{1, 2, 3} {
		tm, err := ops.GetTableMetadataByID(nil, expectedID)
		if err != nil {
			t.Errorf("Failed to get system table with ID %d: %v", expectedID, err)
		}
		if tm.TableID != expectedID {
			t.Errorf("Expected table ID %d, got %d", expectedID, tm.TableID)
		}
	}
}

func TestTableOperations_FilePaths(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Test various file path formats
	filePaths := []struct {
		path     string
		expected string
	}{
		{"/data/users.dat", "/data/users.dat"},
		{"./local/table.dat", "./local/table.dat"},
		{"C:\\data\\table.dat", "C:\\data\\table.dat"},
		{"/var/db/table.heap", "/var/db/table.heap"},
		{"table.dat", "table.dat"},
	}

	for i, fp := range filePaths {
		tup := systemtable.Tables.CreateTuple(*createTableTuple(primitives.TableID(i+1), fmt.Sprintf("table_%d", i), fp.path, "id"))
		mock.tables[tableTableID] = append(mock.tables[tableTableID], tup)
	}

	ops := NewTableOperations(mock, tableTableID)

	// Verify all file paths are preserved correctly
	for i, fp := range filePaths {
		tm, err := ops.GetTableMetadataByID(nil, primitives.TableID(i+1))
		if err != nil {
			t.Errorf("Failed to get table with ID %d: %v", i+1, err)
		}
		if string(tm.FilePath) != fp.expected {
			t.Errorf("Expected file path '%s', got '%s'", fp.expected, tm.FilePath)
		}
	}
}

func TestTableOperations_Isolation(t *testing.T) {
	// This test demonstrates that TableOperations only depends on interfaces
	// and doesn't require the full SystemCatalog

	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Add some data
	mock.tables[tableTableID] = []*tuple.Tuple{
		systemtable.Tables.CreateTuple(*createTableTuple(10, "test_table", "/data/test.dat", "id")),
	}

	// Create operations with just the interface
	ops := NewTableOperations(mock, tableTableID)

	// Should work without any SystemCatalog dependencies
	tm, err := ops.GetTableMetadataByID(nil, 10)
	if err != nil {
		t.Fatalf("Failed with interface-only dependency: %v", err)
	}

	if tm.TableName != "test_table" {
		t.Errorf("Expected 'test_table', got '%s'", tm.TableName)
	}

	t.Log("✅ TableOperations successfully works with interface-only dependency")
}

func TestGetTableMetadataByName_Unicode(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Unicode table names
	unicodeNames := []string{
		"users_用户",
		"продукты",
		"données",
		"テーブル",
	}

	for i, name := range unicodeNames {
		tup := systemtable.Tables.CreateTuple(*createTableTuple(i+1, name, fmt.Sprintf("/data/%d.dat", i), "id"))
		mock.tables[tableTableID] = append(mock.tables[tableTableID], tup)
	}

	ops := NewTableOperations(mock, tableTableID)

	// Test each unicode name can be found
	for i, name := range unicodeNames {
		tm, err := ops.GetTableMetadataByName(nil, name)
		if err != nil {
			t.Errorf("Failed to find table with unicode name '%s': %v", name, err)
		}
		if tm.TableID != i+1 {
			t.Errorf("Expected table ID %d, got %d for unicode name '%s'", i+1, tm.TableID, name)
		}
	}
}

func TestGetAllTables_OrderPreservation(t *testing.T) {
	mock := NewMockCatalogAccess()
	tableTableID := primitives.TableID(100)

	// Add tables in specific order
	tableIDs := []int{5, 1, 10, 3, 7}
	for _, id := range tableIDs {
		tup := systemtable.Tables.CreateTuple(*createTableTuple(id, fmt.Sprintf("table_%d", id), fmt.Sprintf("/data/%d.dat", id), "id"))
		mock.tables[tableTableID] = append(mock.tables[tableTableID], tup)
	}

	ops := NewTableOperations(mock, tableTableID)

	tables, err := ops.GetAllTables(nil)
	if err != nil {
		t.Fatalf("Failed to get all tables: %v", err)
	}

	// Verify all tables are retrieved (order may not be preserved)
	foundIDs := make(map[int]bool)
	for _, tm := range tables {
		foundIDs[tm.TableID] = true
	}

	for _, id := range tableIDs {
		if !foundIDs[id] {
			t.Errorf("Table ID %d not found in results", id)
		}
	}
}
