package memory

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockDbFile implements storage.DbFile for testing
type mockDbFile struct {
	id         int
	tupleDesc  *tuple.TupleDescription
	closed     bool
	closeMutex sync.Mutex
}

func newMockDbFile(id int, fieldTypes []types.Type, fieldNames []string) *mockDbFile {
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

func (m *mockDbFile) ReadPage(pid primitives.PageID) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) WritePage(p page.Page) error {
	return nil
}

func (m *mockDbFile) AddTuple(tid *primitives.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) DeleteTuple(tid *primitives.TransactionID, t *tuple.Tuple) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) Iterator(tid *primitives.TransactionID) iterator.DbFileIterator {
	return nil
}

func (m *mockDbFile) GetID() int {
	return m.id
}

func (m *mockDbFile) GetTupleDesc() *tuple.TupleDescription {
	return m.tupleDesc
}

func (m *mockDbFile) Close() error {
	m.closeMutex.Lock()
	defer m.closeMutex.Unlock()
	m.closed = true
	return nil
}

func (m *mockDbFile) IsClosed() bool {
	m.closeMutex.Lock()
	defer m.closeMutex.Unlock()
	return m.closed
}

// Helper function to create a schema for testing
func createTestSchema(tableID int, tableName string, td *tuple.TupleDescription, primaryKey string) *schema.Schema {
	columns := make([]schema.ColumnMetadata, len(td.Types))
	for i, ft := range td.Types {
		fieldName := ""
		if i < len(td.FieldNames) {
			fieldName = td.FieldNames[i]
		}
		isPrimary := fieldName == primaryKey
		col, err := schema.NewColumnMetadata(fieldName, ft, i, tableID, isPrimary, false)
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

func TestNewTableManager(t *testing.T) {
	tm := NewTableManager()

	if tm == nil {
		t.Fatal("NewTableManager returned nil")
	}

	if tm.nameToTable == nil {
		t.Error("nameToTable map should be initialized")
	}

	if tm.idToTable == nil {
		t.Error("idToTable map should be initialized")
	}

	if len(tm.nameToTable) != 0 {
		t.Errorf("nameToTable should be empty, got %d entries", len(tm.nameToTable))
	}

	if len(tm.idToTable) != 0 {
		t.Errorf("idToTable should be empty, got %d entries", len(tm.idToTable))
	}
}

func TestTableManager_AddTable_ValidCases(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() (*TableManager, page.DbFile)
		tableName   string
		primaryKey  string
		shouldError bool
	}{
		{
			name: "Add first table",
			setupFunc: func() (*TableManager, page.DbFile) {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})
				return tm, dbFile
			},
			tableName:   "users",
			primaryKey:  "id",
			shouldError: false,
		},
		{
			name: "Add table with empty primary key",
			setupFunc: func() (*TableManager, page.DbFile) {
				tm := NewTableManager()
				dbFile := newMockDbFile(2, []types.Type{types.StringType}, []string{"data"})
				return tm, dbFile
			},
			tableName:   "logs",
			primaryKey:  "",
			shouldError: false,
		},
		{
			name: "Add multiple different tables",
			setupFunc: func() (*TableManager, page.DbFile) {
				tm := NewTableManager()
				dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s1 := createTestSchema(1, "table1", dbFile1.GetTupleDesc(), "id")
				tm.AddTable(dbFile1, s1)
				dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
				return tm, dbFile2
			},
			tableName:   "table2",
			primaryKey:  "name",
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, dbFile := tt.setupFunc()

			s := createTestSchema(dbFile.GetID(), tt.tableName, dbFile.GetTupleDesc(), tt.primaryKey)
			err := tm.AddTable(dbFile, s)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify table was added correctly
			tableInfo, exists := tm.nameToTable[tt.tableName]
			if !exists {
				t.Errorf("Table %s not found in nameToTable map", tt.tableName)
				return
			}

			if tableInfo.Schema.TableName != tt.tableName {
				t.Errorf("Expected table name %s, got %s", tt.tableName, tableInfo.Schema.TableName)
			}

			if tableInfo.Schema.PrimaryKey != tt.primaryKey {
				t.Errorf("Expected primary key %s, got %s", tt.primaryKey, tableInfo.Schema.PrimaryKey)
			}

			if tableInfo.File != dbFile {
				t.Errorf("Expected DbFile %v, got %v", dbFile, tableInfo.File)
			}

			// Verify table is also in idToTable map
			tableInfoByID, exists := tm.idToTable[dbFile.GetID()]
			if !exists {
				t.Errorf("Table with ID %d not found in idToTable map", dbFile.GetID())
				return
			}

			if tableInfoByID != tableInfo {
				t.Errorf("Table info mismatch between maps")
			}
		})
	}
}

func TestTableManager_AddTable_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() (*TableManager, page.DbFile, string, string)
		expectedErr string
	}{
		{
			name: "Nil DbFile",
			setupFunc: func() (*TableManager, page.DbFile, string, string) {
				tm := NewTableManager()
				return tm, nil, "test_table", "id"
			},
			expectedErr: "file cannot be nil",
		},
		{
			name: "Nil schema",
			setupFunc: func() (*TableManager, page.DbFile, string, string) {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				return tm, dbFile, "test_table", "id"
			},
			expectedErr: "schema cannot be nil",
		},
		{
			name: "Empty table name",
			setupFunc: func() (*TableManager, page.DbFile, string, string) {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				return tm, dbFile, "", "id"
			},
			expectedErr: "table name cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, dbFile, tableName, primaryKey := tt.setupFunc()

			var err error
			if dbFile == nil {
				err = tm.AddTable(nil, nil)
			} else if tt.expectedErr == "schema cannot be nil" {
				err = tm.AddTable(dbFile, nil)
			} else {
				s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), primaryKey)
				err = tm.AddTable(dbFile, s)
			}

			if err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if err.Error() != tt.expectedErr {
				t.Errorf("Expected error %q, got %q", tt.expectedErr, err.Error())
			}
		})
	}
}

func TestTableManager_AddTable_Replacement(t *testing.T) {
	t.Run("Replace table by name", func(t *testing.T) {
		tm := NewTableManager()

		// Add first table
		dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
		s1 := createTestSchema(1, "users", dbFile1.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile1, s1)
		if err != nil {
			t.Fatalf("Failed to add first table: %v", err)
		}

		// Replace with new table (same name, different ID)
		dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
		s2 := createTestSchema(2, "users", dbFile2.GetTupleDesc(), "name")
		err = tm.AddTable(dbFile2, s2)
		if err != nil {
			t.Fatalf("Failed to replace table: %v", err)
		}

		// Verify replacement
		tableInfo, exists := tm.nameToTable["users"]
		if !exists {
			t.Fatal("Table 'users' not found after replacement")
		}

		if tableInfo.File != dbFile2 {
			t.Errorf("Expected DbFile %v, got %v", dbFile2, tableInfo.File)
		}

		// Verify old ID is removed from idToTable
		if _, exists := tm.idToTable[1]; exists {
			t.Error("Old table ID should be removed from idToTable")
		}

		// Verify new ID is in idToTable
		if _, exists := tm.idToTable[2]; !exists {
			t.Error("New table ID should be in idToTable")
		}
	})

	t.Run("Replace table by ID", func(t *testing.T) {
		tm := NewTableManager()

		// Add first table
		dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
		s1 := createTestSchema(1, "table1", dbFile1.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile1, s1)
		if err != nil {
			t.Fatalf("Failed to add first table: %v", err)
		}

		// Replace with new table (same ID, different name)
		dbFile2 := newMockDbFile(1, []types.Type{types.StringType}, []string{"name"})
		s2 := createTestSchema(1, "table2", dbFile2.GetTupleDesc(), "name")
		err = tm.AddTable(dbFile2, s2)
		if err != nil {
			t.Fatalf("Failed to replace table: %v", err)
		}

		// Verify replacement
		tableInfo, exists := tm.idToTable[1]
		if !exists {
			t.Fatal("Table with ID 1 not found after replacement")
		}

		if tableInfo.Schema.TableName != "table2" {
			t.Errorf("Expected table name 'table2', got %s", tableInfo.Schema.TableName)
		}

		// Verify old name is removed from nameToTable
		if _, exists := tm.nameToTable["table1"]; exists {
			t.Error("Old table name should be removed from nameToTable")
		}

		// Verify new name is in nameToTable
		if _, exists := tm.nameToTable["table2"]; !exists {
			t.Error("New table name should be in nameToTable")
		}
	})
}

func TestTableManager_GetTableID(t *testing.T) {
	tm := NewTableManager()
	dbFile := newMockDbFile(42, []types.Type{types.IntType}, []string{"id"})
	s := createTestSchema(42, "test_table", dbFile.GetTupleDesc(), "id")
	tm.AddTable(dbFile, s)

	tests := []struct {
		name          string
		tableName     string
		expectedID    int
		expectedError bool
	}{
		{
			name:          "Existing table",
			tableName:     "test_table",
			expectedID:    42,
			expectedError: false,
		},
		{
			name:          "Non-existing table",
			tableName:     "non_existing",
			expectedError: true,
		},
		{
			name:          "Empty table name",
			tableName:     "",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := tm.GetTableID(tt.tableName)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if id != tt.expectedID {
				t.Errorf("Expected ID %d, got %d", tt.expectedID, id)
			}
		})
	}
}

func TestTableManager_GetTableName(t *testing.T) {
	tm := NewTableManager()
	dbFile := newMockDbFile(42, []types.Type{types.IntType}, []string{"id"})
	s := createTestSchema(dbFile.GetID(), "test_table", dbFile.GetTupleDesc(), "id")
	tm.AddTable(dbFile, s)

	tests := []struct {
		name          string
		tableID       int
		expectedName  string
		expectedError bool
	}{
		{
			name:          "Existing table ID",
			tableID:       42,
			expectedName:  "test_table",
			expectedError: false,
		},
		{
			name:          "Non-existing table ID",
			tableID:       999,
			expectedError: true,
		},
		{
			name:          "Zero table ID",
			tableID:       0,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableInfo, err := tm.GetTableInfo(tt.tableID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tableInfo.Schema.TableName != tt.expectedName {
				t.Errorf("Expected name %s, got %s", tt.expectedName, tableInfo.Schema.TableName)
			}
		})
	}
}

func TestTableManager_GetTupleDesc(t *testing.T) {
	tm := NewTableManager()
	fieldTypes := []types.Type{types.IntType, types.StringType}
	fieldNames := []string{"id", "name"}
	dbFile := newMockDbFile(42, fieldTypes, fieldNames)
	s := createTestSchema(dbFile.GetID(), "test_table", dbFile.GetTupleDesc(), "id")
	tm.AddTable(dbFile, s)

	tests := []struct {
		name          string
		tableID       int
		expectedError bool
	}{
		{
			name:          "Existing table ID",
			tableID:       42,
			expectedError: false,
		},
		{
			name:          "Non-existing table ID",
			tableID:       999,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tupleDesc, err := tm.GetTupleDesc(tt.tableID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tupleDesc == nil {
				t.Fatal("Expected non-nil TupleDescription")
			}

			if tupleDesc.NumFields() != 2 {
				t.Errorf("Expected 2 fields, got %d", tupleDesc.NumFields())
			}
		})
	}
}

func TestTableManager_GetDbFile(t *testing.T) {
	tm := NewTableManager()
	dbFile := newMockDbFile(42, []types.Type{types.IntType}, []string{"id"})
	s := createTestSchema(dbFile.GetID(), "test_table", dbFile.GetTupleDesc(), "id")
	tm.AddTable(dbFile, s)

	tests := []struct {
		name          string
		tableID       int
		expectedError bool
	}{
		{
			name:          "Existing table ID",
			tableID:       42,
			expectedError: false,
		},
		{
			name:          "Non-existing table ID",
			tableID:       999,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retrievedFile, err := tm.GetDbFile(tt.tableID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if retrievedFile != dbFile {
				t.Errorf("Expected DbFile %v, got %v", dbFile, retrievedFile)
			}
		})
	}
}

func TestTableManager_Clear(t *testing.T) {
	tm := NewTableManager()

	// Add multiple tables
	dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
	dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})

	s1 := createTestSchema(dbFile1.GetID(), "table1", dbFile1.GetTupleDesc(), "id")
	tm.AddTable(dbFile1, s1)
	s2 := createTestSchema(dbFile2.GetID(), "table2", dbFile2.GetTupleDesc(), "name")
	tm.AddTable(dbFile2, s2)

	// Verify tables are added
	if len(tm.nameToTable) != 2 {
		t.Errorf("Expected 2 tables in nameToTable, got %d", len(tm.nameToTable))
	}
	if len(tm.idToTable) != 2 {
		t.Errorf("Expected 2 tables in idToTable, got %d", len(tm.idToTable))
	}

	// Clear the manager
	tm.Clear()

	// Verify everything is cleared
	if len(tm.nameToTable) != 0 {
		t.Errorf("Expected 0 tables in nameToTable after clear, got %d", len(tm.nameToTable))
	}
	if len(tm.idToTable) != 0 {
		t.Errorf("Expected 0 tables in idToTable after clear, got %d", len(tm.idToTable))
	}

	// Verify files were closed
	if !dbFile1.IsClosed() {
		t.Error("Expected dbFile1 to be closed after Clear()")
	}
	if !dbFile2.IsClosed() {
		t.Error("Expected dbFile2 to be closed after Clear()")
	}
}

func TestTableManager_ValidateIntegrity(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func() *TableManager
		expectedError bool
		errorContains string
	}{
		{
			name: "Empty manager - valid",
			setupFunc: func() *TableManager {
				return NewTableManager()
			},
			expectedError: false,
		},
		{
			name: "Single table - valid",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "table1", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			expectedError: false,
		},
		{
			name: "Multiple tables - valid",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
				s1 := createTestSchema(dbFile1.GetID(), "table1", dbFile1.GetTupleDesc(), "id")
				tm.AddTable(dbFile1, s1)
				s2 := createTestSchema(dbFile2.GetID(), "table2", dbFile2.GetTupleDesc(), "name")
				tm.AddTable(dbFile2, s2)
				return tm
			},
			expectedError: false,
		},
		{
			name: "Map size mismatch - invalid",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "table1", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				// Manually corrupt the maps to create size mismatch
				delete(tm.idToTable, 1)
				return tm
			},
			expectedError: true,
			errorContains: "map size mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := tt.setupFunc()

			err := tm.ValidateIntegrity()

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !containsString(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestTableManager_GetAllTableNames(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func() *TableManager
		expectedNames []string
	}{
		{
			name: "Empty manager",
			setupFunc: func() *TableManager {
				return NewTableManager()
			},
			expectedNames: []string{},
		},
		{
			name: "Single table",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "users", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			expectedNames: []string{"users"},
		},
		{
			name: "Multiple tables",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
				dbFile3 := newMockDbFile(3, []types.Type{types.IntType}, []string{"age"})
				s1 := createTestSchema(dbFile1.GetID(), "users", dbFile1.GetTupleDesc(), "id")
				tm.AddTable(dbFile1, s1)
				s2 := createTestSchema(dbFile2.GetID(), "products", dbFile2.GetTupleDesc(), "name")
				tm.AddTable(dbFile2, s2)
				s3 := createTestSchema(dbFile3.GetID(), "orders", dbFile3.GetTupleDesc(), "age")
				tm.AddTable(dbFile3, s3)
				return tm
			},
			expectedNames: []string{"users", "products", "orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := tt.setupFunc()

			names := tm.GetAllTableNames()

			if len(names) != len(tt.expectedNames) {
				t.Errorf("Expected %d names, got %d", len(tt.expectedNames), len(names))
				return
			}

			// Convert to map for easier comparison (order doesn't matter)
			nameMap := make(map[string]bool)
			for _, name := range names {
				nameMap[name] = true
			}

			for _, expectedName := range tt.expectedNames {
				if !nameMap[expectedName] {
					t.Errorf("Expected name %s not found in result", expectedName)
				}
			}
		})
	}
}

func TestTableManager_ConcurrentOperations(t *testing.T) {
	tm := NewTableManager()
	numGoroutines := 10
	numOperationsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numOperationsPerGoroutine; j++ {
				tableName := fmt.Sprintf("table_%d_%d", goroutineID, j)
				tableID := goroutineID*numOperationsPerGoroutine + j

				// Add table
				dbFile := newMockDbFile(tableID, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
				err := tm.AddTable(dbFile, s)
				if err != nil {
					t.Errorf("Failed to add table %s: %v", tableName, err)
					continue
				}

				// Immediately try to retrieve it
				retrievedID, err := tm.GetTableID(tableName)
				if err != nil {
					t.Errorf("Failed to get table ID for %s: %v", tableName, err)
					continue
				}

				if retrievedID != tableID {
					t.Errorf("Expected table ID %d, got %d", tableID, retrievedID)
				}

				// Get all table names (this exercises the read lock)
				tm.GetAllTableNames()
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	allNames := tm.GetAllTableNames()
	expectedCount := numGoroutines * numOperationsPerGoroutine

	if len(allNames) != expectedCount {
		t.Errorf("Expected %d tables, got %d", expectedCount, len(allNames))
	}

	// Verify integrity
	err := tm.ValidateIntegrity()
	if err != nil {
		t.Errorf("Integrity validation failed: %v", err)
	}
}

func TestTableManager_TableExists(t *testing.T) {
	tm := NewTableManager()

	// Test with empty manager
	if tm.TableExists("non_existing") {
		t.Error("TableExists should return false for non-existing table in empty manager")
	}

	// Add a table
	dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
	s := createTestSchema(dbFile.GetID(), "users", dbFile.GetTupleDesc(), "id")
	err := tm.AddTable(dbFile, s)
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	tests := []struct {
		name      string
		tableName string
		expected  bool
	}{
		{
			name:      "Existing table",
			tableName: "users",
			expected:  true,
		},
		{
			name:      "Non-existing table",
			tableName: "products",
			expected:  false,
		},
		{
			name:      "Empty table name",
			tableName: "",
			expected:  false,
		},
		{
			name:      "Case sensitive - different case",
			tableName: "USERS",
			expected:  false,
		},
		{
			name:      "Table name with spaces",
			tableName: " users ",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tm.TableExists(tt.tableName)
			if result != tt.expected {
				t.Errorf("TableExists(%q) = %v, expected %v", tt.tableName, result, tt.expected)
			}
		})
	}

	// Test after removing table
	err = tm.RemoveTable("users")
	if err != nil {
		t.Fatalf("Failed to remove table: %v", err)
	}

	if tm.TableExists("users") {
		t.Error("TableExists should return false after table is removed")
	}
}

func TestTableManager_RenameTable(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func() *TableManager
		oldName       string
		newName       string
		expectedError bool
		errorContains string
	}{
		{
			name: "Successful rename",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "old_table", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			oldName:       "old_table",
			newName:       "new_table",
			expectedError: false,
		},
		{
			name: "Empty old name",
			setupFunc: func() *TableManager {
				return NewTableManager()
			},
			oldName:       "",
			newName:       "new_table",
			expectedError: true,
			errorContains: "table names cannot be empty",
		},
		{
			name: "Empty new name",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "old_table", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			oldName:       "old_table",
			newName:       "",
			expectedError: true,
			errorContains: "table names cannot be empty",
		},
		{
			name: "New name with leading whitespace",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "old_table", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			oldName:       "old_table",
			newName:       " new_table",
			expectedError: true,
			errorContains: "new table name cannot have leading or trailing whitespace",
		},
		{
			name: "New name with trailing whitespace",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), "old_table", dbFile.GetTupleDesc(), "id")
				tm.AddTable(dbFile, s)
				return tm
			},
			oldName:       "old_table",
			newName:       "new_table ",
			expectedError: true,
			errorContains: "new table name cannot have leading or trailing whitespace",
		},
		{
			name: "Old table does not exist",
			setupFunc: func() *TableManager {
				return NewTableManager()
			},
			oldName:       "non_existing",
			newName:       "new_table",
			expectedError: true,
			errorContains: "table 'non_existing' not found",
		},
		{
			name: "New name already exists",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
				s1 := createTestSchema(dbFile1.GetID(), "table1", dbFile1.GetTupleDesc(), "id")
				tm.AddTable(dbFile1, s1)
				s2 := createTestSchema(dbFile2.GetID(), "table2", dbFile2.GetTupleDesc(), "name")
				tm.AddTable(dbFile2, s2)
				return tm
			},
			oldName:       "table1",
			newName:       "table2",
			expectedError: true,
			errorContains: "table 'table2' already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := tt.setupFunc()

			err := tm.RenameTable(tt.oldName, tt.newName)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !containsString(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify the rename was successful
			if !tm.TableExists(tt.newName) {
				t.Errorf("New table name %s should exist after rename", tt.newName)
			}

			if tm.TableExists(tt.oldName) {
				t.Errorf("Old table name %s should not exist after rename", tt.oldName)
			}

			// Verify table info was updated correctly
			tableInfo, exists := tm.nameToTable[tt.newName]
			if !exists {
				t.Errorf("Table info not found for new name %s", tt.newName)
				return
			}

			if tableInfo.Schema.TableName != tt.newName {
				t.Errorf("Table info name should be updated to %s, got %s", tt.newName, tableInfo.Schema.TableName)
			}

			// Verify ID mapping is still correct
			tableID := tableInfo.GetID()
			tableInfoByID, exists := tm.idToTable[tableID]
			if !exists {
				t.Errorf("Table info not found by ID %d after rename", tableID)
				return
			}

			if tableInfoByID != tableInfo {
				t.Error("Table info reference mismatch between name and ID maps after rename")
			}
		})
	}
}

func TestTableManager_RenameTable_IntegrityPreserved(t *testing.T) {
	tm := NewTableManager()

	// Add multiple tables
	dbFile1 := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
	dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
	s1 := createTestSchema(dbFile1.GetID(), "users", dbFile1.GetTupleDesc(), "id")
	tm.AddTable(dbFile1, s1)
	s2 := createTestSchema(dbFile2.GetID(), "products", dbFile2.GetTupleDesc(), "name")
	tm.AddTable(dbFile2, s2)

	// Rename one table
	err := tm.RenameTable("users", "customers")
	if err != nil {
		t.Fatalf("Failed to rename table: %v", err)
	}

	// Verify integrity is maintained
	err = tm.ValidateIntegrity()
	if err != nil {
		t.Errorf("Integrity validation failed after rename: %v", err)
	}

	// Verify all expected tables exist
	expectedTables := []string{"customers", "products"}
	allNames := tm.GetAllTableNames()

	if len(allNames) != len(expectedTables) {
		t.Errorf("Expected %d tables, got %d", len(expectedTables), len(allNames))
	}

	nameMap := make(map[string]bool)
	for _, name := range allNames {
		nameMap[name] = true
	}

	for _, expected := range expectedTables {
		if !nameMap[expected] {
			t.Errorf("Expected table %s not found after rename", expected)
		}
	}
}

func TestTableManager_TableExists_ConcurrentAccess(t *testing.T) {
	tm := NewTableManager()

	// Add a table
	dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
	s := createTestSchema(dbFile.GetID(), "concurrent_test", dbFile.GetTupleDesc(), "id")
	err := tm.AddTable(dbFile, s)
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	numGoroutines := 50
	numChecksPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent TableExists checks
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < numChecksPerGoroutine; j++ {
				// Check existing table
				if !tm.TableExists("concurrent_test") {
					t.Errorf("TableExists should return true for existing table")
				}

				// Check non-existing table
				if tm.TableExists("non_existing") {
					t.Errorf("TableExists should return false for non-existing table")
				}
			}
		}()
	}

	wg.Wait()
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) && containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestTableManager_HighVolumeConcurrentOperations(t *testing.T) {
	tm := NewTableManager()
	numGoroutines := 100
	numOperationsPerGoroutine := 1000

	var wg sync.WaitGroup
	var addErrors, getErrors, removeErrors int32

	wg.Add(numGoroutines * 3)

	// Concurrent adds
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				tableName := fmt.Sprintf("table_%d_%d", goroutineID, j)
				tableID := goroutineID*numOperationsPerGoroutine + j + 1
				dbFile := newMockDbFile(tableID, []types.Type{types.IntType}, []string{"id"})

				s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
				if err := tm.AddTable(dbFile, s); err != nil {
					atomic.AddInt32(&addErrors, 1)
				}
			}
		}(i)
	}

	// Concurrent gets
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				tableName := fmt.Sprintf("table_%d_%d", goroutineID, j)
				if _, err := tm.GetTableID(tableName); err != nil {
					atomic.AddInt32(&getErrors, 1)
				}
				tm.TableExists(tableName)
				tm.GetAllTableNames()
			}
		}(i)
	}

	// Concurrent removes (for some tables)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine/10; j++ {
				tableName := fmt.Sprintf("table_%d_%d", goroutineID, j*10)
				if err := tm.RemoveTable(tableName); err != nil {
					atomic.AddInt32(&removeErrors, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify integrity after all operations
	if err := tm.ValidateIntegrity(); err != nil {
		t.Errorf("Integrity validation failed after high-volume operations: %v", err)
	}

	t.Logf("Operations completed - Add errors: %d, Get errors: %d, Remove errors: %d",
		addErrors, getErrors, removeErrors)
}

func TestTableManager_ConcurrentRenameOperations(t *testing.T) {
	tm := NewTableManager()
	numTables := 100

	// Add initial tables
	for i := 0; i < numTables; i++ {
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		tableName := fmt.Sprintf("table_%d", i)
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile, s)
		if err != nil {
			t.Fatalf("Failed to add initial table %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var renameErrors int32

	// Concurrent renames
	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func(tableNum int) {
			defer wg.Done()
			oldName := fmt.Sprintf("table_%d", tableNum)
			newName := fmt.Sprintf("renamed_table_%d", tableNum)

			if err := tm.RenameTable(oldName, newName); err != nil {
				atomic.AddInt32(&renameErrors, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify all tables were renamed correctly
	allNames := tm.GetAllTableNames()
	renamedCount := 0
	for _, name := range allNames {
		if len(name) >= 14 && name[:14] == "renamed_table_" {
			renamedCount++
		}
	}

	expectedRenamed := numTables - int(renameErrors)
	if renamedCount != expectedRenamed {
		t.Errorf("Expected %d renamed tables, got %d (rename errors: %d)",
			expectedRenamed, renamedCount, renameErrors)
	}

	if err := tm.ValidateIntegrity(); err != nil {
		t.Errorf("Integrity validation failed after concurrent renames: %v", err)
	}
}

func TestTableManager_MemoryPressureScenario(t *testing.T) {
	tm := NewTableManager()
	numTables := 10000

	// Add many tables to simulate memory pressure
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("memory_test_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType, types.StringType, types.FloatType},
			[]string{"id", "name", "value"})

		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile, s)
		if err != nil {
			t.Fatalf("Failed to add table %d: %v", i, err)
		}

		// Periodically verify integrity during the process
		if i%1000 == 0 {
			if err := tm.ValidateIntegrity(); err != nil {
				t.Errorf("Integrity validation failed at table %d: %v", i, err)
			}
		}
	}

	// Verify all tables exist and are accessible
	allNames := tm.GetAllTableNames()
	if len(allNames) != numTables {
		t.Errorf("Expected %d tables, got %d", numTables, len(allNames))
	}

	// Test random access patterns
	for i := 0; i < 1000; i++ {
		tableNum := i * (numTables / 1000)
		tableName := fmt.Sprintf("memory_test_table_%d", tableNum)

		if !tm.TableExists(tableName) {
			t.Errorf("Table %s should exist but doesn't", tableName)
		}

		tableID, err := tm.GetTableID(tableName)
		if err != nil {
			t.Errorf("Failed to get table ID for %s: %v", tableName, err)
		}

		if tableID != tableNum+1 {
			t.Errorf("Expected table ID %d, got %d", tableNum+1, tableID)
		}
	}

	// Clear all and verify memory is properly cleaned
	tm.Clear()

	if len(tm.GetAllTableNames()) != 0 {
		t.Error("Expected empty table manager after Clear()")
	}
}

type errorDbFile struct {
	*mockDbFile
	closeError bool
}

func (e *errorDbFile) Close() error {
	if e.closeError {
		return fmt.Errorf("simulated close error")
	}
	return e.mockDbFile.Close()
}

func TestTableManager_ErrorHandlingDuringFileOperations(t *testing.T) {
	tm := NewTableManager()

	// Test with file that errors on close
	errorFile := &errorDbFile{
		mockDbFile: newMockDbFile(1, []types.Type{types.IntType}, []string{"id"}),
		closeError: true,
	}

	s := createTestSchema(errorFile.GetID(), "error_table", errorFile.GetTupleDesc(), "id")
	err := tm.AddTable(errorFile, s)
	if err != nil {
		t.Fatalf("Failed to add table with error file: %v", err)
	}

	// Remove table - should handle close error gracefully
	err = tm.RemoveTable("error_table")
	if err != nil {
		t.Errorf("RemoveTable should succeed even with close error: %v", err)
	}

	// Verify table was removed despite close error
	if tm.TableExists("error_table") {
		t.Error("Table should be removed even if close failed")
	}

	// Test Clear with error files
	errorFile2 := &errorDbFile{
		mockDbFile: newMockDbFile(2, []types.Type{types.IntType}, []string{"id"}),
		closeError: true,
	}

	s2 := createTestSchema(errorFile2.GetID(), "error_table2", errorFile2.GetTupleDesc(), "id")
	tm.AddTable(errorFile2, s2)
	tm.Clear() // Should handle close errors gracefully

	if len(tm.GetAllTableNames()) != 0 {
		t.Error("Clear should remove all tables even with close errors")
	}
}

func TestTableManager_PropertyBasedScenarios(t *testing.T) {
	// Test that adding and immediately removing maintains empty state
	for i := 0; i < 100; i++ {
		tm := NewTableManager()
		tableName := fmt.Sprintf("prop_test_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})

		// Add then immediately remove
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile, s)
		if err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		err = tm.RemoveTable(tableName)
		if err != nil {
			t.Fatalf("Failed to remove table: %v", err)
		}

		// Verify empty state
		if len(tm.GetAllTableNames()) != 0 {
			t.Error("TableManager should be empty after add-remove cycle")
		}

		if err := tm.ValidateIntegrity(); err != nil {
			t.Errorf("Integrity validation failed: %v", err)
		}
	}
}

func TestTableManager_RenameTablePreservesReferences(t *testing.T) {
	tm := NewTableManager()
	dbFile := newMockDbFile(42, []types.Type{types.IntType, types.StringType}, []string{"id", "name"})

	s := createTestSchema(dbFile.GetID(), "original", dbFile.GetTupleDesc(), "id")
	err := tm.AddTable(dbFile, s)
	if err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Get original references
	originalTableInfo, _ := tm.nameToTable["original"]
	originalTupleDesc, err := tm.GetTupleDesc(42)
	if err != nil {
		t.Fatalf("Failed to get tuple desc: %v", err)
	}
	originalDbFile, err := tm.GetDbFile(42)
	if err != nil {
		t.Fatalf("Failed to get db file: %v", err)
	}

	// Rename
	err = tm.RenameTable("original", "renamed")
	if err != nil {
		t.Fatalf("Failed to rename table: %v", err)
	}

	// Verify all references are preserved
	renamedTableInfo, exists := tm.nameToTable["renamed"]
	if !exists {
		t.Fatal("Renamed table not found")
	}

	if renamedTableInfo != originalTableInfo {
		t.Error("Table info reference should be preserved after rename")
	}

	newTupleDesc, err := tm.GetTupleDesc(42)
	if err != nil {
		t.Fatalf("Failed to get tuple desc after rename: %v", err)
	}

	if newTupleDesc != originalTupleDesc {
		t.Error("TupleDesc reference should be preserved after rename")
	}

	newDbFile, err := tm.GetDbFile(42)
	if err != nil {
		t.Fatalf("Failed to get db file after rename: %v", err)
	}

	if newDbFile != originalDbFile {
		t.Error("DbFile reference should be preserved after rename")
	}
}

func TestTableManager_ConcurrentAddRemoveCycles(t *testing.T) {
	tm := NewTableManager()
	numCycles := 1000
	numGoroutines := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < numCycles; i++ {
				tableName := fmt.Sprintf("cycle_table_%d_%d", goroutineID, i)
				tableID := goroutineID*numCycles + i + 1

				// Add table
				dbFile := newMockDbFile(tableID, []types.Type{types.IntType}, []string{"id"})
				s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
				err := tm.AddTable(dbFile, s)
				if err != nil {
					t.Errorf("Failed to add table %s: %v", tableName, err)
					continue
				}

				// Verify it exists
				if !tm.TableExists(tableName) {
					t.Errorf("Table %s should exist after adding", tableName)
				}

				// Remove table
				err = tm.RemoveTable(tableName)
				if err != nil {
					t.Errorf("Failed to remove table %s: %v", tableName, err)
				}

				// Verify it's gone
				if tm.TableExists(tableName) {
					t.Errorf("Table %s should not exist after removal", tableName)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify final state is clean
	if len(tm.GetAllTableNames()) != 0 {
		t.Errorf("Expected empty manager after all cycles, got %d tables", len(tm.GetAllTableNames()))
	}

	if err := tm.ValidateIntegrity(); err != nil {
		t.Errorf("Integrity validation failed after cycles: %v", err)
	}
}

func TestTableManager_StressTestWithRandomOperations(t *testing.T) {
	tm := NewTableManager()
	numOperations := 10000
	maxTables := 1000

	tableNames := make(map[string]bool)
	tableCounter := 0

	for i := 0; i < numOperations; i++ {
		operation := i % 4

		switch operation {
		case 0: // Add table
			if len(tableNames) < maxTables {
				tableName := fmt.Sprintf("stress_table_%d", tableCounter)
				tableCounter++
				dbFile := newMockDbFile(tableCounter, []types.Type{types.IntType}, []string{"id"})

				s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
				err := tm.AddTable(dbFile, s)
				if err == nil {
					tableNames[tableName] = true
				}
			}

		case 1: // Remove table
			if len(tableNames) > 0 {
				// Pick a random table to remove
				for tableName := range tableNames {
					err := tm.RemoveTable(tableName)
					if err == nil {
						delete(tableNames, tableName)
					}
					break
				}
			}

		case 2: // Check existence
			for tableName := range tableNames {
				if !tm.TableExists(tableName) {
					t.Errorf("Table %s should exist but doesn't", tableName)
				}
				break
			}

		case 3: // Get all names
			allNames := tm.GetAllTableNames()
			if len(allNames) != len(tableNames) {
				t.Errorf("Expected %d tables, got %d", len(tableNames), len(allNames))
			}
		}

		// Periodically validate integrity
		if i%1000 == 0 {
			if err := tm.ValidateIntegrity(); err != nil {
				t.Errorf("Integrity validation failed at operation %d: %v", i, err)
			}
		}
	}

	// Final integrity check
	if err := tm.ValidateIntegrity(); err != nil {
		t.Errorf("Final integrity validation failed: %v", err)
	}
}

func TestTableManager_ExtremeEdgeCases(t *testing.T) {
	t.Run("Table with very long name", func(t *testing.T) {
		tm := NewTableManager()
		longName := string(make([]byte, 10000))
		for i := range longName {
			longName = longName[:i] + "a" + longName[i+1:]
		}

		dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), longName, dbFile.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile, s)
		if err != nil {
			t.Fatalf("Failed to add table with long name: %v", err)
		}

		if !tm.TableExists(longName) {
			t.Error("Table with long name should exist")
		}
	})

	t.Run("Many tables with similar names", func(t *testing.T) {
		tm := NewTableManager()
		basePrefix := "similar_table_name_"
		numTables := 1000

		for i := 0; i < numTables; i++ {
			tableName := fmt.Sprintf("%s%d", basePrefix, i)
			dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})

			s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
			err := tm.AddTable(dbFile, s)
			if err != nil {
				t.Fatalf("Failed to add table %s: %v", tableName, err)
			}
		}

		// Verify all tables exist
		for i := 0; i < numTables; i++ {
			tableName := fmt.Sprintf("%s%d", basePrefix, i)
			if !tm.TableExists(tableName) {
				t.Errorf("Table %s should exist", tableName)
			}
		}
	})

	t.Run("Table operations with nil fields", func(t *testing.T) {
		tm := NewTableManager()

		// Test GetTableID with empty string
		_, err := tm.GetTableID("")
		if err == nil {
			t.Error("GetTableID with empty string should return error")
		}

		// Test RenameTable with same name
		dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), "test", dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)

		err = tm.RenameTable("test", "test")
		if err == nil {
			t.Error("RenameTable with same name should return error")
		}
	})
}

func TestTableManager_RaceConditionDetection(t *testing.T) {
	tm := NewTableManager()
	numGoroutines := 50
	duration := 2 * time.Second

	var operations int64
	done := make(chan struct{})

	// Start timer
	go func() {
		time.Sleep(duration)
		close(done)
	}()

	var wg sync.WaitGroup

	// Concurrent operations that could cause race conditions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			tableCounter := 0
			for {
				select {
				case <-done:
					return
				default:
					// Rapid add/remove cycles
					tableName := fmt.Sprintf("race_table_%d_%d", goroutineID, tableCounter)
					tableID := goroutineID*10000 + tableCounter + 1
					tableCounter++

					dbFile := newMockDbFile(tableID, []types.Type{types.IntType}, []string{"id"})
					s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
					tm.AddTable(dbFile, s)
					tm.TableExists(tableName)
					tm.GetAllTableNames()
					tm.RemoveTable(tableName)

					atomic.AddInt64(&operations, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state integrity
	if err := tm.ValidateIntegrity(); err != nil {
		t.Errorf("Race condition detected - integrity validation failed: %v", err)
	}

	t.Logf("Completed %d operations in %v without race conditions", operations, duration)
}

func TestTableManager_DeepRecursiveOperations(t *testing.T) {
	tm := NewTableManager()
	depth := 1000

	// Create a chain of operations that could cause stack overflow
	for i := 0; i < depth; i++ {
		tableName := fmt.Sprintf("deep_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})

		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		err := tm.AddTable(dbFile, s)
		if err != nil {
			t.Fatalf("Failed at depth %d: %v", i, err)
		}

		// Verify each addition
		if !tm.TableExists(tableName) {
			t.Fatalf("Table should exist at depth %d", i)
		}

		// Test operations at each level
		if i%100 == 0 {
			allNames := tm.GetAllTableNames()
			if len(allNames) != i+1 {
				t.Errorf("Expected %d tables at depth %d, got %d", i+1, i, len(allNames))
			}
		}
	}

	// Remove all in reverse order
	for i := depth - 1; i >= 0; i-- {
		tableName := fmt.Sprintf("deep_table_%d", i)
		err := tm.RemoveTable(tableName)
		if err != nil {
			t.Fatalf("Failed to remove at depth %d: %v", i, err)
		}
	}

	if len(tm.GetAllTableNames()) != 0 {
		t.Error("All tables should be removed after deep operations")
	}
}

func BenchmarkTableManager_AddTable(b *testing.B) {
	tm := NewTableManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}
}

func BenchmarkTableManager_GetTableID(b *testing.B) {
	tm := NewTableManager()
	numTables := 10000

	// Setup tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i%numTables)
		tm.GetTableID(tableName)
	}
}

func BenchmarkTableManager_TableExists(b *testing.B) {
	tm := NewTableManager()
	numTables := 10000

	// Setup tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i%numTables)
		tm.TableExists(tableName)
	}
}

func BenchmarkTableManager_GetAllTableNames(b *testing.B) {
	tm := NewTableManager()
	numTables := 1000

	// Setup tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.GetAllTableNames()
	}
}

func BenchmarkTableManager_ConcurrentAccess(b *testing.B) {
	tm := NewTableManager()
	numTables := 1000

	// Setup tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			tableName := fmt.Sprintf("bench_table_%d", counter%numTables)
			tm.TableExists(tableName)
			counter++
		}
	})
}

func BenchmarkTableManager_ValidateIntegrity(b *testing.B) {
	tm := NewTableManager()
	numTables := 1000

	// Setup tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%d", i)
		dbFile := newMockDbFile(i+1, []types.Type{types.IntType}, []string{"id"})
		s := createTestSchema(dbFile.GetID(), tableName, dbFile.GetTupleDesc(), "id")
		tm.AddTable(dbFile, s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.ValidateIntegrity()
	}
}
