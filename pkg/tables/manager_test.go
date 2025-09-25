package tables

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"sync"
	"testing"
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

func (m *mockDbFile) ReadPage(pid tuple.PageID) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) WritePage(p page.Page) error {
	return nil
}

func (m *mockDbFile) AddTuple(tid *transaction.TransactionID, t *tuple.Tuple) ([]page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) DeleteTuple(tid *transaction.TransactionID, t *tuple.Tuple) (page.Page, error) {
	return nil, nil
}

func (m *mockDbFile) Iterator(tid *transaction.TransactionID) iterator.DbFileIterator {
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
				tm.AddTable(dbFile1, "table1", "id")
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

			err := tm.AddTable(dbFile, tt.tableName, tt.primaryKey)

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

			if tableInfo.Name != tt.tableName {
				t.Errorf("Expected table name %s, got %s", tt.tableName, tableInfo.Name)
			}

			if tableInfo.PrimaryKey != tt.primaryKey {
				t.Errorf("Expected primary key %s, got %s", tt.primaryKey, tableInfo.PrimaryKey)
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

			err := tm.AddTable(dbFile, tableName, primaryKey)

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
		err := tm.AddTable(dbFile1, "users", "id")
		if err != nil {
			t.Fatalf("Failed to add first table: %v", err)
		}

		// Replace with new table (same name, different ID)
		dbFile2 := newMockDbFile(2, []types.Type{types.StringType}, []string{"name"})
		err = tm.AddTable(dbFile2, "users", "name")
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
		err := tm.AddTable(dbFile1, "table1", "id")
		if err != nil {
			t.Fatalf("Failed to add first table: %v", err)
		}

		// Replace with new table (same ID, different name)
		dbFile2 := newMockDbFile(1, []types.Type{types.StringType}, []string{"name"})
		err = tm.AddTable(dbFile2, "table2", "name")
		if err != nil {
			t.Fatalf("Failed to replace table: %v", err)
		}

		// Verify replacement
		tableInfo, exists := tm.idToTable[1]
		if !exists {
			t.Fatal("Table with ID 1 not found after replacement")
		}

		if tableInfo.Name != "table2" {
			t.Errorf("Expected table name 'table2', got %s", tableInfo.Name)
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
	tm.AddTable(dbFile, "test_table", "id")

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
	tm.AddTable(dbFile, "test_table", "id")

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
			name, err := tm.GetTableName(tt.tableID)

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

			if name != tt.expectedName {
				t.Errorf("Expected name %s, got %s", tt.expectedName, name)
			}
		})
	}
}

func TestTableManager_GetTupleDesc(t *testing.T) {
	tm := NewTableManager()
	fieldTypes := []types.Type{types.IntType, types.StringType}
	fieldNames := []string{"id", "name"}
	dbFile := newMockDbFile(42, fieldTypes, fieldNames)
	tm.AddTable(dbFile, "test_table", "id")

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
	tm.AddTable(dbFile, "test_table", "id")

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

	tm.AddTable(dbFile1, "table1", "id")
	tm.AddTable(dbFile2, "table2", "name")

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
				tm.AddTable(dbFile, "table1", "id")
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
				tm.AddTable(dbFile1, "table1", "id")
				tm.AddTable(dbFile2, "table2", "name")
				return tm
			},
			expectedError: false,
		},
		{
			name: "Map size mismatch - invalid",
			setupFunc: func() *TableManager {
				tm := NewTableManager()
				dbFile := newMockDbFile(1, []types.Type{types.IntType}, []string{"id"})
				tm.AddTable(dbFile, "table1", "id")
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
				tm.AddTable(dbFile, "users", "id")
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
				tm.AddTable(dbFile1, "users", "id")
				tm.AddTable(dbFile2, "products", "name")
				tm.AddTable(dbFile3, "orders", "age")
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
				err := tm.AddTable(dbFile, tableName, "id")
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
