package memory

import (
	"fmt"
	"maps"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/page"
	"strings"
	"sync"
)

// TableInfo holds metadata about a table
type TableInfo struct {
	File   page.DbFile    // The file storing the table data
	Schema *schema.Schema // The table schema
}

// NewTableInfo creates a new table info instance
func NewTableInfo(file page.DbFile, schema *schema.Schema) *TableInfo {
	return &TableInfo{
		File:   file,
		Schema: schema,
	}
}

// GetID returns the table's unique identifier
func (ti *TableInfo) GetID() int {
	return ti.File.GetID()
}

// TableManager manages the catalog of database tables, providing thread-safe operations
// for adding, removing, and querying table metadata. It maintains bidirectional mappings
// between table names and IDs for efficient lookups.
type TableManager struct {
	nameToTable map[string]*TableInfo // Maps table names to TableInfo objects
	idToTable   map[int]*TableInfo    // Maps table IDs to TableInfo objects
	mutex       sync.RWMutex          // Protects concurrent access to the maps
}

// NewTableManager creates a new empty TableManager instance.
func NewTableManager() *TableManager {
	return &TableManager{
		nameToTable: make(map[string]*TableInfo),
		idToTable:   make(map[int]*TableInfo),
	}
}

// AddTable adds a new table to the catalog with the specified database file, name, and schema.
// If a table with the same name or ID already exists, it will be replaced.
func (tm *TableManager) AddTable(f page.DbFile, schema *schema.Schema) error {
	name := schema.TableName
	if f == nil {
		return fmt.Errorf("file cannot be nil")
	}
	if name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tableInfo := NewTableInfo(f, schema)
	tid := f.GetID()

	tm.removeExistingTable(name, tid)
	tm.addTableToMaps(name, tid, tableInfo)
	return nil
}

// GetTableID retrieves the unique identifier for a table given its name.
func (tm *TableManager) GetTableID(tableName string) (int, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.nameToTable[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' not found", tableName)
	}

	return tableInfo.GetID(), nil
}

// RemoveTable removes a table from the catalog and closes its associated database file.
// This operation is irreversible and will close the underlying file handle.
func (tm *TableManager) RemoveTable(name string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tableInfo, exists := tm.nameToTable[name]
	if !exists {
		return fmt.Errorf("table '%s' not found", name)
	}

	if tableInfo.File != nil {
		if err := tableInfo.File.Close(); err != nil {
			fmt.Printf("Warning: failed to close file for table '%s': %v\n", name, err)
		}
	}

	delete(tm.nameToTable, name)
	delete(tm.idToTable, tableInfo.GetID())
	return nil
}

// Clear removes all tables from the catalog and closes all associated database files.
// This operation cannot be undone. File closure errors are logged as warnings.
func (tm *TableManager) Clear() {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for _, tableInfo := range tm.idToTable {
		if tableInfo.File == nil {
			continue
		}

		if err := tableInfo.File.Close(); err != nil {
			fmt.Printf("Warning: failed to close file for table '%s': %v\n", tableInfo.Schema.TableName, err)
		}
	}

	clear(tm.nameToTable)
	clear(tm.idToTable)
}

// ValidateIntegrity performs internal consistency checks on the catalog data structures.
// This method verifies that the bidirectional mappings between names and IDs are consistent.
//
// Returns an error if any integrity violations are detected, nil otherwise.
func (tm *TableManager) ValidateIntegrity() error {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	if len(tm.nameToTable) != len(tm.idToTable) {
		return fmt.Errorf("catalog integrity violation: map size mismatch")
	}

	for name, table := range tm.nameToTable {
		if t, exists := tm.idToTable[table.GetID()]; !exists {
			return fmt.Errorf("catalog integrity violation: table %s missing from ID map", name)
		} else if t != table {
			return fmt.Errorf("catalog integrity violation: table %s reference mismatch", name)
		}
	}

	for id, table := range tm.idToTable {
		if otherTable, exists := tm.nameToTable[table.Schema.TableName]; !exists {
			return fmt.Errorf("catalog integrity violation: table ID %d missing from name map", id)
		} else if otherTable != table {
			return fmt.Errorf("catalog integrity violation: table ID %d reference mismatch", id)
		}
	}

	return nil
}

// GetAllTableNames returns a slice containing the names of all tables in the catalog.
// The returned slice is a copy and can be safely modified without affecting the catalog.
func (tm *TableManager) GetAllTableNames() []string {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	return slices.Collect(maps.Keys(tm.nameToTable))
}

// removeExistingTable removes any existing table with the given name or ID from both maps.
// This is a helper method used internally during table addition to handle replacements.
// Must be called with write lock held.
func (tm *TableManager) removeExistingTable(name string, tableID int) {
	if existingTable, exists := tm.nameToTable[name]; exists {
		delete(tm.idToTable, existingTable.GetID())
	}
	if existingTable, exists := tm.idToTable[tableID]; exists {
		delete(tm.nameToTable, existingTable.Schema.TableName)
	}
}

// addTableToMaps adds a table to both the name-to-table and ID-to-table mappings.
// This is a helper method used internally during table addition.
// Must be called with write lock held.
func (tm *TableManager) addTableToMaps(name string, tableID int, tableInfo *TableInfo) {
	tm.nameToTable[name] = tableInfo
	tm.idToTable[tableID] = tableInfo
}

// TableExists checks whether a table with the given name exists in the catalog.
func (tm *TableManager) TableExists(name string) bool {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	_, exists := tm.nameToTable[name]
	return exists
}

func (tm *TableManager) GetTableInfo(tableID int) (*TableInfo, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.idToTable[tableID]
	if !exists {
		return nil, fmt.Errorf("table with ID %d not found", tableID)
	}
	return tableInfo, nil
}

// RenameTable changes the name of an existing table in the catalog.
// The operation maintains all other table metadata and file associations.
func (tm *TableManager) RenameTable(oldName, newName string) error {
	if oldName == "" || newName == "" {
		return fmt.Errorf("table names cannot be empty")
	}
	if strings.TrimSpace(newName) != newName {
		return fmt.Errorf("new table name cannot have leading or trailing whitespace")
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tableInfo, exists := tm.nameToTable[oldName]
	if !exists {
		return fmt.Errorf("table '%s' not found", oldName)
	}

	if _, exists := tm.nameToTable[newName]; exists {
		return fmt.Errorf("table '%s' already exists", newName)
	}

	tableInfo.Schema.TableName = newName
	delete(tm.nameToTable, oldName)
	tm.nameToTable[newName] = tableInfo

	return nil
}
