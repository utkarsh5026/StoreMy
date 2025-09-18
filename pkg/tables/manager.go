package tables

import (
	"fmt"
	"storemy/pkg/storage"
	"storemy/pkg/tuple"
	"sync"
)

type TableManager struct {
	nameToTable map[string]*TableInfo
	idToTable   map[int]*TableInfo
	mutex       sync.RWMutex
}

// NewCatalog creates a new empty catalog
func NewTableManager() *TableManager {
	return &TableManager{
		nameToTable: make(map[string]*TableInfo),
		idToTable:   make(map[int]*TableInfo),
	}
}

// AddTable adds a new table to the catalog, replacing any existing table with the same name or ID
func (tm *TableManager) AddTable(f storage.DbFile, name, pKey string) error {
	if f == nil {
		return fmt.Errorf("file cannot be nil")
	}
	if name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tableInfo := NewTableInfo(f, name, pKey)
	tid := f.GetID()

	if t, exists := tm.nameToTable[name]; exists {
		delete(tm.idToTable, t.GetID())
	}

	if t, exists := tm.idToTable[tid]; exists {
		delete(tm.nameToTable, t.Name)
	}

	tm.nameToTable[name] = tableInfo
	tm.idToTable[tid] = tableInfo
	return nil
}

// GetTableID returns the ID of the table with the specified name
func (tm *TableManager) GetTableID(name string) (int, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.nameToTable[name]
	if !exists {
		return 0, fmt.Errorf("table '%s' not found", name)
	}

	return tableInfo.GetID(), nil
}

// GetTableName returns the name of the table with the specified ID
func (tm *TableManager) GetTableName(tableID int) (string, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.idToTable[tableID]
	if !exists {
		return "", fmt.Errorf("table with ID %d not found", tableID)
	}

	return tableInfo.Name, nil
}

// GetTupleDesc returns the schema for the table with the specified ID
func (tm *TableManager) GetTupleDesc(tableID int) (*tuple.TupleDescription, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.idToTable[tableID]
	if !exists {
		return nil, fmt.Errorf("table with ID %d not found", tableID)
	}

	return tableInfo.TupleDesc, nil
}

func (tm *TableManager) Clear() {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for _, tableInfo := range tm.idToTable {
		tableInfo.File.Close()
	}

	tm.nameToTable = make(map[string]*TableInfo)
	tm.idToTable = make(map[int]*TableInfo)
}

// GetDbFile returns the DbFile for the table with the specified ID
func (tm *TableManager) GetDbFile(tableID int) (storage.DbFile, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	ti, exists := tm.idToTable[tableID]
	if !exists {
		return nil, fmt.Errorf("table with ID %d not found", tableID)
	}

	return ti.File, nil
}

// ValidateIntegrity performs basic integrity checks on the catalog
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
		if otherTable, exists := tm.nameToTable[table.Name]; !exists {
			return fmt.Errorf("catalog integrity violation: table ID %d missing from name map", id)
		} else if otherTable != table {
			return fmt.Errorf("catalog integrity violation: table ID %d reference mismatch", id)
		}
	}

	return nil
}

// GetAllTableNames returns a slice of all table names in the catalog
func (tm *TableManager) GetAllTableNames() []string {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	names := make([]string, 0, len(tm.nameToTable))
	for name := range tm.nameToTable {
		names = append(names, name)
	}

	return names
}
