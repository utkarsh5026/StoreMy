package tables

import (
	"fmt"
	"sort"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"strings"
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

func (tm *TableManager) AddTable(f page.DbFile, name, pKey string) error {
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

	tm.removeExistingTable(name, tid)
	tm.addTableToMaps(name, tid, tableInfo)
	return nil
}

func (tm *TableManager) GetTableID(name string) (int, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.nameToTable[name]
	if !exists {
		return 0, fmt.Errorf("table '%s' not found", name)
	}

	return tableInfo.GetID(), nil
}

func (tm *TableManager) GetTableName(tableID int) (string, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.idToTable[tableID]
	if !exists {
		return "", fmt.Errorf("table with ID %d not found", tableID)
	}

	return tableInfo.Name, nil
}

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

func (tm *TableManager) GetTupleDesc(tableID int) (*tuple.TupleDescription, error) {
	tableInfo, err := tm.getTableInfo(tableID)
	if err != nil {
		return nil, err
	}
	return tableInfo.TupleDesc, nil
}

func (tm *TableManager) Clear() {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for _, tableInfo := range tm.idToTable {
		if tableInfo.File != nil {
			if err := tableInfo.File.Close(); err != nil {
				fmt.Printf("Warning: failed to close file for table '%s': %v\n", tableInfo.Name, err)
			}
		}
	}

	tm.nameToTable = make(map[string]*TableInfo)
	tm.idToTable = make(map[int]*TableInfo)
}

func (tm *TableManager) GetDbFile(tableID int) (page.DbFile, error) {
	ti, err := tm.getTableInfo(tableID)
	if err != nil {
		return nil, err
	}
	return ti.File, nil
}

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

func (tm *TableManager) String() string {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("TableManager(tables=%d):\n", len(tm.nameToTable)))

	names := make([]string, 0, len(tm.nameToTable))
	for name := range tm.nameToTable {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		table := tm.nameToTable[name]
		builder.WriteString(fmt.Sprintf("  %s\n", table.String()))
	}

	return builder.String()
}

func (tm *TableManager) GetAllTableNames() []string {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	names := make([]string, 0, len(tm.nameToTable))
	for name := range tm.nameToTable {
		names = append(names, name)
	}

	return names
}

func (tm *TableManager) removeExistingTable(name string, tableID int) {
	if existingTable, exists := tm.nameToTable[name]; exists {
		delete(tm.idToTable, existingTable.GetID())
	}
	if existingTable, exists := tm.idToTable[tableID]; exists {
		delete(tm.nameToTable, existingTable.Name)
	}
}

func (tm *TableManager) addTableToMaps(name string, tableID int, tableInfo *TableInfo) {
	tm.nameToTable[name] = tableInfo
	tm.idToTable[tableID] = tableInfo
}

func (tm *TableManager) getTableInfo(tableID int) (*TableInfo, error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	tableInfo, exists := tm.idToTable[tableID]
	if !exists {
		return nil, fmt.Errorf("table with ID %d not found", tableID)
	}
	return tableInfo, nil
}

func (tm *TableManager) TableExists(name string) bool {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	_, exists := tm.nameToTable[name]
	return exists
}

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

	tableInfo.Name = newName
	delete(tm.nameToTable, oldName)
	tm.nameToTable[newName] = tableInfo

	return nil
}
