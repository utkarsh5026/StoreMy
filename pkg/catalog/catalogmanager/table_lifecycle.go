package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/storage/heap"
)

// CreateTable creates a new table with complete disk + cache registration.
//
// Steps:
//  1. Creates a new heap file on disk (path determined by CatalogManager)
//  2. Registers table metadata in CATALOG_TABLES
//  3. Registers column definitions in CATALOG_COLUMNS
//  4. Adds table to in-memory cache and page store
//
// This is a complete, atomic operation - the table is fully usable after this call.
//
// Parameters:
//   - tx: Transaction context for catalog updates
//   - tableSchema: Schema definition (TableID will be set to the heap file ID)
//
// Returns:
//   - tableID: The ID assigned to the new table
//   - err: Error if any step fails
func (cm *CatalogManager) CreateTable(tx TxContext, sch TableSchema) (int, error) {
	if sch == nil {
		return 0, fmt.Errorf("schema cannot be nil")
	}

	if cm.TableExists(tx, sch.TableName) {
		return 0, fmt.Errorf("table %s already exists", sch.TableName)
	}

	heapFile, err := cm.createTableFile(sch)
	if err != nil {
		return 0, err
	}

	if err := cm.RegisterTable(tx, sch, heapFile.FilePath()); err != nil {
		heapFile.Close()
		return 0, fmt.Errorf("failed to register table in catalog: %w", err)
	}

	if err := cm.addTableToCache(tx, heapFile, sch); err != nil {
		return 0, err
	}

	return sch.TableID, nil
}

func (cm *CatalogManager) createTableFile(sch TableSchema) (*heap.HeapFile, error) {
	fileName := sch.TableName + ".dat"
	fullPath := filepath.Join(cm.dataDir, fileName)

	heapFile, err := heap.NewHeapFile(fullPath, sch.TupleDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to create heap file: %w", err)
	}

	tableID := heapFile.GetID()
	sch.TableID = tableID
	for i := range sch.Columns {
		sch.Columns[i].TableID = tableID
	}

	return heapFile, err

}

func (cm *CatalogManager) addTableToCache(tx TxContext, file *heap.HeapFile, sch TableSchema) error {
	if err := cm.tableCache.AddTable(file, sch); err != nil {
		if deleteErr := cm.DeleteCatalogEntry(tx, sch.TableID); deleteErr != nil {
			fmt.Printf("Warning: failed to rollback catalog entry after cache failure: %v\n", deleteErr)
		}
		file.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	// CatalogManager owns the file - track it for lifecycle management
	cm.openFiles[sch.TableID] = file
	// Register with PageStore for I/O operations only
	cm.store.RegisterDbFile(sch.TableID, file)
	if _, verifyErr := cm.tableCache.GetDbFile(sch.TableID); verifyErr != nil {
		return fmt.Errorf("table was added to cache but immediate verification failed: %w", verifyErr)
	}

	return nil
}

// DropTable completely removes a table from both disk catalog and memory cache.
//
// Steps:
//  1. Un-registers the table from page store
//  2. Removes all catalog entries (CATALOG_TABLES, CATALOG_COLUMNS, etc.)
//  3. Removes the table from in-memory cache
//
// Note: This does NOT delete the heap file from disk - that must be done separately.
//
// Parameters:
//   - tx: Transaction context for catalog deletions
//   - tableName: Name of the table to drop
//
// Returns error if the table is not found or deletion fails.
func (cm *CatalogManager) DropTable(tx TxContext, tableName string) error {
	tableID, err := cm.GetTableID(tx, tableName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", tableName, err)
	}

	// Close the file handle before unregistering to prevent resource leaks
	if heapFile, exists := cm.openFiles[tableID]; exists {
		heapFile.Close()
		delete(cm.openFiles, tableID)
	}
	cm.store.UnregisterDbFile(tableID)
	if err := cm.DeleteCatalogEntry(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete catalog entry: %w", err)
	}

	if err := cm.tableCache.RemoveTable(tableName); err != nil {
		fmt.Printf("Warning: failed to remove table from cache: %v\n", err)
	}
	return nil
}

// LoadTable loads a specific table from disk into memory by name.
//
// This is useful for lazy loading tables on demand rather than loading all tables at startup.
// If the table is already in the cache, this is a no-op.
//
// Steps:
//  1. Checks if table already exists in cache (returns immediately if so)
//  2. Loads table metadata from CATALOG_TABLES
//  3. Reconstructs schema from CATALOG_COLUMNS
//  4. Opens the heap file
//  5. Adds to cache and registers with page store
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableName: Name of the table to load
//
// Returns error if the table cannot be found or loaded.
func (cm *CatalogManager) LoadTable(tx TxContext, tableName string) error {
	if cm.tableCache.TableExists(tableName) {
		return nil
	}

	sch, filePath, err := cm.loadFromDisk(tx, tableName)
	if err != nil {
		return err
	}

	return cm.registerTable(filePath, sch)
}

func (cm *CatalogManager) loadFromDisk(tx TxContext, tableName string) (TableSchema, string, error) {
	tm, err := cm.GetTableMetadataByName(tx, tableName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get table metadata: %w", err)
	}

	sch, err := cm.LoadTableSchema(tx, tm.TableID, tm.TableName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to load schema: %w", err)
	}

	return sch, tm.FilePath, nil
}

func (cm *CatalogManager) registerTable(filePath string, sch TableSchema) error {
	heapFile, err := heap.NewHeapFile(filePath, sch.TupleDesc)
	if err != nil {
		return fmt.Errorf("failed to open heap file: %w", err)
	}

	if err := cm.tableCache.AddTable(heapFile, sch); err != nil {
		heapFile.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	// CatalogManager owns the file - track it for lifecycle management
	cm.openFiles[sch.TableID] = heapFile
	// Register with PageStore for I/O operations only
	cm.store.RegisterDbFile(sch.TableID, heapFile)
	return nil
}

// LoadAllTables loads all user tables from disk into memory during database startup.
//
// This reads CATALOG_TABLES, reconstructs schemas from CATALOG_COLUMNS,
// opens heap files, and registers everything with the page store.
//
// The transaction is committed after all tables are successfully loaded.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//
// Returns error if any table cannot be loaded.
func (cm *CatalogManager) LoadAllTables(tx TxContext) error {
	tables, err := cm.tableOps.GetAllTables(tx)
	if err != nil {
		return fmt.Errorf("failed to read tables from catalog: %w", err)
	}

	for _, table := range tables {
		if err := cm.LoadTable(tx, table.TableName); err != nil {
			return fmt.Errorf("error loading the table %s: %v", table.TableName, err)
		}
	}

	return nil
}

// RenameTable renames a table in both memory and disk catalog.
//
// Steps:
//  1. Validates new name is not already taken
//  2. Renames in in-memory cache
//  3. Updates CATALOG_TABLES entry on disk (delete old + insert new)
//
// If any step fails, the in-memory rename is rolled back.
//
// Parameters:
//   - tx: Transaction context for catalog updates
//   - oldName: Current table name
//   - newName: New table name
//
// Returns error if the rename fails.
func (cm *CatalogManager) RenameTable(tx TxContext, oldName, newName string) error {
	if cm.TableExists(tx, newName) {
		return fmt.Errorf("table %s already exists", newName)
	}

	if err := cm.tableCache.RenameTable(oldName, newName); err != nil {
		return fmt.Errorf("failed to rename in memory: %w", err)
	}

	err := cm.tableOps.UpdateBy(tx,
		func(tm *systemtable.TableMetadata) bool {
			return tm.TableName == oldName
		},
		func(tm *systemtable.TableMetadata) *systemtable.TableMetadata {
			tm.TableName = newName
			return tm
		})

	if err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to insert new catalog entry: %w", err)
	}
	return nil
}
