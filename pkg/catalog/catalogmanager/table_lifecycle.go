package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
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
func (cm *CatalogManager) CreateTable(
	tx TxContext,
	tableSchema TableSchema,
) (tableID int, err error) {
	if tableSchema == nil {
		return 0, fmt.Errorf("schema cannot be nil")
	}

	fileName := tableSchema.TableName + ".dat"
	fullPath := filepath.Join(cm.dataDir, fileName)

	heapFile, err := heap.NewHeapFile(fullPath, tableSchema.TupleDesc)
	if err != nil {
		return 0, fmt.Errorf("failed to create heap file: %w", err)
	}

	tableID = heapFile.GetID()
	tableSchema.TableID = tableID
	for i := range tableSchema.Columns {
		tableSchema.Columns[i].TableID = tableID
	}

	if err := cm.RegisterTable(tx, tableSchema, fullPath); err != nil {
		heapFile.Close()
		return 0, fmt.Errorf("failed to register table in catalog: %w", err)
	}

	if err := cm.tableCache.AddTable(heapFile, tableSchema); err != nil {
		if deleteErr := cm.DeleteCatalogEntry(tx, tableID); deleteErr != nil {
			fmt.Printf("Warning: failed to rollback catalog entry after cache failure: %v\n", deleteErr)
		}
		heapFile.Close()
		return 0, fmt.Errorf("failed to add table to cache: %w", err)
	}

	cm.store.RegisterDbFile(tableID, heapFile)
	if _, verifyErr := cm.tableCache.GetDbFile(tableID); verifyErr != nil {
		return 0, fmt.Errorf("table was added to cache but immediate verification failed: %w", verifyErr)
	}
	return tableID, nil
}

// DropTable completely removes a table from both disk catalog and memory cache.
//
// Steps:
//  1. Unregisters the table from page store
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

	// Load table from disk
	tm, err := cm.GetTableMetadataByName(tx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	sch, err := cm.LoadTableSchema(tx, tm.TableID, tm.TableName)
	if err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	heapFile, err := heap.NewHeapFile(tm.FilePath, sch.TupleDesc)
	if err != nil {
		return fmt.Errorf("failed to open heap file: %w", err)
	}

	if err := cm.tableCache.AddTable(heapFile, sch); err != nil {
		heapFile.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	cm.store.RegisterDbFile(tm.TableID, heapFile)
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
	defer cm.store.CommitTransaction(tx)

	return cm.iterateTable(cm.SystemTabs.TablesTableID, tx, func(tableTuple *tuple.Tuple) error {
		table, err := systemtable.Tables.Parse(tableTuple)
		if err != nil {
			return err
		}

		if err := cm.LoadTable(tx, table.TableName); err != nil {
			return fmt.Errorf("error loading the table %s: %v", table.TableName, err)
		}

		return nil
	})
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

	tableID, err := cm.GetTableID(tx, oldName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", oldName, err)
	}

	if err := cm.tableCache.RenameTable(oldName, newName); err != nil {
		return fmt.Errorf("failed to rename in memory: %w", err)
	}

	tm, err := cm.GetTableMetadataByID(tx, tableID)
	if err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to find table metadata: %w", err)
	}

	if err := cm.DeleteTableFromSysTable(tx, tableID, cm.SystemTabs.TablesTableID); err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to delete old catalog entry: %w", err)
	}

	tm.TableName = newName
	tup := systemtable.Tables.CreateTuple(*tm)

	tablesFile, err := cm.tableCache.GetDbFile(cm.SystemTabs.TablesTableID)
	if err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to get tables catalog file: %w", err)
	}

	if err := cm.tupMgr.InsertTuple(tx, tablesFile, tup); err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to insert new catalog entry: %w", err)
	}

	return nil
}
