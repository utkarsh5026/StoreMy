package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
)

// CreateTable creates a new table in the database.
//
// This is a multi-step process that creates both the physical storage file
// and the catalog metadata entries. The operation is atomic - if any step fails,
// the table will not be created.
//
// Steps performed:
//  1. Validates that the schema is not nil
//  2. Acquires a lock to prevent race conditions during creation
//  3. Checks if a table with the same name already exists
//  4. Creates the physical heap file on disk
//  5. Registers the table metadata in the catalog (CATALOG_TABLES and CATALOG_COLUMNS)
//  6. Adds the table to the in-memory cache
//  7. Registers the heap file with the page store
//
// The function is thread-safe and uses cm.mu to synchronize access.
//
// Parameters:
//   - tx: Transaction context for catalog operations
//   - sch: TableSchema containing table name, columns, and tuple descriptor
//
// Returns:
//   - int: The auto-generated table ID
//   - error: nil on success, error describing the failure otherwise
//
// Example:
//
//	schema := NewTableSchema("users", columns, tupleDesc)
//	tableID, err := cm.CreateTable(tx, schema)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (cm *CatalogManager) CreateTable(tx TxContext, sch TableSchema) (primitives.FileID, error) {
	if sch == nil {
		return 0, fmt.Errorf("schema cannot be nil")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.TableExists(tx, sch.TableName) {
		return 0, fmt.Errorf("table %s already exists", sch.TableName)
	}

	heapFile, err := cm.createTableFile(sch)
	if err != nil {
		return 0, err
	}

	if err := cm.registerTable(tx, sch, heapFile.FilePath()); err != nil {
		_ = heapFile.Close()
		return 0, fmt.Errorf("failed to register table in catalog: %w", err)
	}

	if err := cm.addTableToCache(tx, heapFile, sch); err != nil {
		return 0, err
	}

	return sch.TableID, nil
}

// createTableFile creates the physical heap file for a table.
//
// The file is created in the data directory with the naming convention:
// <table_name>.dat
//
// After creation, the table ID and column table IDs are updated in the schema
// to match the auto-generated ID from the heap file.
//
// Parameters:
//   - sch: TableSchema containing the table structure
//
// Returns:
//   - *heap.HeapFile: The newly created heap file
//   - error: nil on success, error if file creation fails
func (cm *CatalogManager) createTableFile(sch TableSchema) (*heap.HeapFile, error) {
	fileName := sch.TableName + ".dat"
	fullPath := primitives.Filepath(cm.dataDir).Join(fileName)

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

// addTableToCache adds a newly created table to the in-memory cache.
//
// This is an internal method called by CreateTable. It assumes the caller
// holds the cm.mu lock for thread safety.
//
// If adding to cache fails, the method attempts to rollback by:
//  1. Deleting the catalog entry
//  2. Closing the heap file
//
// The method also:
//   - Tracks the open file in cm.openFiles
//   - Registers the file with the page store
//   - Verifies the cache entry was successfully added
//
// Parameters:
//   - tx: Transaction context for potential rollback operations
//   - file: The heap file to cache
//   - sch: The table schema
//
// Returns:
//   - error: nil on success, error if caching or verification fails
func (cm *CatalogManager) addTableToCache(tx TxContext, file *heap.HeapFile, sch TableSchema) error {
	if err := cm.tableCache.AddTable(file, sch); err != nil {
		if deleteErr := cm.DeleteCatalogEntry(tx, sch.TableID); deleteErr != nil {
			fmt.Printf("Warning: failed to rollback catalog entry after cache failure: %v\n", deleteErr)
		}
		_ = file.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	cm.openFiles[sch.TableID] = file

	cm.store.RegisterDbFile(sch.TableID, file)
	if _, verifyErr := cm.tableCache.GetDbFile(sch.TableID); verifyErr != nil {
		return fmt.Errorf("table was added to cache but immediate verification failed: %w", verifyErr)
	}

	return nil
}

// DropTable permanently removes a table from the database.
//
// This operation removes both the in-memory representation and the catalog
// metadata, but does NOT delete the physical heap file from disk. The physical
// file should be manually deleted if needed.
//
// Steps performed:
//  1. Looks up the table ID by name
//  2. Removes the table from the in-memory cache (so queries immediately stop finding it)
//  3. Closes and removes the open heap file handle
//  4. Un-registers the file from the page store
//  5. Deletes entries from CATALOG_TABLES and CATALOG_COLUMNS
//
// The operation is atomic - if cache removal fails, the table is not dropped.
// If disk catalog deletion fails after cache removal, the operation attempts to
// rollback by re-adding the table to cache.
//
// The function is thread-safe for file handle operations.
//
// Parameters:
//   - tx: Transaction context for catalog operations
//   - tableName: Name of the table to drop
//
// Returns:
//   - error: nil on success, error if table not found or deletion fails
func (cm *CatalogManager) DropTable(tx TxContext, tableName string) error {
	tableID, err := cm.GetTableID(tx, tableName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", tableName, err)
	}

	// Get table info before removing from cache (needed for potential rollback)
	tableInfo, err := cm.tableCache.GetTableInfo(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// Step 1: Remove from cache FIRST so queries immediately stop finding it
	if err := cm.tableCache.RemoveTable(tableName); err != nil {
		return fmt.Errorf("failed to remove table from cache: %w", err)
	}

	// Step 2: Close and remove file handle
	cm.mu.Lock()
	heapFile, exists := cm.openFiles[tableID]
	if exists {
		delete(cm.openFiles, tableID)
	}
	cm.mu.Unlock()

	if exists {
		_ = heapFile.Close()
	}

	// Step 3: Unregister from page store
	cm.store.UnregisterDbFile(tableID)

	// Step 4: Delete from disk catalog
	if err := cm.DeleteCatalogEntry(tx, tableID); err != nil {
		// Rollback: Try to re-add table to cache
		if rollbackErr := cm.tableCache.AddTable(tableInfo.File, tableInfo.Schema); rollbackErr != nil {
			fmt.Printf("CRITICAL: failed to rollback cache after disk deletion failure: %v (original error: %v)\n", rollbackErr, err)
		}
		return fmt.Errorf("failed to delete catalog entry: %w", err)
	}

	return nil
}

// LoadTable loads a table from disk into memory on-demand.
//
// This is used for lazy loading - tables are only loaded when first accessed
// rather than all at startup. If the table is already in cache, this is a no-op.
//
// Steps performed:
//  1. Checks if table already exists in cache
//  2. Reads table metadata from CATALOG_TABLES
//  3. Reconstructs the schema from CATALOG_COLUMNS
//  4. Opens the heap file from disk
//  5. Adds table to in-memory cache
//  6. Registers with the page store
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableName: Name of the table to load
//
// Returns:
//   - error: nil on success, error if table doesn't exist or loading fails
func (cm *CatalogManager) LoadTable(tx TxContext, tableName string) error {
	if cm.tableCache.TableExists(tableName) {
		return nil
	}

	sch, filePath, err := cm.loadFromDisk(tx, tableName)
	if err != nil {
		return err
	}

	return cm.openTable(filePath, sch)
}

// loadFromDisk retrieves table metadata and schema from the catalog.
//
// This is an internal helper method that reads from CATALOG_TABLES and
// CATALOG_COLUMNS to reconstruct a complete TableSchema object.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableName: Name of the table to load
//
// Returns:
//   - TableSchema: The reconstructed schema with columns and tuple descriptor
//   - string: The file path to the heap file
//   - error: nil on success, error if metadata cannot be read
func (cm *CatalogManager) loadFromDisk(tx TxContext, tableName string) (TableSchema, primitives.Filepath, error) {
	tm, err := cm.GetTableMetadataByName(tx, tableName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get table metadata: %w", err)
	}

	sch, err := cm.LoadTableSchema(tx, tm.TableID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to load schema: %w", err)
	}

	return sch, tm.FilePath, nil
}

// registerTable opens a heap file and registers it with the cache and page store.
//
// This is an internal helper method used during table loading. It handles
// the physical file opening and registration with all necessary components.
//
// If adding to cache fails, the heap file is automatically closed to prevent
// resource leaks.
//
// Parameters:
//   - filePath: Absolute path to the heap file
//   - sch: The table schema
//
// Returns:
//   - error: nil on success, error if file cannot be opened or registered
func (cm *CatalogManager) openTable(filePath primitives.Filepath, sch TableSchema) error {
	heapFile, err := heap.NewHeapFile(filePath, sch.TupleDesc)
	if err != nil {
		return fmt.Errorf("failed to open heap file: %w", err)
	}

	if err := cm.tableCache.AddTable(heapFile, sch); err != nil {
		_ = heapFile.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	cm.mu.Lock()
	cm.openFiles[sch.TableID] = heapFile
	cm.mu.Unlock()

	cm.store.RegisterDbFile(sch.TableID, heapFile)
	return nil
}

// LoadAllTables loads all user tables from disk into memory during database startup.
//
// This reads CATALOG_TABLES, reconstructs schemas from CATALOG_COLUMNS,
// opens heap files, and registers everything with the page store.
//
// System tables (CATALOG_TABLES, CATALOG_COLUMNS) are not loaded by this
// method as they are managed separately.
//
// The operation stops at the first error encountered. Previously loaded
// tables remain in memory.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//
// Returns:
//   - error: nil if all tables loaded successfully, error describing which table failed
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
// This is an atomic operation that updates the table name in:
//  1. In-memory cache (tableCache)
//  2. CATALOG_TABLES entry on disk
//
// If any step fails, the in-memory rename is rolled back to maintain consistency.
//
// Steps performed:
//  1. Validates new name is not already taken
//  2. Renames in in-memory cache
//  3. Updates CATALOG_TABLES entry (via UpdateBy operation)
//  4. On failure, reverts the in-memory rename
//
// The physical heap file is NOT renamed - only the logical table name changes.
//
// Parameters:
//   - tx: Transaction context for catalog updates
//   - oldName: Current table name
//   - newName: New table name (must not exist)
//
// Returns:
//   - error: nil on success, error if validation fails or rename cannot complete
//
// Example:
//
//	err := cm.RenameTable(tx, "users", "customers")
//	if err != nil {
//	    log.Printf("Rename failed: %v", err)
//	}
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
		_ = cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to insert new catalog entry: %w", err)
	}
	return nil
}
