package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/page"
	"storemy/pkg/utils/functools"
)

// GetTableID retrieves the table ID for a given table name.
//
// This checks the in-memory cache first (O(1)), then falls back to a disk scan if not found.
//
// Parameters:
//   - tx: Transaction context for reading catalog (only used if cache miss)
//   - tableName: Name of the table
//
// Returns:
//   - tableID: The table's ID
//   - error: Error if table is not found
func (cm *CatalogManager) GetTableID(tx TxContext, tableName string) (primitives.TableID, error) {
	if id, err := cm.tableCache.GetTableID(tableName); err == nil {
		return id, nil
	}

	if md, err := cm.GetTableMetadataByName(tx, tableName); err == nil {
		return md.TableID, nil
	}

	return 0, fmt.Errorf("table %s not found", tableName)
}

// GetTableName retrieves the table name for a given table ID.
//
// This checks the in-memory cache first (O(1)), then falls back to a disk scan if not found.
//
// Parameters:
//   - tx: Transaction context for reading catalog (only used if cache miss)
//   - tableID: ID of the table
//
// Returns:
//   - tableName: The table's name
//   - error: Error if table is not found
func (cm *CatalogManager) GetTableName(tx TxContext, tableID primitives.TableID) (string, error) {
	if info, err := cm.tableCache.GetTableInfo(tableID); err == nil {
		return info.Schema.TableName, nil
	}

	if md, err := cm.GetTableMetadataByID(tx, tableID); err == nil {
		return md.TableName, nil
	}

	return "", fmt.Errorf("table with ID %d not found", tableID)
}

// GetTableSchema retrieves the schema for a table.
//
// This checks the in-memory cache first, then loads from disk if necessary.
//
// Parameters:
//   - tx: Transaction context for reading catalog (only used if cache miss)
//   - tableID: ID of the table
//
// Returns:
//   - schema: The table's schema definition
//   - error: Error if schema cannot be retrieved
func (cm *CatalogManager) GetTableSchema(tx TxContext, tableID primitives.TableID) (*schema.Schema, error) {
	if info, err := cm.tableCache.GetTableInfo(tableID); err == nil {
		return info.Schema, nil
	}

	schema, err := cm.LoadTableSchema(tx, tableID)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// GetTableFile retrieves the DbFile for a table from the in-memory cache.
//
// Returns an error if the table is not loaded in memory.
//
// Parameters:
//   - tableID: ID of the table
//
// Returns:
//   - DbFile: The table's heap file
//   - error: Error if table is not in cache
func (cm *CatalogManager) GetTableFile(tableID primitives.TableID) (page.DbFile, error) {
	file, err := cm.tableCache.GetDbFile(tableID)
	if err != nil {
		allTables := cm.tableCache.GetAllTableNames()
		return nil, fmt.Errorf("table with ID %d not found in cache (cache has %d tables: %v): %w",
			tableID, len(allTables), allTables, err)
	}
	return file, nil
}

// TableExists checks if a table exists by name.
//
// Checks memory first (fast), then disk catalog (slower).
//
// Parameters:
//   - tx: Transaction context for reading catalog (only used if cache miss)
//   - tableName: Name of the table
//
// Returns:
//   - bool: true if table exists, false otherwise
func (cm *CatalogManager) TableExists(tx TxContext, tableName string) bool {
	if cm.tableCache.TableExists(tableName) {
		return true
	}
	_, err := cm.GetTableMetadataByName(tx, tableName)
	return err == nil
}

// ListAllTables returns all table names.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - refreshFromDisk: If true, scans CATALOG_TABLES (slower but includes unloaded tables).
//     If false, returns tables from memory cache (faster).
//
// Returns:
//   - []string: List of table names
//   - error: Error if disk scan fails (only when refreshFromDisk=true)
func (cm *CatalogManager) ListAllTables(tx TxContext, refreshFromDisk bool) ([]string, error) {
	if !refreshFromDisk {
		return cm.tableCache.GetAllTableNames(), nil
	}

	tables, err := cm.GetAllTables(tx)
	if err != nil {
		return nil, err
	}

	tableNames := functools.Map(tables,
		func(t *systemtable.TableMetadata) string { return t.TableName })

	return tableNames, nil
}

// ValidateIntegrity checks consistency between memory and disk catalog.
//
// This verifies:
//  1. Cache internal consistency (no orphaned entries)
//  2. All cached tables exist in disk catalog
//
// Parameters:
//   - tx: Transaction context for reading catalog
//
// Returns error if any inconsistencies are found.
func (cm *CatalogManager) ValidateIntegrity(tx TxContext) error {
	if err := cm.tableCache.ValidateIntegrity(); err != nil {
		return fmt.Errorf("cache integrity error: %w", err)
	}

	names := cm.tableCache.GetAllTableNames()
	for _, n := range names {
		if _, err := cm.GetTableMetadataByName(tx, n); err != nil {
			return fmt.Errorf("table %s exists in memory but not in disk catalog", n)
		}
	}
	return nil
}

// ClearCache removes all user tables from memory cache while preserving system tables.
//
// System tables (CATALOG_*) must remain in cache for the catalog to function properly.
// If you need to fully clear the cache including system tables, call ClearCacheCompletely().
//
// This is useful for:
//   - Clean database shutdown
//   - Testing
//   - Forcing a full reload of user tables from disk
//
// Note: This does NOT modify disk catalog - only clears memory.
func (cm *CatalogManager) ClearCache() {
	tableNames := cm.tableCache.GetAllTableNames()

	systemTables := map[string]bool{
		"CATALOG_TABLES":            true,
		"CATALOG_COLUMNS":           true,
		"CATALOG_STATISTICS":        true,
		"CATALOG_INDEXES":           true,
		"CATALOG_COLUMN_STATISTICS": true,
		"CATALOG_INDEX_STATISTICS":  true,
	}

	for _, name := range tableNames {
		if !systemTables[name] {
			if tableID, err := cm.tableCache.GetTableID(name); err == nil {
				cm.mu.Lock()
				heapFile, exists := cm.openFiles[tableID]
				if exists {
					delete(cm.openFiles, tableID)
				}
				cm.mu.Unlock()

				if exists {
					heapFile.Close()
				}
				cm.store.UnregisterDbFile(tableID)
			}
			cm.tableCache.RemoveTable(name)
		}
	}
}

// ClearCacheCompletely removes ALL tables from memory cache including system tables.
//
// WARNING: This will make the catalog manager unable to query the catalog until
// system tables are reloaded. Use ClearCache() instead unless you know what you're doing.
//
// This is primarily useful for complete database shutdown.
func (cm *CatalogManager) ClearCacheCompletely() {
	tableNames := cm.tableCache.GetAllTableNames()
	for _, name := range tableNames {
		if tableID, err := cm.tableCache.GetTableID(name); err == nil {
			cm.mu.Lock()
			heapFile, exists := cm.openFiles[tableID]
			if exists {
				delete(cm.openFiles, tableID)
			}
			cm.mu.Unlock()

			if exists {
				heapFile.Close()
			}
			cm.store.UnregisterDbFile(tableID)
		}
	}

	cm.tableCache.Clear()
}

// GetAutoIncrementColumn retrieves auto-increment column information for a table.
//
// Returns nil if the table has no auto-increment column.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//
// Returns:
//   - AutoIncrementInfo: Auto-increment metadata (nil if none)
//   - error: Error if catalog read fails
func (cm *CatalogManager) GetAutoIncrementColumn(tx TxContext, tableID primitives.TableID) (AutoIncrementInfo, error) {
	return cm.colOps.GetAutoIncrementColumn(tx, tableID)
}

// IncrementAutoIncrementValue updates the next auto-increment value for a table's auto-increment column.
//
// This implements MVCC by deleting the old tuple and inserting a new one with the updated value.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - tableID: ID of the table
//   - columnName: Name of the auto-increment column
//   - newValue: The new next_auto_value to set
//
// Returns error if the update fails.
func (cm *CatalogManager) IncrementAutoIncrementValue(tx TxContext, tableID primitives.TableID, columnName string, newValue uint64) error {
	return cm.colOps.IncrementAutoIncrementValue(tx, tableID, columnName, newValue)
}
