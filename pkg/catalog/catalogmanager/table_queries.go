package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/page"
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
func (cm *CatalogManager) GetTableID(tx TxContext, tableName string) (int, error) {
	if id, err := cm.tableCache.GetTableID(tableName); err == nil {
		return id, nil
	}

	if md, err := cm.GetTableMetadataByName(tx, tableName); err == nil {
		return md.TableID, nil
	}

	return -1, fmt.Errorf("table %s not found", tableName)
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
func (cm *CatalogManager) GetTableName(tx TxContext, tableID int) (string, error) {
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
func (cm *CatalogManager) GetTableSchema(tx TxContext, tableID int) (*schema.Schema, error) {
	if info, err := cm.tableCache.GetTableInfo(tableID); err == nil {
		return info.Schema, nil
	}

	// Get table metadata to retrieve the table name
	tm, err := cm.GetTableMetadataByID(tx, tableID)
	if err != nil {
		return nil, err
	}

	schema, err := cm.LoadTableSchema(tx, tableID, tm.TableName)
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
func (cm *CatalogManager) GetTableFile(tableID int) (page.DbFile, error) {
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
//                      If false, returns tables from memory cache (faster).
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

	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.TableName)
	}

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

// ClearCache removes all tables from memory cache.
//
// This is useful for:
//  - Clean database shutdown
//  - Testing
//  - Forcing a full reload from disk
//
// Note: This does NOT modify disk catalog - only clears memory.
func (cm *CatalogManager) ClearCache() {
	tableNames := cm.tableCache.GetAllTableNames()
	for _, name := range tableNames {
		if tableID, err := cm.tableCache.GetTableID(name); err == nil {
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
func (cm *CatalogManager) GetAutoIncrementColumn(tx TxContext, tableID int) (AutoIncrementInfo, error) {
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
func (cm *CatalogManager) IncrementAutoIncrementValue(tx TxContext, tableID int, columnName string, newValue int) error {
	return cm.colOps.IncrementAutoIncrementValue(tx, tableID, columnName, newValue)
}
