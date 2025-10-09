package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// CatalogManager coordinates between the persistent SystemCatalog and the runtime TableManager.
// It provides high-level, complete operations that handle both disk persistence and in-memory caching,
// eliminating the need to manage both components separately throughout the codebase.
//
// Design Philosophy:
//   - Complete operations: Each method handles BOTH disk and cache consistently
//   - Clear semantics: Method names clearly indicate what happens (Create/Drop/Load)
//   - Transaction aware: All mutations require explicit transaction context
//   - Cache-first reads: Performance optimization by checking memory before disk
type CatalogManager struct {
	catalog      *SystemCatalog
	tableManager *memory.TableManager
	store        *memory.PageStore
}

// NewCatalogManager creates a new CatalogManager instance.
func NewCatalogManager(ps *memory.PageStore, tm *memory.TableManager) *CatalogManager {
	return &CatalogManager{
		catalog:      NewSystemCatalog(ps, tm),
		tableManager: tm,
		store:        ps,
	}
}

// Initialize initializes the system catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS).
// This must be called once during database initialization before any other operations.
func (cm *CatalogManager) Initialize(ctx *transaction.TransactionContext, dataDir string) error {
	return cm.catalog.Initialize(ctx, dataDir)
}

// LoadAllTables loads all user tables from disk into memory during database startup.
// This reads CATALOG_TABLES, reconstructs schemas from CATALOG_COLUMNS,
// opens heap files, and registers everything with the TableManager.
func (cm *CatalogManager) LoadAllTables(tx *transaction.TransactionContext, dataDir string) error {
	return cm.catalog.LoadTables(tx, dataDir)
}

// CreateTable creates a new table with complete disk + cache registration.
// Steps:
//  1. Creates a new heap file on disk
//  2. Registers table metadata in CATALOG_TABLES
//  3. Registers column definitions in CATALOG_COLUMNS
//  4. Adds table to in-memory TableManager
//
// This is a complete, atomic operation - the table is fully usable after this call.
func (cm *CatalogManager) CreateTable(
	tx *transaction.TransactionContext,
	tableName string,
	filePath string,
	primaryKey string,
	fields []FieldMetadata,
	dataDir string,
) (tableID int, err error) {
	// Build schema from field metadata
	fieldTypes := make([]types.Type, len(fields))
	fieldNames := make([]string, len(fields))
	for i, f := range fields {
		fieldTypes[i] = f.Type
		fieldNames[i] = f.Name
	}

	schema, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		return 0, fmt.Errorf("failed to create tuple description: %w", err)
	}

	// Create heap file on disk
	fullPath := fmt.Sprintf("%s/%s", dataDir, filePath)
	heapFile, err := heap.NewHeapFile(fullPath, schema)
	if err != nil {
		return 0, fmt.Errorf("failed to create heap file: %w", err)
	}

	tableID = heapFile.GetID()

	// Register in persistent catalog (CATALOG_TABLES + CATALOG_COLUMNS)
	if err := cm.catalog.RegisterTable(tx, tableID, tableName, fullPath, primaryKey, fields); err != nil {
		heapFile.Close()
		return 0, fmt.Errorf("failed to register table in catalog: %w", err)
	}

	// Add to runtime TableManager cache
	if err := cm.tableManager.AddTable(heapFile, tableName, primaryKey); err != nil {
		return 0, fmt.Errorf("failed to add table to manager: %w", err)
	}

	return tableID, nil
}

// DropTable completely removes a table from both disk catalog and memory cache.
// Steps:
//  1. Removes from in-memory TableManager (closes file handle)
//  2. Deletes metadata from CATALOG_TABLES
//  3. Deletes column definitions from CATALOG_COLUMNS
//  4. Deletes statistics from CATALOG_STATISTICS
//
// Note: This does NOT delete the physical heap file itself, only the catalog metadata.
func (cm *CatalogManager) DropTable(tx *transaction.TransactionContext, tableName string) error {
	tableID, err := cm.GetTableID(tx.ID, tableName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", tableName, err)
	}

	if err := cm.tableManager.RemoveTable(tableName); err != nil {
		// Log warning but continue - table might not be loaded in memory
		fmt.Printf("Warning: failed to remove table from manager: %v\n", err)
	}

	// Delete from CATALOG_TABLES
	if err := cm.deleteCatalogEntry(tx, cm.catalog.GetTablesTableID(), 0, tableID); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_TABLES: %w", err)
	}

	// Delete from CATALOG_COLUMNS (all columns for this table)
	if err := cm.deleteCatalogEntry(tx, cm.catalog.GetColumnsTableID(), 0, tableID); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_COLUMNS: %w", err)
	}

	// Delete from CATALOG_STATISTICS if exists
	_ = cm.deleteCatalogEntry(tx, cm.catalog.GetStatisticsTableID(), 0, tableID)

	return nil
}

// deleteCatalogEntry is a helper to delete tuples from catalog tables by matching a field value
func (cm *CatalogManager) deleteCatalogEntry(tx *transaction.TransactionContext, catalogTableID, fieldIndex, matchValue int) error {
	file, err := cm.tableManager.GetDbFile(catalogTableID)
	if err != nil {
		return err
	}

	iter := file.Iterator(tx.ID)
	if err := iter.Open(); err != nil {
		return err
	}
	defer iter.Close()

	var tuplesToDelete []*tuple.Tuple
	matchValueInt32 := int32(matchValue)

	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		field, err := tup.GetField(fieldIndex)
		if err != nil {
			continue
		}

		if intField, ok := field.(*types.IntField); ok {
			if intField.Value == matchValueInt32 {
				tuplesToDelete = append(tuplesToDelete, tup)
			}
		}
	}

	for _, tup := range tuplesToDelete {
		if err := cm.store.DeleteTuple(tx, tup); err != nil {
			return err
		}
	}

	return nil
}

// GetTableID retrieves the table ID for a given table name.
// Checks in-memory TableManager first (O(1)), falls back to disk scan if not found.
func (cm *CatalogManager) GetTableID(tid *primitives.TransactionID, tableName string) (int, error) {
	if id, err := cm.tableManager.GetTableID(tableName); err == nil {
		return id, nil
	}
	return cm.catalog.GetTableID(tid, tableName)
}

// GetTableName retrieves the table name for a given table ID.
// Checks in-memory TableManager first (O(1)), falls back to disk scan if not found.
func (cm *CatalogManager) GetTableName(tid *primitives.TransactionID, tableID int) (string, error) {
	if name, err := cm.tableManager.GetTableName(tableID); err == nil {
		return name, nil
	}

	// Slow path: scan catalog on disk (requires custom implementation)
	// SystemCatalog doesn't have GetTableName, so we need to iterate
	var result string
	tableIDInt32 := int(int32(tableID))

	err := cm.catalog.iterateTable(cm.catalog.GetTablesTableID(), tid, func(tup *tuple.Tuple) error {
		storedID := getIntField(tup, 0)
		if storedID == tableIDInt32 {
			result = getStringField(tup, 1)
			return fmt.Errorf("found") // Break iteration
		}
		return nil
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("table with ID %d not found", tableID)
}

// GetTableSchema retrieves the schema (TupleDescription) for a table.
// First checks in-memory cache, then loads from disk if necessary.
func (cm *CatalogManager) GetTableSchema(tid *primitives.TransactionID, tableID int) (*tuple.TupleDescription, error) {
	if schema, err := cm.tableManager.GetTupleDesc(tableID); err == nil {
		return schema, nil
	}

	schema, _, err := cm.catalog.loader.LoadTableSchema(tid, tableID)
	return schema, err
}

// GetTableFile retrieves the DbFile for a table from the in-memory cache.
// Returns an error if the table is not loaded in memory.
func (cm *CatalogManager) GetTableFile(tableID int) (page.DbFile, error) {
	return cm.tableManager.GetDbFile(tableID)
}

// TableExists checks if a table exists by name.
// Checks memory first, then disk catalog.
func (cm *CatalogManager) TableExists(tid *primitives.TransactionID, tableName string) bool {
	// Check memory
	if cm.tableManager.TableExists(tableName) {
		return true
	}

	// Check disk
	_, err := cm.catalog.GetTableID(tid, tableName)
	return err == nil
}

// GetPrimaryKey retrieves the primary key field name for a table from disk catalog.
func (cm *CatalogManager) GetPrimaryKey(tid *primitives.TransactionID, tableID int) (string, error) {
	var primaryKey string
	tableIDInt32 := int(int32(tableID))

	err := cm.catalog.iterateTable(cm.catalog.GetTablesTableID(), tid, func(tup *tuple.Tuple) error {
		storedID := getIntField(tup, 0)
		if storedID == tableIDInt32 {
			primaryKey = getStringField(tup, 3) // primary_key is field 3
			return fmt.Errorf("found")
		}
		return nil
	})

	if err != nil && err.Error() == "found" {
		return primaryKey, nil
	}
	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("table with ID %d not found", tableID)
}

// ListAllTables returns all table names from the in-memory cache.
// For a complete list including tables not loaded in memory, use ListAllTablesFromDisk.
func (cm *CatalogManager) ListAllTables() []string {
	return cm.tableManager.GetAllTableNames()
}

// ListAllTablesFromDisk scans CATALOG_TABLES and returns all table names.
// This is slower than ListAllTables but includes tables not currently loaded in memory.
func (cm *CatalogManager) ListAllTablesFromDisk(tid *primitives.TransactionID) ([]string, error) {
	var tableNames []string

	err := cm.catalog.iterateTable(cm.catalog.GetTablesTableID(), tid, func(tup *tuple.Tuple) error {
		name := getStringField(tup, 1)
		tableNames = append(tableNames, name)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tableNames, nil
}

// UpdateTableStatistics collects statistics for a table and updates both disk and cache.
// This scans the table, computes cardinality/page count/etc., and stores in CATALOG_STATISTICS.
func (cm *CatalogManager) UpdateTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	return cm.catalog.UpdateTableStatistics(tx, tableID)
}

// GetTableStatistics retrieves statistics for a table from disk.
func (cm *CatalogManager) GetTableStatistics(tid *primitives.TransactionID, tableID int) (*TableStatistics, error) {
	return cm.catalog.GetTableStatistics(tid, tableID)
}

// RefreshStatistics updates statistics for a table and returns the updated stats.
// This is a convenience method that combines update + get.
func (cm *CatalogManager) RefreshStatistics(tx *transaction.TransactionContext, tableID int) (*TableStatistics, error) {
	if err := cm.UpdateTableStatistics(tx, tableID); err != nil {
		return nil, err
	}
	return cm.catalog.GetTableStatistics(tx.ID, tableID)
}

// RenameTable renames a table in both memory and disk catalog.
// Steps:
//  1. Renames in TableManager (in-memory)
//  2. Updates CATALOG_TABLES entry on disk
func (cm *CatalogManager) RenameTable(tx *transaction.TransactionContext, oldName, newName string) error {
	// Check if new name already exists
	if cm.TableExists(tx.ID, newName) {
		return fmt.Errorf("table %s already exists", newName)
	}

	// Get table ID first
	tableID, err := cm.GetTableID(tx.ID, oldName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", oldName, err)
	}

	// Rename in memory
	if err := cm.tableManager.RenameTable(oldName, newName); err != nil {
		return fmt.Errorf("failed to rename in memory: %w", err)
	}

	// Update disk catalog - delete old entry and insert new one
	// Get full table metadata
	var filePath, primaryKey string
	tableIDInt32 := int(int32(tableID))

	err = cm.catalog.iterateTable(cm.catalog.GetTablesTableID(), tx.ID, func(tup *tuple.Tuple) error {
		storedID := getIntField(tup, 0)
		if storedID == tableIDInt32 {
			filePath = getStringField(tup, 2)
			primaryKey = getStringField(tup, 3)
			return fmt.Errorf("found")
		}
		return nil
	})

	if err == nil || err.Error() != "found" {
		// Rollback memory change
		cm.tableManager.RenameTable(newName, oldName)
		return fmt.Errorf("failed to find table metadata: %w", err)
	}

	// Delete old entry from CATALOG_TABLES
	if err := cm.deleteCatalogEntry(tx, cm.catalog.GetTablesTableID(), 0, tableID); err != nil {
		// Rollback memory change
		cm.tableManager.RenameTable(newName, oldName)
		return fmt.Errorf("failed to delete old catalog entry: %w", err)
	}

	// Insert new entry with updated name
	tup := createTablesTuple(tableID, newName, filePath, primaryKey)
	if err := cm.store.InsertTuple(tx, cm.catalog.GetTablesTableID(), tup); err != nil {
		// Rollback everything
		cm.tableManager.RenameTable(newName, oldName)
		return fmt.Errorf("failed to insert new catalog entry: %w", err)
	}

	return nil
}

// LoadTable loads a specific table from disk into memory by name.
// Useful for lazy loading tables on demand rather than loading all tables at startup.
func (cm *CatalogManager) LoadTable(tid *primitives.TransactionID, tableName string, dataDir string) error {
	// Check if already loaded
	if cm.tableManager.TableExists(tableName) {
		return nil // Already loaded
	}

	// Find table in catalog
	var tableID int
	var filePath, primaryKey string

	err := cm.catalog.iterateTable(cm.catalog.GetTablesTableID(), tid, func(tup *tuple.Tuple) error {
		name := getStringField(tup, 1)
		if name == tableName {
			tableID = getIntField(tup, 0)
			filePath = getStringField(tup, 2)
			primaryKey = getStringField(tup, 3)
			return fmt.Errorf("found")
		}
		return nil
	})

	if err == nil || err.Error() != "found" {
		return fmt.Errorf("table %s not found in catalog", tableName)
	}

	// Load schema
	schema, pk, err := cm.catalog.loader.LoadTableSchema(tid, tableID)
	if err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	// Open heap file
	heapFile, err := heap.NewHeapFile(filePath, schema)
	if err != nil {
		return fmt.Errorf("failed to open heap file: %w", err)
	}

	// Add to TableManager
	if err := cm.tableManager.AddTable(heapFile, tableName, pk); err != nil {
		heapFile.Close()
		return fmt.Errorf("failed to add table to manager: %w", err)
	}

	// Use loaded primary key if not in catalog metadata
	if primaryKey == "" {
		primaryKey = pk
	}

	return nil
}

// UnloadTable removes a table from memory but keeps it in the disk catalog.
// This is useful for memory management with large numbers of tables.
func (cm *CatalogManager) UnloadTable(tableName string) error {
	return cm.tableManager.RemoveTable(tableName)
}

// GetSystemCatalog returns the underlying SystemCatalog for advanced operations.
// Use this sparingly - most operations should go through CatalogManager methods.
func (cm *CatalogManager) GetSystemCatalog() *SystemCatalog {
	return cm.catalog
}

// GetTableManager returns the underlying TableManager for advanced operations.
// Use this sparingly - most operations should go through CatalogManager methods.
func (cm *CatalogManager) GetTableManager() *memory.TableManager {
	return cm.tableManager
}

// ValidateIntegrity checks consistency between memory and disk catalog.
// Returns an error if any inconsistencies are found.
func (cm *CatalogManager) ValidateIntegrity(tid *primitives.TransactionID) error {
	if err := cm.tableManager.ValidateIntegrity(); err != nil {
		return fmt.Errorf("table manager integrity error: %w", err)
	}

	memoryTables := cm.tableManager.GetAllTableNames()
	for _, tableName := range memoryTables {
		if _, err := cm.catalog.GetTableID(tid, tableName); err != nil {
			return fmt.Errorf("table %s exists in memory but not in disk catalog", tableName)
		}
	}

	return nil
}
