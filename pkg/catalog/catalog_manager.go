package catalog

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
)

// CatalogManager coordinates between the persistent SystemCatalog and the in-memory tableCache.
// It provides high-level, complete operations that handle both disk persistence and in-memory caching,
// eliminating the need to manage both components separately throughout the codebase.
//
// Design Philosophy:
//   - Complete operations: Each method handles BOTH disk and cache consistently
//   - Clear semantics: Method names clearly indicate what happens (Create/Drop/Load)
//   - Transaction aware: All mutations require explicit transaction context
//   - Cache-first reads: Performance optimization by checking memory before disk
//   - Encapsulation: tableCache is private - all access must go through CatalogManager
type CatalogManager struct {
	catalog    *SystemCatalog
	tableCache *tableCache
	store      PageStoreInterface
	dataDir    string
}

// PageStoreInterface defines the minimal interface that CatalogManager needs from PageStore
// This avoids circular dependencies and tight coupling
type PageStoreInterface interface {
	DeleteTuple(tx *transaction.TransactionContext, dbFile page.DbFile, tup *tuple.Tuple) error
	InsertTuple(tx *transaction.TransactionContext, dbFile page.DbFile, tup *tuple.Tuple) error
	CommitTransaction(tx *transaction.TransactionContext) error
	RegisterDbFile(tableID int, dbFile page.DbFile)
	UnregisterDbFile(tableID int)
}

// NewCatalogManager creates a new CatalogManager instance.
// dataDir is the base directory where table files will be stored.
func NewCatalogManager(ps PageStoreInterface, dataDir string) *CatalogManager {
	cache := newTableCache()
	return &CatalogManager{
		catalog:    NewSystemCatalog(ps, cache),
		tableCache: cache,
		store:      ps,
		dataDir:    dataDir,
	}
}

// Initialize initializes the system catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS).
// This must be called once during database initialization before any other operations.
func (cm *CatalogManager) Initialize(ctx *transaction.TransactionContext) error {
	return cm.catalog.Initialize(ctx, cm.dataDir)
}

// LoadAllTables loads all user tables from disk into memory during database startup.
// This reads CATALOG_TABLES, reconstructs schemas from CATALOG_COLUMNS,
// opens heap files, and registers everything with the TableManager.
func (cm *CatalogManager) LoadAllTables(tx *transaction.TransactionContext) error {
	return cm.catalog.LoadTables(tx, cm.dataDir)
}

// CreateTable creates a new table with complete disk + cache registration.
// Steps:
//  1. Creates a new heap file on disk (path determined by CatalogManager)
//  2. Registers table metadata in CATALOG_TABLES
//  3. Registers column definitions in CATALOG_COLUMNS
//  4. Adds table to in-memory TableManager
//
// This is a complete, atomic operation - the table is fully usable after this call.
// The schema parameter should have TableID set to 0; it will be updated with the actual file ID.
func (cm *CatalogManager) CreateTable(
	tx *transaction.TransactionContext,
	tableSchema *schema.Schema,
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

	if err := cm.catalog.RegisterTable(tx, tableSchema, fullPath); err != nil {
		heapFile.Close()
		return 0, fmt.Errorf("failed to register table in catalog: %w", err)
	}

	if err := cm.tableCache.AddTable(heapFile, tableSchema); err != nil {
		return 0, fmt.Errorf("failed to add table to cache: %w", err)
	}

	// Register the DbFile with the PageStore for flush operations
	cm.store.RegisterDbFile(tableID, heapFile)

	return tableID, nil
}

// DropTable completely removes a table from both disk catalog and memory cache.
func (cm *CatalogManager) DropTable(tx *transaction.TransactionContext, tableName string) error {
	tableID, err := cm.GetTableID(tx.ID, tableName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", tableName, err)
	}

	if err := cm.tableCache.removeTable(tableName); err != nil {
		fmt.Printf("Warning: failed to remove table from cache: %v\n", err)
	}

	// Unregister the DbFile from PageStore
	cm.store.UnregisterDbFile(tableID)

	return cm.catalog.DeleteCatalogEntry(tx, tableID)
}

// GetTableID retrieves the table ID for a given table name.
// Checks in-memory cache first (O(1)), falls back to disk scan if not found.
func (cm *CatalogManager) GetTableID(tid *primitives.TransactionID, tableName string) (int, error) {
	if id, err := cm.tableCache.getTableID(tableName); err == nil {
		return id, nil
	}

	if md, err := cm.catalog.GetTableMetadataByName(tid, tableName); err == nil {
		return md.TableID, nil
	}

	return -1, fmt.Errorf("table %s not found", tableName)
}

// GetTableName retrieves the table name for a given table ID.
// Checks in-memory cache first (O(1)), falls back to disk scan if not found.
func (cm *CatalogManager) GetTableName(tid *primitives.TransactionID, tableID int) (string, error) {
	if info, err := cm.tableCache.GetTableInfo(tableID); err == nil {
		return info.Schema.TableName, nil
	}

	if md, err := cm.catalog.GetTableMetadataByID(tid, tableID); err == nil {
		return md.TableName, nil
	}

	return "", fmt.Errorf("table with ID %d not found", tableID)
}

// GetTableSchema retrieves the schema for a table.
// First checks in-memory cache, then loads from disk if necessary.
func (cm *CatalogManager) GetTableSchema(tid *primitives.TransactionID, tableID int) (*schema.Schema, error) {
	if info, err := cm.tableCache.GetTableInfo(tableID); err == nil {
		return info.Schema, nil
	}

	schema, err := cm.catalog.loader.LoadTableSchema(tid, tableID)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// GetTableFile retrieves the DbFile for a table from the in-memory cache.
// Returns an error if the table is not loaded in memory.
func (cm *CatalogManager) GetTableFile(tableID int) (page.DbFile, error) {
	return cm.tableCache.GetDbFile(tableID)
}

// TableExists checks if a table exists by name.
// Checks memory first, then disk catalog.
func (cm *CatalogManager) TableExists(tid *primitives.TransactionID, tableName string) bool {
	if cm.tableCache.TableExists(tableName) {
		return true
	}
	_, err := cm.catalog.GetTableMetadataByName(tid, tableName)
	return err == nil
}

// ListAllTablesFromDisk scans CATALOG_TABLES and returns all table names.
// This is slower than ListAllTables but includes tables not currently loaded in memory.
func (cm *CatalogManager) ListAllTables(tid *primitives.TransactionID, refreshFromDisk bool) ([]string, error) {
	if !refreshFromDisk {
		return cm.tableCache.GetAllTableNames(), nil
	}

	tables, err := cm.catalog.GetAllTables(tid)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.TableName)
	}

	return tableNames, nil
}

// UpdateTableStatistics collects statistics for a table and updates both disk and cache.
// This scans the table, computes cardinality/page count/etc., and stores in CATALOG_STATISTICS.
func (cm *CatalogManager) UpdateTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	return cm.catalog.UpdateTableStatistics(tx, tableID)
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
	if cm.TableExists(tx.ID, newName) {
		return fmt.Errorf("table %s already exists", newName)
	}

	tableID, err := cm.GetTableID(tx.ID, oldName)
	if err != nil {
		return fmt.Errorf("table %s not found: %w", oldName, err)
	}

	if err := cm.tableCache.RenameTable(oldName, newName); err != nil {
		return fmt.Errorf("failed to rename in memory: %w", err)
	}

	tm, err := cm.catalog.GetTableMetadataByID(tx.ID, tableID)
	if err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to find table metadata: %w", err)
	}

	if err := cm.catalog.DeleteTableFromSysTable(tx, tableID, cm.catalog.TablesTableID); err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to delete old catalog entry: %w", err)
	}

	tup := systemtable.Tables.CreateTuple(*tm)

	// Get the DbFile for the tables catalog
	tablesFile, err := cm.tableCache.GetDbFile(cm.catalog.TablesTableID)
	if err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to get tables catalog file: %w", err)
	}

	if err := cm.store.InsertTuple(tx, tablesFile, tup); err != nil {
		cm.tableCache.RenameTable(newName, oldName)
		return fmt.Errorf("failed to insert new catalog entry: %w", err)
	}

	return nil
}

// LoadTable loads a specific table from disk into memory by name.
// Useful for lazy loading tables on demand rather than loading all tables at startup.
func (cm *CatalogManager) LoadTable(tid *primitives.TransactionID, tableName string) error {
	if cm.tableCache.TableExists(tableName) {
		return nil
	}

	tm, err := cm.catalog.GetTableMetadataByName(tid, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	sch, err := cm.catalog.loader.LoadTableSchema(tid, tm.TableID)
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

	// Register the DbFile with the PageStore for flush operations
	cm.store.RegisterDbFile(tm.TableID, heapFile)

	return nil
}

// ValidateIntegrity checks consistency between memory and disk catalog.
// Returns an error if any inconsistencies are found.
func (cm *CatalogManager) ValidateIntegrity(tid *primitives.TransactionID) error {
	if err := cm.tableCache.validateIntegrity(); err != nil {
		return fmt.Errorf("cache integrity error: %w", err)
	}

	names := cm.tableCache.GetAllTableNames()
	for _, n := range names {
		if _, err := cm.catalog.GetTableMetadataByName(tid, n); err != nil {
			return fmt.Errorf("table %s exists in memory but not in disk catalog", n)
		}
	}
	return nil
}

// ClearCache removes all tables from memory cache (useful for shutdown)
func (cm *CatalogManager) ClearCache() {
	cm.tableCache.clear()
}

// GetAutoIncrementColumn retrieves auto-increment column information for a table.
// Returns nil if the table has no auto-increment column.
func (cm *CatalogManager) GetAutoIncrementColumn(tid *primitives.TransactionID, tableID int) (*AutoIncrementInfo, error) {
	return cm.catalog.GetAutoIncrementColumn(tid, tableID)
}

// IncrementAutoIncrementValue updates the next auto-increment value for a table's auto-increment column.
func (cm *CatalogManager) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID int, columnName string, newValue int) error {
	return cm.catalog.IncrementAutoIncrementValue(tx, tableID, columnName, newValue)
}

// GetTableStatisticsWithCache retrieves statistics, using cache when available.
// Falls back to disk if not cached, then caches the result.
func (cm *CatalogManager) GetTableStatistics(tid *primitives.TransactionID, tableID int) (*TableStatistics, error) {
	if stats, found := cm.tableCache.getCachedStatistics(tableID); found {
		return stats, nil
	}

	stats, err := cm.catalog.GetTableStatistics(tid, tableID)
	if err != nil {
		return nil, err
	}

	_ = cm.tableCache.setCachedStatistics(tableID, stats)
	return stats, nil
}
