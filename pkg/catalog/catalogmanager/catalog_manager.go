package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/storage/heap"
)

// CatalogManager manages all database metadata including tables, columns, indexes, and statistics.
// This is the unified catalog layer that handles both persistence and caching.
//
// Architecture:
//   - CatalogIO: Low-level read/write primitives
//   - TableCache: In-memory table metadata and files
//   - Operations: Domain-specific business logic (tables, columns, indexes, statistics)
//   - CatalogManager: Public API that coordinates all layers
//
// Design Philosophy:
//   - Single responsibility: Each operation class handles one domain
//   - Transactional: All mutations require transaction context
//   - Cache-first: Check memory before disk for performance
//   - Complete operations: Methods handle both disk and cache atomically
//
// The CatalogManager is organized across multiple files:
//   - catalog_manager.go: Core struct, constructor, initialization
//   - types.go: Type definitions and SystemTableIDs
//   - table_lifecycle.go: Create, Drop, Load, Rename tables
//   - table_queries.go: Get, Exists, List tables
//   - index_manager.go: Create, Drop, Get indexes
//   - statistics_collector.go: Collect, Update, Get statistics
//   - helpers.go: Internal helper methods
type CatalogManager struct {
	// Core infrastructure
	io         *catalogio.CatalogIO
	store      *memory.PageStore
	tableCache *tablecache.TableCache
	tupMgr     *table.TupleManager
	dataDir    string
	SystemTabs SystemTableIDs

	// Domain-specific operation handlers
	indexOps      *operations.IndexOperations
	colOps        *operations.ColumnOperations
	statsOps      *operations.StatsOperations
	tableOps      *operations.TableOperations
	colStatsOps   *operations.ColStatsOperations
	indexStatsOps *operations.IndexStatsOperations
}

// NewCatalogManager creates a new CatalogManager instance.
//
// The catalog must be initialized via Initialize() before use.
//
// Parameters:
//   - ps: PageStore for transaction and page management
//   - dataDir: Base directory where table and index files will be stored
//
// Returns:
//   - *CatalogManager: A new CatalogManager instance (not yet initialized)
func NewCatalogManager(ps *memory.PageStore, dataDir string) *CatalogManager {
	cache := tablecache.NewTableCache()
	io := catalogio.NewCatalogIO(ps, cache)
	return &CatalogManager{
		io:         io,
		store:      ps,
		tableCache: cache,
		dataDir:    dataDir,
		tupMgr:     table.NewTupleManager(ps),
	}
}

// Initialize creates or loads all system catalog tables and registers them with the page store.
//
// This must be called before any other catalog operations.
// Works for both new and existing databases - heap.NewHeapFile handles both creation and loading.
//
// System tables created/loaded:
//   - CATALOG_TABLES: table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: column definitions (table ID, name, type, position, is_primary)
//   - CATALOG_STATISTICS: table statistics for query optimization
//   - CATALOG_INDEXES: index metadata (ID, name, table ID, column, type, file path)
//   - CATALOG_COLUMN_STATISTICS: column-level statistics for selectivity estimation
//   - CATALOG_INDEX_STATISTICS: index statistics for query optimization
//
// The operation handlers are initialized after system tables are created.
// The transaction is committed upon successful completion.
//
// Parameters:
//   - ctx: Transaction context for all catalog initialization operations
//
// Returns error if any system table cannot be created, loaded, or registered.
func (cm *CatalogManager) Initialize(ctx TxContext) error {
	defer cm.store.CommitTransaction(ctx)

	systemTables := systemtable.AllSystemTables

	for _, table := range systemTables {
		sch := table.Schema()
		f, err := heap.NewHeapFile(
			filepath.Join(cm.dataDir, table.FileName()),
			sch.TupleDesc,
		)
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %w", table.TableName(), err)
		}

		if err := cm.tableCache.AddTable(f, sch); err != nil {
			return fmt.Errorf("failed to register %s: %w", table.TableName(), err)
		}

		cm.SystemTabs.SetSystemTableID(table.TableName(), f.GetID())
		cm.store.RegisterDbFile(f.GetID(), f)
	}

	// Initialize operation handlers after system tables are set up
	cm.indexOps = operations.NewIndexOperations(cm.io, cm.SystemTabs.IndexesTableID)
	cm.colOps = operations.NewColumnOperations(cm.io, cm.SystemTabs.ColumnsTableID)
	cm.statsOps = operations.NewStatsOperations(cm.io, cm.SystemTabs.StatisticsTableID, cm.SystemTabs.ColumnsTableID, cm.tableCache.GetDbFile, cm.tableCache)
	cm.tableOps = operations.NewTableOperations(cm.io, cm.SystemTabs.TablesTableID)
	cm.colStatsOps = operations.NewColStatsOperations(cm.io, cm.SystemTabs.ColumnStatisticsTableID, cm.tableCache.GetDbFile, cm.colOps)
	cm.indexStatsOps = operations.NewIndexStatsOperations(cm.io, cm.SystemTabs.IndexStatisticsTableID, cm.SystemTabs.IndexesTableID, cm.tableCache.GetDbFile, cm.statsOps)

	return nil
}
