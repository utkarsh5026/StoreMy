package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/catalogio"
	ops "storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/storage/heap"
	"sync"
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
// File Ownership:
//   - CatalogManager owns all table file life-cycles (open/close)
//   - PageStore only performs I/O operations via PageIO interface
//   - openFiles map tracks owned files for proper cleanup
type CatalogManager struct {
	// Core infrastructure
	io         *catalogio.CatalogIO
	store      *memory.PageStore
	tableCache *tablecache.TableCache
	tupMgr     *table.TupleManager
	dataDir    string
	SystemTabs SystemTableIDs

	// File ownership - tracks all open table files for lifecycle management
	mu        sync.RWMutex // protects openFiles and concurrent operations
	openFiles map[int]*heap.HeapFile

	// Domain-specific operation handlers
	indexOps      *ops.IndexOperations
	colOps        *ops.ColumnOperations
	statsOps      *ops.StatsOperations
	tableOps      *ops.TableOperations
	colStatsOps   *ops.ColStatsOperations
	indexStatsOps *ops.IndexStatsOperations
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
		openFiles:  make(map[int]*heap.HeapFile),
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
		cm.mu.Lock()
		cm.openFiles[f.GetID()] = f
		cm.mu.Unlock()
		cm.store.RegisterDbFile(f.GetID(), f)
	}

	cm.setupSysTables()
	return nil
}

// setupSysTables initializes all domain-specific operation handlers with their respective system table IDs.
//
// This method is called during Initialize() after all system catalog tables have been created/loaded.
// It wires up the operation handlers that provide high-level business logic for catalog operations.
//
// Operation handlers initialized:
//   - indexOps: Manages index metadata in CATALOG_INDEXES
//   - colOps: Manages column definitions in CATALOG_COLUMNS
//   - statsOps: Manages table statistics in CATALOG_STATISTICS
//   - tableOps: Manages table metadata in CATALOG_TABLES
//   - colStatsOps: Manages column statistics in CATALOG_COLUMN_STATISTICS
//   - indexStatsOps: Manages index statistics in CATALOG_INDEX_STATISTICS
//
// Dependencies:
//   - All handlers depend on CatalogIO for low-level read/write operations
//   - Stats handlers require access to DbFile instances via tableCache.GetDbFile
//   - Index stats depends on statsOps for table-level statistics
//   - Column stats depends on colOps for column metadata
//
// This separation allows each operation handler to focus on a single domain
// while sharing common infrastructure through CatalogIO and TableCache.
func (cm *CatalogManager) setupSysTables() {
	cm.indexOps = ops.NewIndexOperations(cm.io, cm.SystemTabs.IndexesTableID)
	cm.colOps = ops.NewColumnOperations(cm.io, cm.SystemTabs.ColumnsTableID)
	cm.statsOps = ops.NewStatsOperations(cm.io, cm.SystemTabs.StatisticsTableID, cm.SystemTabs.ColumnsTableID, cm.tableCache.GetDbFile, cm.tableCache)
	cm.tableOps = ops.NewTableOperations(cm.io, cm.SystemTabs.TablesTableID)
	cm.colStatsOps = ops.NewColStatsOperations(cm.io, cm.SystemTabs.ColumnStatisticsTableID, cm.tableCache.GetDbFile, cm.colOps)
	cm.indexStatsOps = ops.NewIndexStatsOperations(cm.io, cm.SystemTabs.IndexStatisticsTableID, cm.SystemTabs.IndexesTableID, cm.tableCache.GetDbFile, cm.statsOps)
}
