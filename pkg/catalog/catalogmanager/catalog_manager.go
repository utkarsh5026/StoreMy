package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"sync"
)

type SysTables struct {
	tableIdsMap map[string]primitives.FileID

	IndexTable        *systable.IndexesTable
	ColumnTable       *systable.ColumnsTable
	StatsTable        *systable.StatsTable
	TablesTable       *systable.TablesTable
	ColumnStatsTable  *systable.ColumnStatsTable
	IndexesStatsTable *systable.IndexStatsTable
}

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

	// File ownership - tracks all open table files for lifecycle management
	mu        sync.RWMutex // protects openFiles and concurrent operations
	openFiles map[primitives.FileID]*heap.HeapFile

	// Domain-specific operation handlers
	SysTables
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
		openFiles:  make(map[primitives.FileID]*heap.HeapFile),
		SysTables: SysTables{
			tableIdsMap: make(map[string]primitives.FileID),
		},
	}
}

// Initialize creates or loads all system catalog tables and registers them with the page store.
//
// This must be called before any other catalog operations.
// Works for both new and existing databases - heap.NewHeapFile handles both creation and loading.
//
// The operation handlers are initialized after system tables are created.
// The transaction is committed upon successful completion.
//
// Parameters:
//   - ctx: Transaction context for all catalog initialization operations
//
// Returns error if any system table cannot be created, loaded, or registered.
func (cm *CatalogManager) Initialize(ctx *transaction.TransactionContext) error {
	defer func() { _ = cm.store.CommitTransaction(ctx) }()

	systemTables := systable.AllSystemTables

	for _, table := range systemTables {
		sch := table.Schema()

		path := primitives.Filepath(filepath.Join(cm.dataDir, table.FileName()))
		f, err := heap.NewHeapFile(path, sch.TupleDesc)
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %w", table.TableName(), err)
		}

		if err := cm.tableCache.AddTable(f, sch); err != nil {
			return fmt.Errorf("failed to register %s: %w", table.TableName(), err)
		}

		cm.tableIdsMap[table.TableName()] = f.GetID()
		cm.mu.Lock()
		cm.openFiles[f.GetID()] = f
		cm.mu.Unlock()
		cm.store.RegisterDbFile(f.GetID(), f)
	}

	cm.setupSysTables()
	return nil
}

// setupSysTables initializes all domain-specific operation handlers with their respective system table IDs.
func (cm *CatalogManager) setupSysTables() {
	var systables SysTables
	systables.IndexTable = systable.NewIndexOperations(cm.io, cm.tableIdsMap[systable.IndexesTableDescriptor.TableName()])

	systables.TablesTable = systable.NewTablesTable(cm.io, cm.tableIdsMap[systable.TablesTableDescriptor.TableName()])

	systables.ColumnTable = systable.NewColumnOperations(cm.io,
		cm.tableIdsMap[systable.ColumnsTableDescriptor.TableName()])

	systables.ColumnStatsTable = systable.NewColStatsOperations(cm.io, cm.tableIdsMap[systable.ColumnStatisticsTableDescriptor.TableName()], cm.tableCache.GetDbFile, systables.ColumnTable)

	systables.StatsTable = systable.NewStatsOperations(cm.io, cm.tableIdsMap[systable.CatalogStatisticsTableDescriptor.TableName()], cm.tableCache.GetDbFile, nil, systables.ColumnTable)

	systables.IndexesStatsTable = systable.NewIndexStatsOperations(cm.io, cm.tableIdsMap[systable.IndexStatisticsTableDescriptor.TableName()], cm.tableIdsMap[systable.IndexesTableDescriptor.TableName()], cm.tableCache.GetDbFile, systables.StatsTable, systables.IndexTable)

	systables.tableIdsMap = cm.tableIdsMap
	cm.SysTables = systables
}

func (cm *CatalogManager) getSystemTable(tableName string) (systable.SystemTable, error) {
	switch tableName {
	case systable.TablesTableDescriptor.TableName():
		return &systable.TablesTableDescriptor, nil

	case systable.ColumnsTableDescriptor.TableName():
		return &systable.ColumnsTableDescriptor, nil

	case systable.CatalogStatisticsTableDescriptor.TableName():
		return &systable.CatalogStatisticsTableDescriptor, nil

	case systable.ColumnStatisticsTableDescriptor.TableName():
		return &systable.ColumnStatisticsTableDescriptor, nil

	case systable.IndexesTableDescriptor.TableName():
		return &systable.IndexesTableDescriptor, nil

	case systable.IndexStatisticsTableDescriptor.TableName():
		return &systable.IndexStatisticsTableDescriptor, nil

	default:
		return nil, fmt.Errorf("unknown system table: %s", tableName)
	}
}

// getSystemTableByID looks up a system table descriptor by its FileID.
// Used to reverse-map a known system table ID back to its descriptor.
func (cm *CatalogManager) getSystemTableByID(tableID primitives.FileID) (systable.SystemTable, error) {
	for name, id := range cm.tableIdsMap {
		if id == tableID {
			return cm.getSystemTable(name)
		}
	}
	return nil, fmt.Errorf("system table with ID %d not found", tableID)
}
