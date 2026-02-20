package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/scanner"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
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

// RegisterTable adds a new user table to the system catalog.
// It inserts metadata into CATALOG_TABLES and column definitions into CATALOG_COLUMNS.
//
// Note: Primary key indexes should be created separately by the DDL layer to ensure
// both catalog metadata and physical index files are created together.
//
// This is an internal helper used by CreateTable.
func (cm *CatalogManager) registerTable(tx *transaction.TransactionContext, sch *schema.Schema, filepath primitives.Filepath) error {
	tm := systable.TableMetadata{
		TableName:     sch.TableName,
		TableID:       sch.TableID,
		FilePath:      filepath,
		PrimaryKeyCol: sch.PrimaryKey,
	}
	if err := cm.TablesTable.Insert(tx, tm); err != nil {
		return err
	}

	if err := cm.ColumnTable.InsertColumns(tx, sch.Columns); err != nil {
		return err
	}

	return nil
}

// DeleteCatalogEntry removes all catalog metadata for a table.
// This includes entries in CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, and CATALOG_INDEXES.
//
// This is typically called as part of a DROP TABLE operation.
// Note: This only removes catalog entries - the heap file must be deleted separately.
func (cm *CatalogManager) DeleteCatalogEntry(tx *transaction.TransactionContext, tableID primitives.FileID) error {
	if err := cm.TablesTable.DeleteTable(tx, tableID); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_TABLES: %w", err)
	}

	if err := cm.ColumnTable.DeleteBy(tx, func(c schema.ColumnMetadata) bool {
		return c.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_COLUMNS: %w", err)
	}

	if err := cm.StatsTable.DeleteBy(tx, func(s systable.TableStatistics) bool {
		return s.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_STATISTICS: %w", err)
	}

	if err := cm.IndexTable.DeleteBy(tx, func(i systable.IndexMetadata) bool {
		return i.TableID == tableID
	}); err != nil {
		return fmt.Errorf("failed to delete from CATALOG_INDEXES: %w", err)
	}

	return nil
}

// DeleteTableFromSysTable removes all entries for a specific table from a given system table.
// Due to MVCC, there may be multiple versions of tuples for the same table - this deletes all.
func (cm *CatalogManager) DeleteTableFromSysTable(tx *transaction.TransactionContext, tableID, sysTableID primitives.FileID) error {
	tableInfo, err := cm.tableCache.GetTableInfo(sysTableID)
	if err != nil {
		return err
	}

	syst, err := cm.getSystemTableByID(sysTableID)
	if err != nil {
		return nil
	}

	var tuplesToDelete []*tuple.Tuple

	// Iterate through the system table to find all tuples matching the tableID
	err = cm.iterateTable(sysTableID, tx, func(t *tuple.Tuple) error {
		field, err := t.GetField(primitives.ColumnID(syst.TableIDIndex())) // #nosec G115
		if err != nil {
			return err
		}

		// All system tables use Uint64Field for table_id
		if uint64Field, ok := field.(*types.Uint64Field); ok {
			if uint64Field.Value == uint64(tableID) {
				tuplesToDelete = append(tuplesToDelete, t)
			}
		}

		return nil
	})

	// Check if iteration failed
	if err != nil {
		return fmt.Errorf("failed to iterate system table %d: %w", sysTableID, err)
	}

	// Delete all matching tuples
	for _, tup := range tuplesToDelete {
		if err := cm.tupMgr.DeleteTuple(tx, tableInfo.File, tup); err != nil {
			return err
		}
	}
	return nil
}

// LoadTableSchema reconstructs the complete schema for a table from CATALOG_COLUMNS.
// This includes column definitions, types, and constraints.
func (cm *CatalogManager) LoadTableSchema(tx *transaction.TransactionContext, tableID primitives.FileID) (*schema.Schema, error) {
	tm, err := cm.TablesTable.GetByID(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	columns, err := cm.ColumnTable.LoadColumnMetadata(tx, tableID)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %d", tableID)
	}

	sch, err := schema.NewSchema(tableID, tm.TableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return sch, nil
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is the core primitive for catalog queries, handling MVCC visibility and locking.
func (cm *CatalogManager) iterateTable(tableID primitives.FileID, tx *transaction.TransactionContext, processFunc func(*tuple.Tuple) error) error {
	file, err := cm.tableCache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	heapFile := file.(*heap.HeapFile)

	iter, err := scanner.NewSeqScan(tx, tableID, heapFile, cm.store)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}

	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}
