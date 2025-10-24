package catalog

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/page"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type (
	TxContext       = *transaction.TransactionContext
	Tuple           = *tuple.Tuple
	IndexStatistics = systemtable.IndexStatisticsRow
)

// SystemCatalog manages database metadata including table and column definitions.
// It maintains six system tables:
//   - CATALOG_TABLES: stores table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: stores column metadata (table ID, name, type, position, is_primary)
//   - CATALOG_STATISTICS: stores table statistics for query optimization
//   - CATALOG_INDEXES: stores index metadata (ID, name, table ID, column, type, file path)
//   - CATALOG_COLUMN_STATISTICS: stores column-level statistics for selectivity estimation
//   - CATALOG_INDEX_STATISTICS: stores index statistics for query optimization
type SystemCatalog struct {
	// I/O layer (handles all read/write operations)
	io         *catalogio.CatalogIO
	store      *memory.PageStore
	cache      *tablecache.TableCache
	tupMgr     *table.TupleManager
	SystemTabs SystemTableIDs

	// Domain-specific operation handlers (injected with interfaces)
	indexOps      *operations.IndexOperations
	colOps        *operations.ColumnOperations
	statsOps      *operations.StatsOperations
	tableOps      *operations.TableOperations
	colStatsOps   *operations.ColStatsOperations
	indexStatsOps *operations.IndexStatsOperations
}

// NewSystemCatalog creates a new system catalog instance.
// The catalog must be initialized via Initialize() before use.
func NewSystemCatalog(ps *memory.PageStore, cache *tablecache.TableCache) *SystemCatalog {
	io := catalogio.NewCatalogIO(ps, cache)
	sc := &SystemCatalog{
		io:     io,
		store:  ps,
		cache:  cache,
		tupMgr: table.NewTupleManager(ps),
	}
	return sc
}

// Initialize creates or loads all system catalog tables and registers them with the table manager.
// This must be called before any other catalog operations.
// Works for both new and existing databases - heap.NewHeapFile handles both creation and loading.
//
// Parameters:
//   - ctx: Transaction context for all catalog initialization operations
//   - dataDir: Directory path where system catalog heap files will be stored/loaded
//
// Returns an error if any system table cannot be created, loaded, or registered.
// On success, the catalog is ready for use and the transaction is committed.
func (sc *SystemCatalog) Initialize(ctx TxContext, dataDir string) error {
	defer sc.store.CommitTransaction(ctx)

	systemTables := systemtable.AllSystemTables

	for _, table := range systemTables {
		sch := table.Schema()
		f, err := heap.NewHeapFile(
			filepath.Join(dataDir, table.FileName()),
			sch.TupleDesc,
		)
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %w", table.TableName(), err)
		}

		if err := sc.cache.AddTable(f, sch); err != nil {
			return fmt.Errorf("failed to register %s: %w", table.TableName(), err)
		}

		sc.SystemTabs.SetSystemTableID(table.TableName(), f.GetID())
		sc.store.RegisterDbFile(f.GetID(), f)
	}

	sc.indexOps = operations.NewIndexOperations(sc.io, sc.SystemTabs.IndexesTableID)
	sc.colOps = operations.NewColumnOperations(sc.io, sc.SystemTabs.ColumnsTableID)
	sc.statsOps = operations.NewStatsOperations(sc.io, sc.SystemTabs.StatisticsTableID, sc.cache.GetDbFile, sc.cache)
	sc.tableOps = operations.NewTableOperations(sc.io, sc.SystemTabs.TablesTableID)
	sc.colStatsOps = operations.NewColStatsOperations(sc.io, sc.SystemTabs.ColumnStatisticsTableID, sc.cache.GetDbFile, sc.colOps)
	sc.indexStatsOps = operations.NewIndexStatsOperations(sc.io, sc.SystemTabs.IndexStatisticsTableID, sc.SystemTabs.IndexesTableID, sc.cache.GetDbFile, sc.statsOps)
	return nil
}

// RegisterTable adds a new user table to the system catalog.
// It inserts metadata into CATALOG_TABLES, column definitions into CATALOG_COLUMNS,
// and initializes statistics in CATALOG_STATISTICS.
//
// This operation is transactional - all catalog updates occur within the provided transaction.
//
// Parameters:
//   - tx: Transaction context for catalog updates
//   - sch: Schema definition containing table name, columns, primary key, and table ID
//   - filepath: Path to the heap file for this table
//
// Returns an error if table or column metadata cannot be inserted into the catalog.
func (sc *SystemCatalog) RegisterTable(tx TxContext, sch *schema.Schema, filepath string) error {
	tablesFile, err := sc.cache.GetDbFile(sc.SystemTabs.TablesTableID)
	if err != nil {
		return err
	}

	tm := systemtable.TableMetadata{
		TableName:     sch.TableName,
		TableID:       sch.TableID,
		FilePath:      filepath,
		PrimaryKeyCol: sch.PrimaryKey,
	}
	tup := systemtable.Tables.CreateTuple(tm)
	if err := sc.tupMgr.InsertTuple(tx, tablesFile, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	colFile, err := sc.cache.GetDbFile(sc.SystemTabs.ColumnsTableID)
	if err != nil {
		return err
	}

	for _, col := range sch.Columns {
		tup = systemtable.Columns.CreateTuple(col)
		if err := sc.tupMgr.InsertTuple(tx, colFile, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

// LoadTables reads all table metadata from CATALOG_TABLES, reconstructs their schemas
// from CATALOG_COLUMNS, opens their heap files, and registers them with the table manager.
//
// This is called during database startup to restore all user tables into memory.
// The transaction is committed after all tables are successfully loaded.
//
// Parameters:
//   - tx: Transaction context for reading catalog metadata
//   - dataDir: Base directory where table heap files are stored
//
// Returns an error if any table cannot be loaded. The database may be in a partial state
// if this fails, requiring recovery or manual intervention.
func (sc *SystemCatalog) LoadTables(tx TxContext, dataDir string) error {
	defer sc.store.CommitTransaction(tx)

	return sc.iterateTable(sc.SystemTabs.TablesTableID, tx, func(tableTuple *tuple.Tuple) error {
		table, err := systemtable.Tables.Parse(tableTuple)
		if err != nil {
			return err
		}

		if err := sc.LoadTable(tx, table.TableName); err != nil {
			return fmt.Errorf("error loading the table %s: %v", table.TableName, err)
		}

		return nil
	})
}

// LoadTable loads a single table into the cache by reconstructing its schema from the catalog,
// opening its heap file, and registering it with the page store.
//
// This is used both during startup (via LoadTables) and for on-demand table loading.
//
// Parameters:
//   - tid: Transaction ID for reading catalog metadata
//   - tableName: Name of the table to load
//
// Returns an error if the table metadata cannot be retrieved, schema cannot be loaded,
// heap file cannot be opened, or registration fails.
func (sc *SystemCatalog) LoadTable(tx TxContext, tableName string) error {
	tm, err := sc.GetTableMetadataByName(tx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}

	sch, err := sc.LoadTableSchema(tx, tm.TableID, tm.TableName)
	if err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	heapFile, err := heap.NewHeapFile(tm.FilePath, sch.TupleDesc)
	if err != nil {
		return fmt.Errorf("failed to open heap file: %w", err)
	}

	if err := sc.cache.AddTable(heapFile, sch); err != nil {
		heapFile.Close()
		return fmt.Errorf("failed to add table to cache: %w", err)
	}

	sc.store.RegisterDbFile(tm.TableID, heapFile)
	return nil
}

// DeleteCatalogEntry removes all catalog metadata for a table, including entries in
// CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, and CATALOG_INDEXES.
//
// This is typically called as part of a DROP TABLE operation. Note that this only removes
// catalog entries - the heap file itself must be deleted separately.
//
// Parameters:
//   - tx: Transaction context for catalog deletions
//   - tableID: ID of the table whose catalog entries should be removed
//
// Returns an error if any system table deletion fails.
func (sc *SystemCatalog) DeleteCatalogEntry(tx TxContext, tableID int) error {
	sysTableIDs := []int{sc.SystemTabs.TablesTableID, sc.SystemTabs.ColumnsTableID, sc.SystemTabs.StatisticsTableID, sc.SystemTabs.IndexesTableID}
	for _, id := range sysTableIDs {
		if err := sc.DeleteTableFromSysTable(tx, tableID, id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTableFromSysTable removes all entries for a specific table from a given system table.
// This is a helper function used by DeleteCatalogEntry to clean up catalog metadata.
//
// Due to MVCC, there may be multiple versions of tuples for the same table. This function
// deletes all matching tuples.
//
// Parameters:
//   - tx: Transaction context for deletions
//   - tableID: ID of the table to remove entries for
//   - sysTableID: ID of the system table to delete from (CATALOG_TABLES, CATALOG_COLUMNS, or CATALOG_STATISTICS)
//
// Returns an error if tuples cannot be deleted from the system table.
func (sc *SystemCatalog) DeleteTableFromSysTable(tx TxContext, tableID, sysTableID int) error {
	tableInfo, err := sc.cache.GetTableInfo(sysTableID)
	if err != nil {
		return err
	}

	syst, err := sc.SystemTabs.GetSysTable(sysTableID)
	if err != nil {
		return nil
	}
	var tuplesToDelete []*tuple.Tuple

	sc.iterateTable(sysTableID, tx, func(t *tuple.Tuple) error {
		field, err := t.GetField(syst.TableIDIndex())
		if err != nil {
			return err
		}

		if intField, ok := field.(*types.IntField); ok {
			if intField.Value == int64(tableID) {
				tuplesToDelete = append(tuplesToDelete, t)
			}
		}

		return nil
	})

	for _, tup := range tuplesToDelete {
		if err := sc.tupMgr.DeleteTuple(tx, tableInfo.File, tup); err != nil {
			return err
		}
	}
	return nil
}

// GetTableMetadataByID retrieves complete table metadata from CATALOG_TABLES by table ID.
// Returns TableMetadata containing table name, file path, and primary key column,
// or an error if the table is not found.
func (sc *SystemCatalog) GetTableMetadataByID(tx TxContext, tableID int) (*systemtable.TableMetadata, error) {
	return sc.tableOps.GetTableMetadataByID(tx, tableID)
}

// GetTableMetadataByName retrieves complete table metadata from CATALOG_TABLES by table name.
// Table name matching is case-insensitive.
// Returns TableMetadata containing table ID, file path, and primary key column,
// or an error if the table is not found.
func (sc *SystemCatalog) GetTableMetadataByName(tx TxContext, tableName string) (*systemtable.TableMetadata, error) {
	return sc.tableOps.GetTableMetadataByName(tx, tableName)
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is the core primitive for catalog queries, used internally by methods like
// LoadTables(), GetTableMetadataByName(), and GetAutoIncrementColumn().
//
// The iterator follows proper concurrency protocols - all page accesses go through
// the lock manager using the provided transaction ID.
//
// Parameters:
//   - tableID: ID of the table to scan
//   - tid: Transaction ID for locking and MVCC visibility
//   - processFunc: Function to apply to each tuple. Return an error to stop iteration early.
//
// Returns an error if the iterator cannot be opened or if processFunc returns an error.
func (sc *SystemCatalog) iterateTable(tableID int, tx TxContext, processFunc func(*tuple.Tuple) error) error {
	file, err := sc.cache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	heapFile := file.(*heap.HeapFile)

	iter, err := query.NewSeqScan(tx, tableID, heapFile, sc.store)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}

	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}

// GetAllTables retrieves metadata for all tables registered in the catalog.
// This includes both user tables and system catalog tables.
//
// Used by commands like SHOW TABLES and for query planning operations that need
// to enumerate available tables.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//
// Returns a slice of TableMetadata for all tables, or an error if the catalog cannot be read.
func (sc *SystemCatalog) GetAllTables(tx TxContext) ([]*systemtable.TableMetadata, error) {
	return sc.tableOps.GetAllTables(tx)
}

// IterateTable implements CatalogReader interface by delegating to CatalogIO.
// Scans all tuples in a table and applies a processing function to each.
func (sc *SystemCatalog) IterateTable(tableID int, tx TxContext, processFunc func(Tuple) error) error {
	return sc.io.IterateTable(tableID, tx, processFunc)
}

// GetTableFile retrieves the DbFile for a table from the cache.
// Retrieves the DbFile for a table from the cache.
func (sc *SystemCatalog) GetTableFile(tableID int) (page.DbFile, error) {
	return sc.cache.GetDbFile(tableID)
}

// InsertRow implements CatalogWriter interface by delegating to CatalogIO.
// Inserts a tuple into a table within a transaction.
func (sc *SystemCatalog) InsertRow(tableID int, tx TxContext, tup Tuple) error {
	return sc.io.InsertRow(tableID, tx, tup)
}

// DeleteRow implements CatalogWriter interface by delegating to CatalogIO.
// Deletes a tuple from a table within a transaction.
func (sc *SystemCatalog) DeleteRow(tableID int, tx TxContext, tup Tuple) error {
	return sc.io.DeleteRow(tableID, tx, tup)
}

//==========================Index Operarions========================================

// GetIndexesByTable retrieves all indexes for a given table from CATALOG_INDEXES.
func (sc *SystemCatalog) GetIndexesByTable(tx TxContext, tableID int) ([]*systemtable.IndexMetadata, error) {
	return sc.indexOps.GetIndexesByTable(tx, tableID)
}

// GetIndexByName retrieves index metadata from CATALOG_INDEXES by index name.
// Index name matching is case-insensitive.
func (sc *SystemCatalog) GetIndexByName(tx TxContext, indexName string) (*systemtable.IndexMetadata, error) {
	return sc.indexOps.GetIndexByName(tx, indexName)
}

// GetIndexByID retrieves index metadata from CATALOG_INDEXES by index ID.
func (sc *SystemCatalog) GetIndexByID(tx TxContext, indexID int) (*systemtable.IndexMetadata, error) {
	return sc.indexOps.GetIndexByID(tx, indexID)
}

// DeleteIndexFromCatalog removes an index entry from CATALOG_INDEXES.
func (sc *SystemCatalog) DeleteIndexFromCatalog(tx TxContext, indexID int) error {
	return sc.indexOps.DeleteIndexFromCatalog(tx, indexID)
}

// UpdateTableStatistics collects and updates statistics for a given table
// Also updates the cache with fresh statistics
func (sc *SystemCatalog) UpdateTableStatistics(tx *transaction.TransactionContext, tableID int) error {
	return sc.statsOps.UpdateTableStatistics(tx, tableID)
}

// GetTableStatistics retrieves statistics for a given table
func (sc *SystemCatalog) GetTableStatistics(tx *transaction.TransactionContext, tableID int) (*systemtable.TableStatistics, error) {
	return sc.statsOps.GetTableStatistics(tx, tableID)
}

// CollectIndexStatistics collects statistics for a specific index
// Delegates to IndexStatsOperations for the actual work
func (sc *SystemCatalog) CollectIndexStatistics(
	tx *transaction.TransactionContext,
	indexID, tableID int,
	indexName string,
	indexType index.IndexType,
	columnName string,
) (*IndexStatistics, error) {
	indexStatsOps := sc.getIndexStatsOps()
	return indexStatsOps.CollectIndexStatistics(tx, indexID, tableID, indexName, indexType, columnName)
}

// UpdateIndexStatistics updates statistics for all indexes on a table
// Delegates to IndexStatsOperations for the actual work
func (sc *SystemCatalog) UpdateIndexStatistics(tx *transaction.TransactionContext, tableID int) error {
	indexStatsOps := sc.getIndexStatsOps()
	return indexStatsOps.UpdateIndexStatistics(tx, tableID)
}

// GetIndexStatistics retrieves statistics for a specific index from CATALOG_INDEX_STATISTICS
// Delegates to IndexStatsOperations for the actual work
func (sc *SystemCatalog) GetIndexStatistics(
	tx *transaction.TransactionContext,
	indexID int,
) (*IndexStatistics, error) {
	indexStatsOps := sc.getIndexStatsOps()
	return indexStatsOps.GetIndexStatistics(tx, indexID)
}

// getIndexStatsOps returns an IndexStatsOperations instance
func (sc *SystemCatalog) getIndexStatsOps() *operations.IndexStatsOperations {
	return operations.NewIndexStatsOperations(
		sc,
		sc.SystemTabs.IndexStatisticsTableID,
		sc.SystemTabs.IndexesTableID,
		sc.cache.GetDbFile,
		sc.statsOps,
	)
}

// IndexInfo represents basic index metadata (already exists in catalog)
type IndexInfo struct {
	IndexID    int
	TableID    int
	IndexName  string
	IndexType  index.IndexType
	ColumnName string
	FilePath   string
}

// GetIndexesForTable retrieves all indexes for a table
func (sc *SystemCatalog) GetIndexesForTable(tx *transaction.TransactionContext, tableID int) ([]*IndexInfo, error) {
	var indexes []*IndexInfo

	err := sc.iterateTable(sc.SystemTabs.IndexesTableID, tx, func(t *tuple.Tuple) error {
		// Parse index metadata properly using systemtable.Indexes
		im, err := systemtable.Indexes.Parse(t)
		if err != nil {
			return err
		}

		if im.TableID == tableID {
			// Convert IndexMetadata to IndexInfo
			indexes = append(indexes, &IndexInfo{
				IndexID:    im.IndexID,
				TableID:    im.TableID,
				IndexName:  im.IndexName,
				IndexType:  im.IndexType,
				ColumnName: im.ColumnName,
				FilePath:   im.FilePath,
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// CollectColumnStatistics collects statistics for a specific column in a table.
// Delegates to ColStatsOperations for the actual collection logic.
func (sc *SystemCatalog) CollectColumnStatistics(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
	columnIndex int,
	histogramBuckets int,
	mcvCount int,
) (*ColumnStatistics, error) {
	info, err := sc.colStatsOps.CollectColumnStatistics(tx, columnName, tableID, columnIndex, histogramBuckets, mcvCount)
	if err != nil {
		return nil, err
	}
	return toColumnStatistics(info), nil
}

// UpdateColumnStatistics updates statistics for all columns in a table.
// Delegates to ColStatsOperations which handles column iteration and stats storage.
func (sc *SystemCatalog) UpdateColumnStatistics(tx *transaction.TransactionContext, tableID int) error {
	return sc.colStatsOps.UpdateColumnStatistics(tx, tableID)
}

// GetColumnStatistics retrieves statistics for a specific column from CATALOG_COLUMN_STATISTICS.
// Note: Histogram and MCV data are not stored in the system table and will be nil.
// Use CollectColumnStatistics to rebuild these on demand if needed.
func (sc *SystemCatalog) GetColumnStatistics(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
) (*ColumnStatistics, error) {
	if sc.colStatsOps == nil {
		return nil, nil
	}

	row, err := sc.colStatsOps.GetColumnStatistics(tx, tableID, columnName)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	// Convert row to ColumnStatistics (Histogram and MCVs will be nil)
	return &ColumnStatistics{
		TableID:        row.TableID,
		ColumnName:     row.ColumnName,
		ColumnIndex:    row.ColumnIndex,
		DistinctCount:  row.DistinctCount,
		NullCount:      row.NullCount,
		MinValue:       stringToField(row.MinValue),
		MaxValue:       stringToField(row.MaxValue),
		AvgWidth:       row.AvgWidth,
		Histogram:      nil, // Not stored in system table
		MostCommonVals: nil, // Not stored in system table
		MCVFreqs:       nil, // Not stored in system table
		LastUpdated:    row.LastUpdated,
	}, nil
}
