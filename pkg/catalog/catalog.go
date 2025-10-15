package catalog

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// SystemCatalog manages database metadata including table and column definitions.
// It maintains four system tables:
//   - CATALOG_TABLES: stores table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: stores column metadata (table ID, name, type, position, is_primary)
//   - CATALOG_STATISTICS: stores table statistics for query optimization
//   - CATALOG_INDEXES: stores index metadata (ID, name, table ID, column, type, file path)
type SystemCatalog struct {
	store             *memory.PageStore
	cache             *tablecache.TableCache
	tupMgr            *table.TupleManager
	TablesTableID     int
	ColumnsTableID    int
	StatisticsTableID int
	IndexesTableID    int
}

// NewSystemCatalog creates a new system catalog instance.
// The catalog must be initialized via Initialize() before use.
func NewSystemCatalog(ps *memory.PageStore, cache *tablecache.TableCache) *SystemCatalog {
	sc := &SystemCatalog{
		store:  ps,
		cache:  cache,
		tupMgr: table.NewTupleManager(ps),
	}
	return sc
}

// Initialize creates or loads the system catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, and CATALOG_INDEXES)
// and registers them with the table manager. This must be called before any other catalog operations.
// Works for both new and existing databases - heap.NewHeapFile handles both creation and loading.
//
// Parameters:
//   - ctx: Transaction context for all catalog initialization operations
//   - dataDir: Directory path where system catalog heap files will be stored/loaded
//
// Returns an error if any system table cannot be created, loaded, or registered.
// On success, the catalog is ready for use and the transaction is committed.
func (sc *SystemCatalog) Initialize(ctx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(ctx)

	systemTables := []systemtable.SystemTable{
		systemtable.Tables,
		systemtable.Columns,
		systemtable.Stats,
		systemtable.Indexes,
	}

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

		sc.setSystemTableID(table.TableName(), f.GetID())

		// Register the DbFile with PageStore for flush operations
		sc.store.RegisterDbFile(f.GetID(), f)
	}
	return nil
}

// setSystemTableID sets the appropriate system table ID field based on table name.
// This is called during initialization to track the IDs of the four system catalog tables.
//
// Parameters:
//   - tableName: Name of the system table (CATALOG_TABLES, CATALOG_COLUMNS, CATALOG_STATISTICS, or CATALOG_INDEXES)
//   - tableID: Heap file ID assigned to this system table
func (sc *SystemCatalog) setSystemTableID(tableName string, tableID int) {
	switch tableName {
	case systemtable.Tables.TableName():
		sc.TablesTableID = tableID
	case systemtable.Columns.TableName():
		sc.ColumnsTableID = tableID
	case systemtable.Stats.TableName():
		sc.StatisticsTableID = tableID
	case systemtable.Indexes.TableName():
		sc.IndexesTableID = tableID
	}
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
func (sc *SystemCatalog) RegisterTable(tx *transaction.TransactionContext, sch *schema.Schema, filepath string) error {
	tablesFile, err := sc.cache.GetDbFile(sc.TablesTableID)
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

	colFile, err := sc.cache.GetDbFile(sc.ColumnsTableID)
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
func (sc *SystemCatalog) LoadTables(tx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(tx)

	return sc.iterateTable(sc.TablesTableID, tx, func(tableTuple *tuple.Tuple) error {
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
func (sc *SystemCatalog) LoadTable(tx *transaction.TransactionContext, tableName string) error {
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
func (sc *SystemCatalog) DeleteCatalogEntry(tx *transaction.TransactionContext, tableID int) error {
	sysTableIDs := []int{sc.TablesTableID, sc.ColumnsTableID, sc.StatisticsTableID, sc.IndexesTableID}
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
func (sc *SystemCatalog) DeleteTableFromSysTable(tx *transaction.TransactionContext, tableID, sysTableID int) error {
	tableInfo, err := sc.cache.GetTableInfo(sysTableID)
	if err != nil {
		return err
	}

	syst, err := sc.getSysTable(sysTableID)
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

// getSysTable returns the SystemTable interface for a given system table ID.
// This is used to access system table-specific methods like TableIDIndex().
//
// Parameters:
//   - id: System table ID (TablesTableID, ColumnsTableID, StatisticsTableID, or IndexesTableID)
//
// Returns the corresponding SystemTable interface or an error if the ID is invalid.
func (sc *SystemCatalog) getSysTable(id int) (systemtable.SystemTable, error) {
	switch id {
	case sc.TablesTableID:
		return systemtable.Tables, nil
	case sc.ColumnsTableID:
		return systemtable.Columns, nil
	case sc.StatisticsTableID:
		return systemtable.Stats, nil
	case sc.IndexesTableID:
		return systemtable.Indexes, nil
	default:
		return nil, fmt.Errorf("unknown system table ID: %d", id)
	}
}

// GetTableMetadataByID retrieves complete table metadata from CATALOG_TABLES by table ID.
// Returns TableMetadata containing table name, file path, and primary key column,
// or an error if the table is not found.
func (sc *SystemCatalog) GetTableMetadataByID(tx *transaction.TransactionContext, tableID int) (*systemtable.TableMetadata, error) {
	return sc.findTableMetadata(tx, func(tm *systemtable.TableMetadata) bool {
		return tm.TableID == tableID
	})
}

// GetTableMetadataByName retrieves complete table metadata from CATALOG_TABLES by table name.
// Table name matching is case-insensitive.
// Returns TableMetadata containing table ID, file path, and primary key column,
// or an error if the table is not found.
func (sc *SystemCatalog) GetTableMetadataByName(tx *transaction.TransactionContext, tableName string) (*systemtable.TableMetadata, error) {
	return sc.findTableMetadata(tx, func(tm *systemtable.TableMetadata) bool {
		return strings.EqualFold(tm.TableName, tableName)
	})
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
func (sc *SystemCatalog) iterateTable(tableID int, tx *transaction.TransactionContext, processFunc func(*tuple.Tuple) error) error {
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

// findTableMetadata is a generic helper for searching CATALOG_TABLES with a custom predicate.
// Used by GetTableMetadataByID and GetTableMetadataByName to avoid code duplication.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - pred: Predicate function that returns true when the desired table is found
//
// Returns the matching TableMetadata or an error if not found or if catalog access fails.
func (sc *SystemCatalog) findTableMetadata(tx *transaction.TransactionContext, pred func(tm *systemtable.TableMetadata) bool) (*systemtable.TableMetadata, error) {
	var result *systemtable.TableMetadata

	err := sc.iterateTable(sc.TablesTableID, tx, func(tableTuple *tuple.Tuple) error {
		table, err := systemtable.Tables.Parse(tableTuple)
		if err != nil {
			return err
		}

		if pred(table) {
			result = table
			return fmt.Errorf("found")
		}

		return nil
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("table not found in catalog")
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
func (sc *SystemCatalog) GetAllTables(tx *transaction.TransactionContext) ([]*systemtable.TableMetadata, error) {
	var tables []*systemtable.TableMetadata
	err := sc.iterateTable(sc.TablesTableID, tx, func(tup *tuple.Tuple) error {
		tm, err := systemtable.Tables.Parse(tup)
		if err != nil {
			return err
		}
		tables = append(tables, tm)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tables, nil
}
