package catalog

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// SystemCatalog manages database metadata including table and column definitions.
// It maintains three system tables:
//   - CATALOG_TABLES: stores table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: stores column metadata (table ID, name, type, position, is_primary)
//   - CATALOG_STATISTICS: stores table statistics for query optimization
type SystemCatalog struct {
	store             PageStoreInterface
	cache             *tableCache
	loader            *SchemaLoader
	TablesTableID     int
	ColumnsTableID    int
	StatisticsTableID int
}

// NewSystemCatalog creates a new system catalog instance.
// The catalog must be initialized via Initialize() before use.
func NewSystemCatalog(ps PageStoreInterface, cache *tableCache) *SystemCatalog {
	sc := &SystemCatalog{
		store: ps,
		cache: cache,
	}

	sc.loader = NewSchemaLoader(0, sc.iterateTable)
	return sc
}

// Initialize creates or loads the system catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, and CATALOG_STATISTICS)
// and registers them with the table manager. This must be called before any other catalog operations.
// Works for both new and existing databases - heap.NewHeapFile handles both creation and loading.
func (sc *SystemCatalog) Initialize(ctx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(ctx)

	systemTables := []systemtable.SystemTable{
		systemtable.Tables,
		systemtable.Columns,
		systemtable.Stats,
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
	}

	sc.loader.columnsTableID = sc.ColumnsTableID
	return nil
}

// setSystemTableID sets the appropriate system table ID based on table name
func (sc *SystemCatalog) setSystemTableID(tableName string, tableID int) {
	switch tableName {
	case systemtable.Tables.TableName():
		sc.TablesTableID = tableID
	case systemtable.Columns.TableName():
		sc.ColumnsTableID = tableID
	case systemtable.Stats.TableName():
		sc.StatisticsTableID = tableID
	}
}

// RegisterTable adds a new table to the system catalog.
// It inserts metadata into CATALOG_TABLES, column definitions into CATALOG_COLUMNS,
// and initializes statistics in CATALOG_STATISTICS.
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
	if err := sc.store.InsertTuple(tx, tablesFile, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	colFile, err := sc.cache.GetDbFile(sc.ColumnsTableID)
	if err != nil {
		return err
	}

	for _, col := range sch.Columns {
		tup = systemtable.Columns.CreateTuple(col)
		if err := sc.store.InsertTuple(tx, colFile, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

// LoadTables reads all table metadata from CATALOG_TABLES, reconstructs their schemas
// from CATALOG_COLUMNS, opens their heap files, and registers them with the table manager.
//
// This is called during database startup to restore all user tables.
func (sc *SystemCatalog) LoadTables(tx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(tx)

	return sc.iterateTable(sc.TablesTableID, tx.ID, func(tableTuple *tuple.Tuple) error {
		table, err := systemtable.Tables.Parse(tableTuple)
		if err != nil {
			return err
		}

		sch, err := sc.loader.LoadTableSchema(tx.ID, table.TableID)
		if err != nil {
			return fmt.Errorf("failed to load schema for table %s: %w", table.TableName, err)
		}

		f, err := heap.NewHeapFile(table.FilePath, sch.TupleDesc)
		if err != nil {
			return fmt.Errorf("failed to open heap file for %s: %w", table.TableName, err)
		}

		if err := sc.cache.AddTable(f, sch); err != nil {
			return fmt.Errorf("failed to add table %s: %w", table.TableName, err)
		}

		return nil
	})
}

func (sc *SystemCatalog) DeleteCatalogEntry(tx *transaction.TransactionContext, tableID int) error {
	sysTableIDs := []int{sc.TablesTableID, sc.ColumnsTableID, sc.StatisticsTableID}
	for _, id := range sysTableIDs {
		if err := sc.DeleteTableFromSysTable(tx, tableID, id); err != nil {
			return err
		}
	}
	return nil
}

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

	sc.iterateTable(sysTableID, tx.ID, func(t *tuple.Tuple) error {
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
		if err := sc.store.DeleteTuple(tx, tableInfo.File, tup); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SystemCatalog) getSysTable(id int) (systemtable.SystemTable, error) {
	switch id {
	case sc.TablesTableID:
		return systemtable.Tables, nil
	case sc.ColumnsTableID:
		return systemtable.Columns, nil
	case sc.StatisticsTableID:
		return systemtable.Stats, nil
	default:
		return nil, fmt.Errorf("unknown system table ID: %d", id)
	}
}

// GetTableID looks up the heap file ID for a table by name.
// Returns -1 and an error if the table is not found in the catalog.
func (sc *SystemCatalog) GetTableMetadataByID(tid *primitives.TransactionID, tableID int) (*systemtable.TableMetadata, error) {
	return sc.findTableMetadata(tid, func(tm *systemtable.TableMetadata) bool {
		return tm.TableID == tableID
	})
}

func (sc *SystemCatalog) GetTableMetadataByName(tid *primitives.TransactionID, tableName string) (*systemtable.TableMetadata, error) {
	return sc.findTableMetadata(tid, func(tm *systemtable.TableMetadata) bool {
		return strings.EqualFold(tm.TableName, tableName)
	})
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is used internally for catalog queries like LoadTables() and GetTableID().
//
// The processFunc can return an error to stop iteration early.
func (sc *SystemCatalog) iterateTable(tableID int, tid *primitives.TransactionID, processFunc func(*tuple.Tuple) error) error {
	file, err := sc.cache.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	iter := file.Iterator(tid)
	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iter: %w", err)
	}
	defer iter.Close()

	return iterator.ForEach(iter, processFunc)
}

// AutoIncrementInfo represents auto-increment metadata for a column
type AutoIncrementInfo struct {
	ColumnName  string
	ColumnIndex int
	NextValue   int
}

// GetAutoIncrementColumn retrieves auto-increment column info for a table
// Returns nil if the table has no auto-increment column
// Note: Due to MVCC, there may be multiple versions of the same column tuple.
// We return the one with the highest next_auto_value (most recent).
func (sc *SystemCatalog) GetAutoIncrementColumn(tid *primitives.TransactionID, tableID int) (*AutoIncrementInfo, error) {
	var result *AutoIncrementInfo

	err := sc.iterateTable(sc.ColumnsTableID, tid, func(columnTuple *tuple.Tuple) error {
		colTableID, err := systemtable.Columns.GetTableID(columnTuple)
		if err != nil {
			return err
		}

		if colTableID != tableID {
			return nil
		}

		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return err
		}

		if !col.IsAutoInc {
			return nil
		}

		if result == nil || col.NextAutoValue > result.NextValue {
			result = &AutoIncrementInfo{
				ColumnName:  getStringField(columnTuple, 1),
				ColumnIndex: getIntField(columnTuple, 3),
				NextValue:   col.NextAutoValue,
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// IncrementAutoIncrementValue updates the next auto-increment value for a column
func (sc *SystemCatalog) IncrementAutoIncrementValue(tx *transaction.TransactionContext, tableID int, columnName string, newValue int) error {
	file, err := sc.cache.GetDbFile(sc.ColumnsTableID)
	if err != nil {
		return fmt.Errorf("failed to get columns table: %w", err)
	}

	iter := file.Iterator(tx.ID)
	if err := iter.Open(); err != nil {
		return fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iter.Close()

	// Use system table method to find latest version (MVCC-aware)
	match, err := systemtable.Columns.FindLatestAutoIncrementColumn(iter, tableID, columnName)
	if err != nil {
		return fmt.Errorf("failed to find auto-increment column: %w", err)
	}

	if match == nil {
		return fmt.Errorf("auto-increment column %s not found for table %d", columnName, tableID)
	}

	col := schema.ColumnMetadata{
		TableID:   tableID,
		Name:      columnName,
		FieldType: match.ColumnType,
		Position:  match.Position,
		IsPrimary: match.IsPrimary,
		IsAutoInc: match.IsAutoInc,
	}
	newTuple := systemtable.Columns.CreateTuple(col)
	newTuple.SetField(6, types.NewIntField(int64(newValue)))

	if err := sc.store.DeleteTuple(tx, file, match.Tuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := sc.store.InsertTuple(tx, file, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}

func (sc *SystemCatalog) findTableMetadata(tid *primitives.TransactionID, pred func(tm *systemtable.TableMetadata) bool) (*systemtable.TableMetadata, error) {
	var result *systemtable.TableMetadata

	err := sc.iterateTable(sc.TablesTableID, tid, func(tableTuple *tuple.Tuple) error {
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

func (sc *SystemCatalog) getAllTableIDS() []int {
	return []int{sc.TablesTableID, sc.ColumnsTableID, sc.StatisticsTableID}
}

func (sc *SystemCatalog) GetAllTables(tid *primitives.TransactionID) ([]*systemtable.TableMetadata, error) {
	var tables []*systemtable.TableMetadata
	err := sc.iterateTable(sc.TablesTableID, tid, func(tup *tuple.Tuple) error {
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
