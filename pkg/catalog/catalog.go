package catalog

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/memory"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// SystemCatalog manages database metadata including table and column definitions.
// It maintains three system tables:
//   - CATALOG_TABLES: stores table metadata (ID, name, file path, primary key)
//   - CATALOG_COLUMNS: stores column metadata (table ID, name, type, position, is_primary)
//   - CATALOG_STATISTICS: stores table statistics for query optimization
type SystemCatalog struct {
	store             *memory.PageStore
	tableManager      *memory.TableManager
	loader            *SchemaLoader
	tablesTableID     int
	columnsTableID    int
	statisticsTableID int
}

// NewSystemCatalog creates a new system catalog instance.
// The catalog must be initialized via Initialize() before use.
func NewSystemCatalog(ps *memory.PageStore, tm *memory.TableManager) *SystemCatalog {
	sc := &SystemCatalog{
		store:        ps,
		tableManager: tm,
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
		f, err := heap.NewHeapFile(
			filepath.Join(dataDir, table.FileName()),
			table.Schema(),
		)
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %w", table.TableName(), err)
		}

		if err := sc.tableManager.AddTable(f, table.TableName(), table.PrimaryKey()); err != nil {
			return fmt.Errorf("failed to register %s: %w", table.TableName(), err)
		}

		sc.setSystemTableID(table.TableName(), f.GetID())
	}

	sc.loader.columnsTableID = sc.columnsTableID
	return nil
}

// setSystemTableID sets the appropriate system table ID based on table name
func (sc *SystemCatalog) setSystemTableID(tableName string, tableID int) {
	switch tableName {
	case systemtable.Tables.TableName():
		sc.tablesTableID = tableID
	case systemtable.Columns.TableName():
		sc.columnsTableID = tableID
	case systemtable.Stats.TableName():
		sc.statisticsTableID = tableID
	}
}

// RegisterTable adds a new table to the system catalog.
// It inserts metadata into CATALOG_TABLES, column definitions into CATALOG_COLUMNS,
// and initializes statistics in CATALOG_STATISTICS.
func (sc *SystemCatalog) RegisterTable(tx *transaction.TransactionContext, tableID int, tableName, filePath, primaryKey string, fields []FieldMetadata,
) error {
	tm := systemtable.TableMetadata{
		TableName:     tableName,
		TableID:       tableID,
		FilePath:      filePath,
		PrimaryKeyCol: primaryKey,
	}
	tup := systemtable.Tables.CreateTuple(tm)
	if err := sc.store.InsertTuple(tx, sc.tablesTableID, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	for pos, f := range fields {
		col := schema.ColumnMetadata{
			TableID:   tableID,
			Name:      f.Name,
			FieldType: f.Type,
			Position:  pos,
			IsPrimary: f.Name == primaryKey,
			IsAutoInc: f.IsAutoInc,
		}
		tup = systemtable.Columns.CreateTuple(col)
		if err := sc.store.InsertTuple(tx, sc.columnsTableID, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

type FieldMetadata struct {
	Name      string
	Type      types.Type
	IsAutoInc bool
}

// LoadTables reads all table metadata from CATALOG_TABLES, reconstructs their schemas
// from CATALOG_COLUMNS, opens their heap files, and registers them with the table manager.
//
// This is called during database startup to restore all user tables.
func (sc *SystemCatalog) LoadTables(tx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(tx)

	return sc.iterateTable(sc.tablesTableID, tx.ID, func(tableTuple *tuple.Tuple) error {
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

		if err := sc.tableManager.AddTable(f, table.TableName, sch.PrimaryKey); err != nil {
			return fmt.Errorf("failed to add table %s: %w", table.TableName, err)
		}

		return nil
	})
}

// GetTableID looks up the heap file ID for a table by name.
// Returns -1 and an error if the table is not found in the catalog.
func (sc *SystemCatalog) GetTableID(tid *primitives.TransactionID, tableName string) (int, error) {
	var result int = -1

	err := sc.iterateTable(sc.tablesTableID, tid, func(tableTuple *tuple.Tuple) error {
		table, err := systemtable.Tables.Parse(tableTuple)
		if err != nil {
			return err
		}

		if table.TableName == tableName {
			result = table.TableID
			return fmt.Errorf("found")
		}
		return nil
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return -1, err
	}

	return -1, fmt.Errorf("table %s not found in catalog", tableName)
}

// iterateTable scans all tuples in a table and applies a processing function to each.
// This is used internally for catalog queries like LoadTables() and GetTableID().
//
// The processFunc can return an error to stop iteration early.
func (sc *SystemCatalog) iterateTable(tableID int, tid *primitives.TransactionID, processFunc func(*tuple.Tuple) error) error {
	file, err := sc.tableManager.GetDbFile(tableID)
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

// GetTablesTableID returns the table ID for CATALOG_TABLES
func (sc *SystemCatalog) GetTablesTableID() int {
	return sc.tablesTableID
}

// GetColumnsTableID returns the table ID for CATALOG_COLUMNS
func (sc *SystemCatalog) GetColumnsTableID() int {
	return sc.columnsTableID
}

// GetStatisticsTableID returns the table ID for CATALOG_STATISTICS
func (sc *SystemCatalog) GetStatisticsTableID() int {
	return sc.statisticsTableID
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

	err := sc.iterateTable(sc.columnsTableID, tid, func(columnTuple *tuple.Tuple) error {
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
	file, err := sc.tableManager.GetDbFile(sc.columnsTableID)
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

	if err := sc.store.DeleteTuple(tx, match.Tuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := sc.store.InsertTuple(tx, sc.columnsTableID, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}
