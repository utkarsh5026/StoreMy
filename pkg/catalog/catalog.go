package catalog

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
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

// Initialize creates the system catalog tables (CATALOG_TABLES, CATALOG_COLUMNS, and CATALOG_STATISTICS)
// and registers them with the table manager. This must be called before any other
// catalog operations.
func (sc *SystemCatalog) Initialize(ctx *transaction.TransactionContext, dataDir string) error {
	defer sc.store.CommitTransaction(ctx)

	systemTables := []systemtable.SystemTable{
		systemtable.Tables,
		systemtable.Columns,
		systemtable.Stats,
	}

	for _, table := range systemTables {
		tableID, err := sc.createSystemTables(dataDir, table)
		if err != nil {
			return fmt.Errorf("failed to create %s catalog: %w", table.TableName(), err)
		}

		switch table.TableName() {
		case systemtable.Tables.TableName():
			sc.tablesTableID = tableID
		case systemtable.Columns.TableName():
			sc.columnsTableID = tableID
		case systemtable.Stats.TableName():
			sc.statisticsTableID = tableID
		}
	}

	sc.loader.columnsTableID = sc.columnsTableID
	return nil
}

// createCatalogTable creates a heap file for a system catalog table and registers it
// with the table manager. This is an internal helper for Initialize().
func (sc *SystemCatalog) createSystemTables(dataDir string, table systemtable.SystemTable) (int, error) {
	f, err := heap.NewHeapFile(
		fmt.Sprintf("%s/%s", dataDir, table.FileName()),
		table.Schema(),
	)
	if err != nil {
		return 0, err
	}

	if err := sc.tableManager.AddTable(f, table.TableName(), table.PrimaryKey()); err != nil {
		return 0, err
	}

	return f.GetID(), nil
}

// RegisterTable adds a new table to the system catalog.
// It inserts metadata into CATALOG_TABLES, column definitions into CATALOG_COLUMNS,
// and initializes statistics in CATALOG_STATISTICS.
func (sc *SystemCatalog) RegisterTable(tx *transaction.TransactionContext, tableID int, tableName, filePath, primaryKey string, fields []FieldMetadata,
) error {
	tup := systemtable.Tables.CreateTuple(tableID, tableName, filePath, primaryKey)
	if err := sc.store.InsertTuple(tx, sc.tablesTableID, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	for pos, f := range fields {
		tup = systemtable.Columns.CreateTuple(int64(tableID), f.Name, f.Type, int64(pos), f.Name == primaryKey, f.IsAutoInc)
		if err := sc.store.InsertTuple(tx, sc.columnsTableID, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	// Note: Statistics are not initialized here automatically.
	// Call UpdateTableStatistics explicitly after populating the table with data.
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
		tableID, name, filePath := parseTableMetadata(tableTuple)

		schema, pk, err := sc.loader.LoadTableSchema(tx.ID, tableID)
		if err != nil {
			return fmt.Errorf("failed to load schema for table %s: %w", name, err)
		}

		f, err := heap.NewHeapFile(filePath, schema)
		if err != nil {
			return fmt.Errorf("failed to open heap file for %s: %w", name, err)
		}

		if err := sc.tableManager.AddTable(f, name, pk); err != nil {
			return fmt.Errorf("failed to add table %s: %w", name, err)
		}

		return nil
	})
}

// GetTableID looks up the heap file ID for a table by name.
// Returns -1 and an error if the table is not found in the catalog.
func (sc *SystemCatalog) GetTableID(tid *primitives.TransactionID, tableName string) (int, error) {
	var result int = -1

	err := sc.iterateTable(sc.tablesTableID, tid, func(tableTuple *tuple.Tuple) error {
		tableID, name, _ := parseTableMetadata(tableTuple)

		if name == tableName {
			result = tableID
			return fmt.Errorf("found") // Use error to break iteration
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

	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		if err := processFunc(tup); err != nil {
			return err
		}
	}

	return nil
}

// parseTableMetadata extracts table metadata fields from a CATALOG_TABLES tuple
func parseTableMetadata(tableTuple *tuple.Tuple) (tableID int, tableName, filePath string) {
	return getIntField(tableTuple, 0),
		getStringField(tableTuple, 1),
		getStringField(tableTuple, 2)
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

	var oldTuple *tuple.Tuple
	var colType types.Type
	var position int
	var isPrimary bool
	var isAutoInc bool
	maxValue := -1

	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		colTableID := getIntField(tup, 0)
		colName := getStringField(tup, 1)

		if colTableID == tableID && colName == columnName {
			currentValue := getIntField(tup, 6)
			if currentValue > maxValue {
				oldTuple = tup
				colType = types.Type(getIntField(tup, 2))
				position = getIntField(tup, 3)
				isPrimary = getBoolField(tup, 4)
				isAutoInc = getBoolField(tup, 5)
				maxValue = currentValue
			}
		}
	}

	if oldTuple == nil {
		return fmt.Errorf("auto-increment column %s not found for table %d", columnName, tableID)
	}

	newTuple := systemtable.Columns.CreateTuple(int64(tableID), columnName, colType, int64(position), isPrimary, isAutoInc)
	newTuple.SetField(6, types.NewIntField(int64(newValue)))

	if err := sc.store.DeleteTuple(tx, oldTuple); err != nil {
		return fmt.Errorf("failed to delete old tuple: %w", err)
	}

	if err := sc.store.InsertTuple(tx, sc.columnsTableID, newTuple); err != nil {
		return fmt.Errorf("failed to insert updated tuple: %w", err)
	}

	return nil
}
