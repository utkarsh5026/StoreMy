package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

const (
	CatalogTable           = "CATALOG_TABLES"
	ColumnsTable           = "CATALOG_COLUMNS"
	CatalogTableFileName   = "catalog_tables.dat"
	CatalogColumnsFileName = "catalog_columns.dat"
)

type SystemCatalog struct {
	pageStore      *memory.PageStore
	tableManager   *memory.TableManager
	schemaLoader   *SchemaLoader
	tablesTableID  int // Actual ID of CATALOG_TABLES
	columnsTableID int // Actual ID of CATALOG_COLUMNS
}

func NewSystemCatalog(ps *memory.PageStore, tm *memory.TableManager) *SystemCatalog {
	sc := &SystemCatalog{
		pageStore:    ps,
		tableManager: tm,
	}

	sc.schemaLoader = NewSchemaLoader(0, sc.iterateTable)
	return sc
}

func (sc *SystemCatalog) Initialize(dataDir string) error {
	tid := transaction.NewTransactionID()
	defer sc.pageStore.CommitTransaction(tid)

	var err error
	sc.tablesTableID, err = sc.createCatalogTable(dataDir, CatalogTableFileName, CatalogTable, "table_id", GetTablesSchema())
	if err != nil {
		return fmt.Errorf("failed to create tables catalog: %w", err)
	}

	sc.columnsTableID, err = sc.createCatalogTable(dataDir, CatalogColumnsFileName, ColumnsTable, "", GetColumnsSchema())
	if err != nil {
		return fmt.Errorf("failed to create columns catalog: %w", err)
	}

	return nil
}

func (sc *SystemCatalog) createCatalogTable(dataDir, fileName, tableName, primaryKey string, schema *tuple.TupleDescription) (int, error) {
	heapFile, err := heap.NewHeapFile(
		fmt.Sprintf("%s/%s", dataDir, fileName),
		schema,
	)
	if err != nil {
		return 0, err
	}

	if err := sc.tableManager.AddTable(heapFile, tableName, primaryKey); err != nil {
		return 0, err
	}

	return heapFile.GetID(), nil
}

// RegisterTable adds a table to the system catalog
// The tableID parameter should be the actual heap file ID
func (sc *SystemCatalog) RegisterTable(
	tid *transaction.TransactionID,
	tableID int,
	tableName string,
	filePath string,
	primaryKey string,
	fields []FieldMetadata,
) error {

	tablesTuple := createTablesTuple(tableID, tableName, filePath, primaryKey)
	if err := sc.pageStore.InsertTuple(tid, sc.tablesTableID, tablesTuple); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	for pos, field := range fields {
		columnsTuple := createColumnsTuple(tableID, field.Name, field.Type, pos, field.Name == primaryKey)
		if err := sc.pageStore.InsertTuple(tid, sc.columnsTableID, columnsTuple); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

func createTablesTuple(tableID int, tableName, filePath, primaryKey string) *tuple.Tuple {
	tablesTuple := tuple.NewTuple(GetTablesSchema())
	tablesTuple.SetField(0, types.NewIntField(int32(tableID)))
	tablesTuple.SetField(1, types.NewStringField(tableName, types.StringMaxSize))
	tablesTuple.SetField(2, types.NewStringField(filePath, types.StringMaxSize))
	tablesTuple.SetField(3, types.NewStringField(primaryKey, types.StringMaxSize))
	return tablesTuple
}

func createColumnsTuple(tableID int, colName string, colType types.Type, position int, isPrimary bool) *tuple.Tuple {
	columnsTuple := tuple.NewTuple(GetColumnsSchema())
	columnsTuple.SetField(0, types.NewIntField(int32(tableID)))
	columnsTuple.SetField(1, types.NewStringField(colName, types.StringMaxSize))
	columnsTuple.SetField(2, types.NewIntField(int32(colType)))
	columnsTuple.SetField(3, types.NewIntField(int32(position)))
	columnsTuple.SetField(4, types.NewBoolField(isPrimary))
	return columnsTuple
}

type FieldMetadata struct {
	Name string
	Type types.Type
}

func (sc *SystemCatalog) LoadTables(dataDir string) error {
	tid := transaction.NewTransactionID()
	defer sc.pageStore.CommitTransaction(tid)

	return sc.iterateTable(sc.tablesTableID, tid, func(tableTuple *tuple.Tuple) error {
		tableID, tableName, filePath := parseTableMetadata(tableTuple)

		tupleSchema, pk, err := sc.schemaLoader.LoadTableSchema(tid, tableID)
		if err != nil {
			return fmt.Errorf("failed to load schema for table %s: %w", tableName, err)
		}

		heapFile, err := heap.NewHeapFile(filePath, tupleSchema)
		if err != nil {
			return fmt.Errorf("failed to open heap file for %s: %w", tableName, err)
		}

		if err := sc.tableManager.AddTable(heapFile, tableName, pk); err != nil {
			return fmt.Errorf("failed to add table %s: %w", tableName, err)
		}

		return nil
	})
}

// GetTableID returns the ID for a table name, or -1 if not found
func (sc *SystemCatalog) GetTableID(tid *transaction.TransactionID, tableName string) (int, error) {
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

// GetTablesTableID returns the actual table ID for CATALOG_TABLES
func (sc *SystemCatalog) GetTablesTableID() int {
	return sc.tablesTableID
}

// GetColumnsTableID returns the actual table ID for CATALOG_COLUMNS
func (sc *SystemCatalog) GetColumnsTableID() int {
	return sc.columnsTableID
}

func (sc *SystemCatalog) iterateTable(tableID int, tid *transaction.TransactionID, processFunc func(*tuple.Tuple) error) error {
	dbFile, err := sc.tableManager.GetDbFile(tableID)
	if err != nil {
		return fmt.Errorf("failed to get table file: %w", err)
	}

	iterator := dbFile.Iterator(tid)
	if err := iterator.Open(); err != nil {
		return fmt.Errorf("failed to open iterator: %w", err)
	}
	defer iterator.Close()

	for {
		hasNext, err := iterator.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iterator.Next()
		if err != nil || tup == nil {
			break
		}

		if err := processFunc(tup); err != nil {
			return err
		}
	}

	return nil
}

func parseTableMetadata(tableTuple *tuple.Tuple) (tableID int, tableName, filePath string) {
	return getIntField(tableTuple, 0),
		getStringField(tableTuple, 1),
		getStringField(tableTuple, 2)
}
