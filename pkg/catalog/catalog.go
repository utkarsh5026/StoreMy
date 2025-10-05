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
	store          *memory.PageStore
	tableManager   *memory.TableManager
	loader         *SchemaLoader
	tablesTableID  int // Actual ID of CATALOG_TABLES
	columnsTableID int // Actual ID of CATALOG_COLUMNS
}

func NewSystemCatalog(ps *memory.PageStore, tm *memory.TableManager) *SystemCatalog {
	sc := &SystemCatalog{
		store:        ps,
		tableManager: tm,
	}

	sc.loader = NewSchemaLoader(0, sc.iterateTable)
	return sc
}

func (sc *SystemCatalog) Initialize(dataDir string) error {
	tid := transaction.NewTransactionID()
	defer sc.store.CommitTransaction(tid)

	var err error
	sc.tablesTableID, err = sc.createCatalogTable(dataDir, CatalogTableFileName, CatalogTable, "table_id", GetTablesSchema())
	if err != nil {
		return fmt.Errorf("failed to create tables catalog: %w", err)
	}

	sc.columnsTableID, err = sc.createCatalogTable(dataDir, CatalogColumnsFileName, ColumnsTable, "", GetColumnsSchema())
	if err != nil {
		return fmt.Errorf("failed to create columns catalog: %w", err)
	}

	sc.loader.columnsTableID = sc.columnsTableID
	return nil
}

func (sc *SystemCatalog) createCatalogTable(dataDir, fileName, tableName, primaryKey string, schema *tuple.TupleDescription) (int, error) {
	f, err := heap.NewHeapFile(
		fmt.Sprintf("%s/%s", dataDir, fileName),
		schema,
	)
	if err != nil {
		return 0, err
	}

	if err := sc.tableManager.AddTable(f, tableName, primaryKey); err != nil {
		return 0, err
	}

	return f.GetID(), nil
}

// RegisterTable adds a table to the system catalog
// The tableID parameter should be the actual heap file ID
func (sc *SystemCatalog) RegisterTable(tid *transaction.TransactionID, tableID int, tableName, filePath, primaryKey string, fields []FieldMetadata,
) error {
	tup := createTablesTuple(tableID, tableName, filePath, primaryKey)
	if err := sc.store.InsertTuple(tid, sc.tablesTableID, tup); err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}

	for pos, f := range fields {
		tup = createColumnsTuple(tableID, f.Name, f.Type, pos, f.Name == primaryKey)
		if err := sc.store.InsertTuple(tid, sc.columnsTableID, tup); err != nil {
			return fmt.Errorf("failed to insert column metadata: %w", err)
		}
	}

	return nil
}

func createTablesTuple(tableID int, tableName, filePath, primaryKey string) *tuple.Tuple {
	t := tuple.NewTuple(GetTablesSchema())
	t.SetField(0, types.NewIntField(int32(tableID)))
	t.SetField(1, types.NewStringField(tableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(filePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(primaryKey, types.StringMaxSize))
	return t
}

func createColumnsTuple(tableID int, colName string, colType types.Type, position int, isPrimary bool) *tuple.Tuple {
	t := tuple.NewTuple(GetColumnsSchema())
	t.SetField(0, types.NewIntField(int32(tableID)))
	t.SetField(1, types.NewStringField(colName, types.StringMaxSize))
	t.SetField(2, types.NewIntField(int32(colType)))
	t.SetField(3, types.NewIntField(int32(position)))
	t.SetField(4, types.NewBoolField(isPrimary))
	return t
}

type FieldMetadata struct {
	Name string
	Type types.Type
}

func (sc *SystemCatalog) LoadTables(dataDir string) error {
	tid := transaction.NewTransactionID()
	defer sc.store.CommitTransaction(tid)

	return sc.iterateTable(sc.tablesTableID, tid, func(tableTuple *tuple.Tuple) error {
		tableID, name, filePath := parseTableMetadata(tableTuple)

		schema, pk, err := sc.loader.LoadTableSchema(tid, tableID)
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

func (sc *SystemCatalog) GetTablesTableID() int {
	return sc.tablesTableID
}

func (sc *SystemCatalog) GetColumnsTableID() int {
	return sc.columnsTableID
}

func (sc *SystemCatalog) iterateTable(tableID int, tid *transaction.TransactionID, processFunc func(*tuple.Tuple) error) error {
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

func parseTableMetadata(tableTuple *tuple.Tuple) (tableID int, tableName, filePath string) {
	return getIntField(tableTuple, 0),
		getStringField(tableTuple, 1),
		getStringField(tableTuple, 2)
}
