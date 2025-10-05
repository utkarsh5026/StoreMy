package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/memory"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type SystemCatalog struct {
	pageStore    *memory.PageStore
	tableManager *memory.TableManager
	nextTableID  int
}

func NewSystemCatalog(ps *memory.PageStore, tm *memory.TableManager) *SystemCatalog {
	return &SystemCatalog{
		pageStore:    ps,
		tableManager: tm,
		nextTableID:  2, // 0 and 1 reserved for catalog tables
	}
}

func (sc *SystemCatalog) Initialize(dataDir string) error {
	tid := transaction.NewTransactionID()
	defer sc.pageStore.CommitTransaction(tid)

	tablesFile, err := heap.NewHeapFile(
		fmt.Sprintf("%s/catalog_tables.dat", dataDir),
		GetTablesSchema(),
	)
	if err != nil {
		return fmt.Errorf("failed to create tables catalog: %v", err)
	}

	if err := sc.tableManager.AddTable(tablesFile, "CATALOG_TABLES", "table_id"); err != nil {
		return err
	}

	columnsFile, err := heap.NewHeapFile(
		fmt.Sprintf("%s/catalog_columns.dat", dataDir),
		GetColumnsSchema(),
	)
	if err != nil {
		return fmt.Errorf("failed to create columns catalog: %v", err)
	}

	return sc.tableManager.AddTable(columnsFile, "CATALOG_COLUMNS", "")
}

// RegisterTable adds a table to the system catalog
func (sc *SystemCatalog) RegisterTable(
	tid *transaction.TransactionID,
	tableName string,
	filePath string,
	primaryKey string,
	fields []FieldMetadata,
) (int, error) {

	tableID := sc.nextTableID
	sc.nextTableID++

	tablesTuple := tuple.NewTuple(GetTablesSchema())
	tablesTuple.SetField(0, types.NewIntField(int32(tableID)))
	tablesTuple.SetField(1, types.NewStringField(tableName, types.StringMaxSize))
	tablesTuple.SetField(2, types.NewStringField(filePath, types.StringMaxSize))
	tablesTuple.SetField(3, types.NewStringField(primaryKey, types.StringMaxSize))

	if err := sc.pageStore.InsertTuple(tid, TablesTableID, tablesTuple); err != nil {
		return 0, fmt.Errorf("failed to insert table metadata: %v", err)
	}

	for pos, field := range fields {
		columnsTuple := tuple.NewTuple(GetColumnsSchema())
		columnsTuple.SetField(0, types.NewIntField(int32(tableID)))
		columnsTuple.SetField(1, types.NewStringField(field.Name, types.StringMaxSize))
		columnsTuple.SetField(2, types.NewIntField(int32(field.Type)))
		columnsTuple.SetField(3, types.NewIntField(int32(pos)))
		columnsTuple.SetField(4, types.NewBoolField(field.Name == primaryKey))

		if err := sc.pageStore.InsertTuple(tid, ColumnsTableID, columnsTuple); err != nil {
			return 0, fmt.Errorf("failed to insert column metadata: %v", err)
		}
	}

	return tableID, nil
}

type FieldMetadata struct {
	Name string
	Type types.Type
}
