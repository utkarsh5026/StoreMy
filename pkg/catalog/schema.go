package catalog

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

const (
	TablesTableID  = 0
	ColumnsTableID = 1
)

// GetTablesSchema returns the schema for the system tables catalog
func GetTablesSchema() *tuple.TupleDescription {
	types := []types.Type{
		types.IntType,
		types.StringType,
		types.StringType,
		types.StringType,
	}

	names := []string{
		"table_id",
		"table_name",
		"file_path",
		"primary_key",
	}

	desc, _ := tuple.NewTupleDesc(types, names)
	return desc
}

// GetColumnsSchema returns the schema for the system columns catalog
func GetColumnsSchema() *tuple.TupleDescription {
	types := []types.Type{
		types.IntType,
		types.StringType,
		types.IntType,
		types.IntType,
		types.BoolType,
	}

	names := []string{
		"table_id",
		"column_name",
		"type_id",
		"position",
		"is_primary_key",
	}

	desc, _ := tuple.NewTupleDesc(types, names)
	return desc
}

type ColumnInfo struct {
	name      string
	fieldType types.Type
	position  int
	isPrimary bool
}

type IterFunc = func(int, *transaction.TransactionID, func(*tuple.Tuple) error) error

// SchemaLoader handles loading and parsing schema information
type SchemaLoader struct {
	columnsTableID int
	iterateFunc    IterFunc
}

func NewSchemaLoader(columnsTableID int, iterateFunc IterFunc) *SchemaLoader {
	return &SchemaLoader{
		columnsTableID: columnsTableID,
		iterateFunc:    iterateFunc,
	}
}

func (sl *SchemaLoader) LoadTableSchema(tid *transaction.TransactionID, tableID int) (*tuple.TupleDescription, string, error) {
	columns, primaryKey, err := sl.loadColumnMetadata(tid, tableID)
	if err != nil {
		return nil, "", err
	}

	if len(columns) == 0 {
		return nil, "", fmt.Errorf("no columns found for table %d", tableID)
	}

	tupleDesc := buildTupleDescription(columns)
	return tupleDesc, primaryKey, nil
}

func buildTupleDescription(columns []ColumnInfo) *tuple.TupleDescription {
	sortedColumns := make([]ColumnInfo, len(columns))
	for _, col := range columns {
		sortedColumns[col.position] = col
	}

	fieldTypes := make([]types.Type, len(sortedColumns))
	fieldNames := make([]string, len(sortedColumns))
	for i, col := range sortedColumns {
		fieldTypes[i] = col.fieldType
		fieldNames[i] = col.name
	}

	schema, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return schema
}

func (sl *SchemaLoader) loadColumnMetadata(tid *transaction.TransactionID, tableID int) ([]ColumnInfo, string, error) {
	var columns []ColumnInfo
	primaryKey := ""

	err := sl.iterateFunc(sl.columnsTableID, tid, func(columnTuple *tuple.Tuple) error {
		colTableID := getIntField(columnTuple, 0)
		if colTableID != tableID {
			return nil // Skip columns for other tables
		}

		col := ColumnInfo{
			name:      getStringField(columnTuple, 1),
			fieldType: types.Type(getIntField(columnTuple, 2)),
			position:  getIntField(columnTuple, 3),
			isPrimary: getBoolField(columnTuple, 4),
		}

		if col.isPrimary {
			primaryKey = col.name
		}

		columns = append(columns, col)
		return nil
	})

	return columns, primaryKey, err
}
