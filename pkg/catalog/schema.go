package catalog

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

const (
	// TablesTableID is the reserved heap file ID for the CATALOG_TABLES system table
	TablesTableID = 0
	// ColumnsTableID is the reserved heap file ID for the CATALOG_COLUMNS system table
	ColumnsTableID = 1
)

// GetTablesSchema returns the schema for the CATALOG_TABLES system table.
// Schema: (table_id INT, table_name STRING, file_path STRING, primary_key STRING)
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

// GetColumnsSchema returns the schema for the CATALOG_COLUMNS system table.
// Schema: (table_id INT, column_name STRING, type_id INT, position INT, is_primary_key BOOL)
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

// ColumnInfo represents metadata for a single column during schema reconstruction.
// Used internally by SchemaLoader to build TupleDescription from catalog data.
type ColumnInfo struct {
	name      string
	fieldType types.Type
	position  int
	isPrimary bool
}

type IterFunc = func(int, *primitives.TransactionID, func(*tuple.Tuple) error) error

// SchemaLoader reconstructs table schemas from the system catalog.
// It queries CATALOG_COLUMNS to build TupleDescription objects during database startup.
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

// LoadTableSchema reconstructs the schema for a table from CATALOG_COLUMNS.
// It scans all column metadata for the given tableID, sorts by position,
// and builds a TupleDescription.
func (sl *SchemaLoader) LoadTableSchema(tid *primitives.TransactionID, tableID int) (*tuple.TupleDescription, string, error) {
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

// buildTupleDescription converts sorted ColumnInfo slice into a TupleDescription.
// Columns are sorted by their position field to ensure correct tuple layout.
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

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
// It filters rows by table_id and collects ColumnInfo for each matching column.
func (sl *SchemaLoader) loadColumnMetadata(tid *primitives.TransactionID, tableID int) ([]ColumnInfo, string, error) {
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
