package catalog

import (
	"fmt"
	"slices"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

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
func buildTupleDescription(columns []systemtable.ColumnInfo) *tuple.TupleDescription {
	sortedCols := slices.Clone(columns)
	slices.SortFunc(sortedCols, func(a, b systemtable.ColumnInfo) int {
		return a.Position - b.Position
	})

	fieldTypes := make([]types.Type, len(sortedCols))
	fieldNames := make([]string, len(sortedCols))
	for i, col := range sortedCols {
		fieldTypes[i] = col.FieldType
		fieldNames[i] = col.Name
	}

	schema, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return schema
}

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
// It filters rows by table_id and collects ColumnInfo for each matching column.
func (sl *SchemaLoader) loadColumnMetadata(tid *primitives.TransactionID, tableID int) ([]systemtable.ColumnInfo, string, error) {
	var columns []systemtable.ColumnInfo
	primaryKey := ""

	err := sl.iterateFunc(sl.columnsTableID, tid, func(columnTuple *tuple.Tuple) error {
		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return fmt.Errorf("failed to parse column tuple: %v", err)
		}

		if col.IsPrimary {
			primaryKey = col.Name
		}

		columns = append(columns, *col)
		return nil
	})

	return columns, primaryKey, err
}
