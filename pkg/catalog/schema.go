package catalog

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

type IterFunc = func(int, *primitives.TransactionID, func(*tuple.Tuple) error) error

// SchemaLoader reconstructs table schemas from the system catalog.
// It queries CATALOG_COLUMNS to build Schema objects during database startup.
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
// It scans all column metadata for the given tableID and builds a Schema object.
func (sl *SchemaLoader) LoadTableSchema(tid *primitives.TransactionID, tableID int) (*schema.Schema, error) {
	columns, err := sl.loadColumnMetadata(tid, tableID)
	if err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %d", tableID)
	}

	schemaObj, err := schema.NewSchema(tableID, "", columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return schemaObj, nil
}

// loadColumnMetadata queries CATALOG_COLUMNS for all columns belonging to tableID.
// It filters rows by table_id and collects ColumnInfo for each matching column.
func (sl *SchemaLoader) loadColumnMetadata(tid *primitives.TransactionID, tableID int) ([]schema.ColumnMetadata, error) {
	var columns []schema.ColumnMetadata

	err := sl.iterateFunc(sl.columnsTableID, tid, func(columnTuple *tuple.Tuple) error {
		id, err := systemtable.Columns.GetTableID(columnTuple)
		if err != nil {
			return err
		}

		if id != tableID {
			return nil
		}

		col, err := systemtable.Columns.Parse(columnTuple)
		if err != nil {
			return fmt.Errorf("failed to parse column tuple: %v", err)
		}

		columns = append(columns, *col)
		return nil
	})

	return columns, err
}
