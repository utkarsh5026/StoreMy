package operations

import (
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// createColumnMetadata creates a column metadata tuple for testing with all options
func createColumnMetadata(tableID int, name string, position int, fieldType types.Type, isPrimary, isAutoInc bool, nextAutoValue int) *tuple.Tuple {
	col := schema.ColumnMetadata{
		TableID:       tableID,
		Name:          name,
		Position:      position,
		FieldType:     fieldType,
		IsPrimary:     isPrimary,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
	}
	return systemtable.Columns.CreateTuple(col)
}

// setupTableColumns adds column metadata to the mock for a table
func setupTableColumns(mock *mockCatalogAccess, columnsTableID int, tableID int, columns []struct {
	name      string
	fieldType types.Type
	isPrimary bool
}) {
	for i, col := range columns {
		colTuple := createColumnMetadata(tableID, col.name, i, col.fieldType, col.isPrimary, false, 0)
		mock.tuples[columnsTableID] = append(mock.tuples[columnsTableID], colTuple)
	}
}
