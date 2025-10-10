package systemtable

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type ColumnsTable struct{}

// Schema returns the schema for the CATALOG_COLUMNS system table.
// Schema: (table_id INT, column_name STRING, type_id INT, position INT, is_primary_key BOOL, is_auto_increment BOOL, next_auto_value INT)
func (ct *ColumnsTable) Schema() *tuple.TupleDescription {
	types := []types.Type{
		types.IntType,
		types.StringType,
		types.IntType,
		types.IntType,
		types.BoolType,
		types.BoolType,
		types.IntType,
	}

	names := []string{
		"table_id",
		"column_name",
		"type_id",
		"position",
		"is_primary_key",
		"is_auto_increment",
		"next_auto_value",
	}

	desc, _ := tuple.NewTupleDesc(types, names)
	return desc
}

func (ct *ColumnsTable) ID() int {
	return ColumnsTableID
}

func (ct *ColumnsTable) TableName() string {
	return "CATALOG_COLUMNS"
}

func (ct *ColumnsTable) FileName() string {
	return "catalog_columns.dat"
}

func (ct *ColumnsTable) PrimaryKey() string {
	return ""
}
