package catalog

import (
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
