package systemtable

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type StatsTable struct {
}

// Schema returns the schema for the CATALOG_STATS system table.
func (st *StatsTable) Schema() *tuple.TupleDescription {
	fieldTypes := []types.Type{
		types.IntType,    // table_id
		types.IntType,    // cardinality
		types.IntType,    // page_count
		types.IntType,    // avg_tuple_size
		types.IntType,    // last_updated (Unix timestamp as int32)
		types.IntType,    // distinct_values
		types.IntType,    // null_count
		types.StringType, // min_value
		types.StringType, // max_value
	}

	fieldNames := []string{
		"table_id",
		"cardinality",
		"page_count",
		"avg_tuple_size",
		"last_updated",
		"distinct_values",
		"null_count",
		"min_value",
		"max_value",
	}

	desc, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return desc
}

func (st *StatsTable) FileName() string {
	return "catalog_statistics.dat"
}

func (st *StatsTable) TableName() string {
	return "CATALOG_STATISTICS"
}

func (st *StatsTable) PrimaryKey() string {
	return "table_id"
}

func (st *StatsTable) ID() int {
	return 3
}
