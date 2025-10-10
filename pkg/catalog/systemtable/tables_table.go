package systemtable

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type TablesTable struct {
}

// Schema returns the schema for the CATALOG_TABLES system table.
// Schema: (table_id INT, table_name STRING, file_path STRING, primary_key STRING)
func (tt *TablesTable) Schema() *tuple.TupleDescription {
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

func (tt *TablesTable) ID() int {
	return TablesTableID
}

func (tt *TablesTable) TableName() string {
	return "CATALOG_TABLES"
}

func (tt *TablesTable) FileName() string {
	return "catalog_tables.dat"
}

func (tt *TablesTable) PrimaryKey() string {
	return "table_id"
}

func (tt *TablesTable) CreateTuple(tableID int, tableName, filePath, primaryKey string) *tuple.Tuple {
	t := tuple.NewTuple(tt.Schema())
	t.SetField(0, types.NewIntField(int64(tableID)))
	t.SetField(1, types.NewStringField(tableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(filePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(primaryKey, types.StringMaxSize))
	return t
}
