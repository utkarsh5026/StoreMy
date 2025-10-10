package systemtable

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type TableMetadata struct {
	TableID       int
	TableName     string
	FilePath      string
	PrimaryKeyCol string
}

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

func (tt *TablesTable) CreateTuple(tm TableMetadata) *tuple.Tuple {
	t := tuple.NewTuple(tt.Schema())
	t.SetField(0, types.NewIntField(int64(tm.TableID)))
	t.SetField(1, types.NewStringField(tm.TableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(tm.FilePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(tm.PrimaryKeyCol, types.StringMaxSize))
	return t
}

func (tt *TablesTable) Parse(t *tuple.Tuple) (*TableMetadata, error) {
	if t.TupleDesc.NumFields() != 4 {
		return nil, fmt.Errorf("invalid tuple: expected 4 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	tableName := getStringField(t, 1)
	filePath := getStringField(t, 2)
	primaryKey := getStringField(t, 3)

	if tableID <= 0 {
		return nil, fmt.Errorf("invalid table_id %d: must be positive", tableID)
	}

	if tableName == "" {
		return nil, fmt.Errorf("table_name cannot be empty")
	}

	if filePath == "" {
		return nil, fmt.Errorf("file_path cannot be empty")
	}

	return &TableMetadata{
		TableID:       tableID,
		TableName:     tableName,
		FilePath:      filePath,
		PrimaryKeyCol: primaryKey,
	}, nil
}
