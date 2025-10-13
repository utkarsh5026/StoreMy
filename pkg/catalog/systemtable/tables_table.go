package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
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
func (tt *TablesTable) Schema() *schema.Schema {
	return schema.NewSchemaBuilder(InvalidTableID, tt.TableName()).
		AddPrimaryKey("table_id", types.IntType).
		AddColumn("table_name", types.StringType).
		AddColumn("file_path", types.StringType).
		AddColumn("primary_key", types.StringType).
		Build()
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

func (tt *TablesTable) GetNumFields() int {
	return 4
}

func (tt *TablesTable) CreateTuple(tm TableMetadata) *tuple.Tuple {
	t := tuple.NewTuple(tt.Schema().TupleDesc)
	t.SetField(0, types.NewIntField(int64(tm.TableID)))
	t.SetField(1, types.NewStringField(tm.TableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(tm.FilePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(tm.PrimaryKeyCol, types.StringMaxSize))
	return t
}

func (tt *TablesTable) GetID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != tt.GetNumFields() {
		return -1, fmt.Errorf("invalid tuple: expected 4 fields, got %d", t.TupleDesc.NumFields())
	}
	return getIntField(t, 0), nil
}

func (tt *TablesTable) TableIDIndex() int {
	return 0
}

func (tt *TablesTable) Parse(t *tuple.Tuple) (*TableMetadata, error) {
	if t.TupleDesc.NumFields() != tt.GetNumFields() {
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
