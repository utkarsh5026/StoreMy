package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// IndexMetadata represents metadata for a database index
type IndexMetadata struct {
	IndexID    int
	IndexName  string
	TableID    int
	ColumnName string
	IndexType  index.IndexType
	FilePath   string
	CreatedAt  int64 // Unix timestamp
}

type IndexesTable struct {
}

// Schema returns the schema for the CATALOG_INDEXES system table.
// Schema: (index_id INT, index_name STRING, table_id INT, column_name STRING, index_type STRING, file_path STRING, created_at INT)
func (it *IndexesTable) Schema() *schema.Schema {
	return schema.NewSchemaBuilder(InvalidTableID, it.TableName()).
		AddPrimaryKey("index_id", types.IntType).
		AddColumn("index_name", types.StringType).
		AddColumn("table_id", types.IntType).
		AddColumn("column_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("file_path", types.StringType).
		AddColumn("created_at", types.IntType).
		Build()
}

func (it *IndexesTable) TableName() string {
	return "CATALOG_INDEXES"
}

func (it *IndexesTable) FileName() string {
	return "catalog_indexes.dat"
}

func (it *IndexesTable) PrimaryKey() string {
	return "index_id"
}

func (it *IndexesTable) GetNumFields() int {
	return 7
}

// CreateTuple creates a tuple from IndexMetadata
func (it *IndexesTable) CreateTuple(im IndexMetadata) *tuple.Tuple {
	t := tuple.NewTuple(it.Schema().TupleDesc)
	t.SetField(0, types.NewIntField(int64(im.IndexID)))
	t.SetField(1, types.NewStringField(im.IndexName, types.StringMaxSize))
	t.SetField(2, types.NewIntField(int64(im.TableID)))
	t.SetField(3, types.NewStringField(im.ColumnName, types.StringMaxSize))
	t.SetField(4, types.NewStringField(string(im.IndexType), types.StringMaxSize))
	t.SetField(5, types.NewStringField(im.FilePath, types.StringMaxSize))
	t.SetField(6, types.NewIntField(im.CreatedAt))
	return t
}

// GetID retrieves the index ID from a tuple
func (it *IndexesTable) GetID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != it.GetNumFields() {
		return -1, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}
	return getIntField(t, 0), nil
}

func (it *IndexesTable) TableIDIndex() int {
	return 2 // table_id is at position 2
}

// Parse parses a tuple into IndexMetadata
func (it *IndexesTable) Parse(t *tuple.Tuple) (*IndexMetadata, error) {
	if t.TupleDesc.NumFields() != it.GetNumFields() {
		return nil, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}

	indexID := getIntField(t, 0)
	indexName := getStringField(t, 1)
	tableID := getIntField(t, 2)
	columnName := getStringField(t, 3)
	indexTypeStr := getStringField(t, 4)
	filePath := getStringField(t, 5)
	createdAt := getInt64Field(t, 6)

	if indexID <= 0 {
		return nil, fmt.Errorf("invalid index_id %d: must be positive", indexID)
	}

	if indexName == "" {
		return nil, fmt.Errorf("index_name cannot be empty")
	}

	if tableID <= 0 {
		return nil, fmt.Errorf("invalid table_id %d: must be positive", tableID)
	}

	if columnName == "" {
		return nil, fmt.Errorf("column_name cannot be empty")
	}

	indexType := index.IndexType(indexTypeStr)
	if indexType != index.HashIndex && indexType != index.BTreeIndex {
		return nil, fmt.Errorf("invalid index_type %s: must be HASH or BTREE", indexTypeStr)
	}

	if filePath == "" {
		return nil, fmt.Errorf("file_path cannot be empty")
	}

	return &IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexName,
		TableID:    tableID,
		ColumnName: columnName,
		IndexType:  indexType,
		FilePath:   filePath,
		CreatedAt:  createdAt,
	}, nil
}
