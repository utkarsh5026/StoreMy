package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// IndexMetadata represents metadata for a database index
type IndexMetadata struct {
	IndexID    primitives.FileID
	IndexName  string
	TableID    primitives.FileID
	ColumnName string
	IndexType  index.IndexType
	FilePath   primitives.Filepath
	CreatedAt  time.Time
}

type IndexesTable struct {
}

// Schema returns the schema for the CATALOG_INDEXES system table.
// Schema: (index_id UINT64, index_name STRING, table_id UINT64, column_name STRING, index_type STRING, file_path STRING, created_at INT64)
func (it *IndexesTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, it.TableName()).
		AddPrimaryKey("index_id", types.Uint64Type).
		AddColumn("index_name", types.StringType).
		AddColumn("table_id", types.Uint64Type).
		AddColumn("column_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("file_path", types.StringType).
		AddColumn("created_at", types.Int64Type).
		Build()
	return sch
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
	return tuple.NewBuilder(it.Schema().TupleDesc).
		AddUint64(uint64(im.IndexID)).
		AddString(im.IndexName).
		AddUint64(uint64(im.TableID)).
		AddString(im.ColumnName).
		AddString(string(im.IndexType)).
		AddString(string(im.FilePath)).
		AddInt64(im.CreatedAt.Unix()).
		MustBuild()
}

// GetID retrieves the index ID from a tuple
func (it *IndexesTable) GetID(t *tuple.Tuple) (primitives.FileID, error) {
	if int(t.NumFields()) != it.GetNumFields() {
		return 0, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}
	return primitives.FileID(getUint64Field(t, 0)), nil
}

func (it *IndexesTable) TableIDIndex() int {
	return 2 // table_id is at position 2
}

// Parse parses a tuple into IndexMetadata
func (it *IndexesTable) Parse(t *tuple.Tuple) (*IndexMetadata, error) {
	p := tuple.NewParser(t).ExpectFields(it.GetNumFields())

	indexID := primitives.FileID(p.ReadUint64())
	indexName := p.ReadString()
	tableID := primitives.FileID(p.ReadUint64())
	columnName := p.ReadString()
	indexTypeStr := p.ReadString()
	filePath := p.ReadString()
	createdAt := p.ReadTimestamp()

	if err := p.Error(); err != nil {
		return nil, err
	}

	if indexID == 0 {
		return nil, fmt.Errorf("invalid index_id %d: must be positive", indexID)
	}

	if indexName == "" {
		return nil, fmt.Errorf("index_name cannot be empty")
	}

	if tableID == InvalidTableID {
		return nil, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
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
		FilePath:   primitives.Filepath(filePath),
		CreatedAt:  createdAt,
	}, nil
}
