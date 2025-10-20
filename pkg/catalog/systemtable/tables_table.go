package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// TableMetadata holds persisted metadata for a single table recorded in the system catalog.
// It is used by TableManager to rebuild in-memory catalog state during database startup.
type TableMetadata struct {
	TableID       int    // Unique numeric identifier for the table
	TableName     string // Canonical table name used in SQL
	FilePath      string // Heap file name where the table data is stored
	PrimaryKeyCol string // Name of the primary key column (empty if none or composite)
}

// TablesTable provides accessors and helpers for the CATALOG_TABLES system table.
// Each row in CATALOG_TABLES maps a table id to its storage file and primary key.
type TablesTable struct {
}

// Schema returns the schema for the CATALOG_TABLES system table.
// Schema layout:
//
//	(table_id INT PRIMARY KEY, table_name STRING, file_path STRING, primary_key STRING)
//
// Notes:
//   - table_id is the primary key for the system table and must be unique.
//   - file_path is the on-disk heap file name used by the storage engine for this table.
//   - primary_key is the column name used as primary key; empty string denotes none or composite keys recorded elsewhere.
func (tt *TablesTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, tt.TableName()).
		AddPrimaryKey("table_id", types.IntType).
		AddColumn("table_name", types.StringType).
		AddColumn("file_path", types.StringType).
		AddColumn("primary_key", types.StringType).
		Build()
	return sch
}

// TableName returns the canonical name of the system table.
func (tt *TablesTable) TableName() string {
	return "CATALOG_TABLES"
}

// FileName returns the filename used to persist the CATALOG_TABLES heap.
func (tt *TablesTable) FileName() string {
	return "catalog_tables.dat"
}

// PrimaryKey returns the primary key field name in the schema.
func (tt *TablesTable) PrimaryKey() string {
	return "table_id"
}

// GetNumFields returns the number of fields in the CATALOG_TABLES schema.
func (tt *TablesTable) GetNumFields() int {
	return 4
}

// CreateTuple constructs a catalog tuple for a given TableMetadata.
// Fields are populated in schema order: table_id, table_name, file_path, primary_key.
func (tt *TablesTable) CreateTuple(tm TableMetadata) *tuple.Tuple {
	t := tuple.NewTuple(tt.Schema().TupleDesc)
	t.SetField(0, types.NewIntField(int64(tm.TableID)))
	t.SetField(1, types.NewStringField(tm.TableName, types.StringMaxSize))
	t.SetField(2, types.NewStringField(tm.FilePath, types.StringMaxSize))
	t.SetField(3, types.NewStringField(tm.PrimaryKeyCol, types.StringMaxSize))
	return t
}

// GetID extracts the table_id from a catalog tuple and validates tuple arity.
// Returns an error when the tuple does not match the expected schema length.
func (tt *TablesTable) GetID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != tt.GetNumFields() {
		return -1, fmt.Errorf("invalid tuple: expected 4 fields, got %d", t.TupleDesc.NumFields())
	}
	return getIntField(t, 0), nil
}

// TableIDIndex returns the field index where table_id is stored.
func (tt *TablesTable) TableIDIndex() int {
	return 0
}

// Parse converts a catalog tuple into a TableMetadata instance with validation.
// Validation performed:
//   - Tuple arity matches expected schema length.
//   - table_id is not InvalidTableID (reserved).
//   - table_name and file_path are non-empty strings.
//
// Returns parsed TableMetadata or an error if validation fails.
func (tt *TablesTable) Parse(t *tuple.Tuple) (*TableMetadata, error) {
	if t.TupleDesc.NumFields() != tt.GetNumFields() {
		return nil, fmt.Errorf("invalid tuple: expected 4 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	tableName := getStringField(t, 1)
	filePath := getStringField(t, 2)
	primaryKey := getStringField(t, 3)

	// table_id must not be the special InvalidTableID reserved for system schema definitions.
	if tableID == InvalidTableID {
		return nil, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
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
