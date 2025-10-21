package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// ColumnsTable is a system catalog table that stores metadata about all columns
// in the database. Each row represents one column definition with its properties
// including type, position, and auto-increment state.
//
// This table is part of StoreMy's metadata system and is used by the TableManager
// to reconstruct schema definitions during database initialization and query planning.
type ColumnsTable struct{}

// Schema returns the schema for the CATALOG_COLUMNS system table.
// Schema: (table_id INT, column_name STRING, type_id INT, position INT, is_primary_key BOOL, is_auto_increment BOOL, next_auto_value INT)
//
// Column descriptions:
//   - table_id: References the table this column belongs to (from CATALOG_TABLES)
//   - column_name: Name of the column as used in SQL queries
//   - type_id: Type identifier from pkg/types (IntType, StringType, BoolType, FloatType)
//   - position: Zero-based column position in the table schema
//   - is_primary_key: True if this column is part of the primary key
//   - is_auto_increment: True if this column auto-generates values on INSERT
//   - next_auto_value: Next value to use for auto-increment (>=1 when is_auto_increment=true)
func (ct *ColumnsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, ct.TableName()).
		AddColumn("table_id", types.IntType).
		AddColumn("column_name", types.StringType).
		AddColumn("type_id", types.IntType).
		AddColumn("position", types.IntType).
		AddColumn("is_primary_key", types.BoolType).
		AddColumn("is_auto_increment", types.BoolType).
		AddColumn("next_auto_value", types.IntType).
		Build()

	return sch
}

// TableName returns the canonical name for the columns system catalog table.
func (ct *ColumnsTable) TableName() string {
	return "CATALOG_COLUMNS"
}

// FileName returns the heap file name where column metadata is persisted.
func (ct *ColumnsTable) FileName() string {
	return "catalog_columns.dat"
}

// PrimaryKey returns an empty string as CATALOG_COLUMNS uses table_id + column_name
// as a composite key rather than a single-column primary key.
func (ct *ColumnsTable) PrimaryKey() string {
	return ""
}

// TableIDIndex returns the field index (0) where table_id is stored in tuples.
// Used by catalog operations to filter columns by table.
func (ct *ColumnsTable) TableIDIndex() int {
	return 0
}

// CreateTuple constructs a new catalog tuple from column metadata.
// Auto-increment columns are initialized with next_auto_value=1.
func (ct *ColumnsTable) CreateTuple(col schema.ColumnMetadata) *tuple.Tuple {
	return tuple.NewBuilder(ct.Schema().TupleDesc).
		AddInt(int64(col.TableID)).
		AddString(col.Name).
		AddInt(int64(col.FieldType)).
		AddInt(int64(col.Position)).
		AddBool(col.IsPrimary).
		AddBool(col.IsAutoInc).
		AddInt(int64(col.NextAutoValue)). // Start auto-increment at 1
		MustBuild()
}

// GetTableID extracts and validates the table_id from a catalog tuple.
// Returns an error if the tuple structure is invalid or table_id is InvalidTableID (-1).
func (ct *ColumnsTable) GetTableID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != 7 {
		return 0, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	if tableID == InvalidTableID {
		return 0, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	return tableID, nil
}

// Parse converts a catalog tuple into a ColumnMetadata struct with full validation.
// Validates:
//   - table_id is not InvalidTableID
//   - column name is non-empty
//   - type_id is a recognized Type from pkg/types
//   - position is non-negative
//   - auto-increment columns are INT type with next_auto_value >= 1
func (ct *ColumnsTable) Parse(t *tuple.Tuple) (*schema.ColumnMetadata, error) {
	p := tuple.NewParser(t).ExpectFields(7)

	tableID := p.ReadInt()
	name := p.ReadString()
	typeID := p.ReadInt()
	position := p.ReadInt()
	isPrimary := p.ReadBool()
	isAutoInc := p.ReadBool()
	nextAutoValue := p.ReadInt()

	if err := p.Error(); err != nil {
		return nil, err
	}

	if tableID == InvalidTableID {
		return nil, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	if name == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}

	fieldType := types.Type(typeID)
	if !types.IsValidType(fieldType) {
		return nil, fmt.Errorf("invalid type_id %d: not a recognized type", typeID)
	}

	if isLessThanZero(position) {
		return nil, fmt.Errorf("invalid column position %d: must be non-negative", position)
	}

	if isAutoInc {
		if fieldType != types.IntType {
			return nil, fmt.Errorf("auto-increment column must be INT type, got type_id %d", typeID)
		}

		if isLessThan(nextAutoValue, 1) {
			return nil, fmt.Errorf("invalid next_auto_value %d: must be >= 1", nextAutoValue)
		}
	}

	col := &schema.ColumnMetadata{
		Name:          name,
		FieldType:     fieldType,
		Position:      position,
		IsPrimary:     isPrimary,
		IsAutoInc:     isAutoInc,
		NextAutoValue: nextAutoValue,
		TableID:       tableID,
	}

	return col, nil
}

// UpdateAutoIncrementValue creates a new tuple with updated next_auto_value field.
// This is used during INSERT operations to persist the next available auto-increment value.
// The new tuple preserves all other fields from oldTuple and only modifies field index 6.
func (ct *ColumnsTable) UpdateAutoIncrementValue(oldTuple *tuple.Tuple, newValue int) *tuple.Tuple {
	newTuple := tuple.NewTuple(ct.Schema().TupleDesc)
	for i := range 6 {
		field, _ := oldTuple.GetField(i)
		newTuple.SetField(i, field)
	}
	newTuple.SetField(6, types.NewIntField(int64(newValue)))
	return newTuple
}
