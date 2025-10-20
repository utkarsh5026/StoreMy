package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/iterator"
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
	t := tuple.NewTuple(ct.Schema().TupleDesc)
	t.SetField(0, types.NewIntField(int64(col.TableID)))
	t.SetField(1, types.NewStringField(col.Name, types.StringMaxSize))
	t.SetField(2, types.NewIntField(int64(col.FieldType)))
	t.SetField(3, types.NewIntField(int64(col.Position)))
	t.SetField(4, types.NewBoolField(col.IsPrimary))
	t.SetField(5, types.NewBoolField(col.IsAutoInc))
	t.SetField(6, types.NewIntField(1)) // Start auto-increment at 1
	return t
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
	tableID, err := ct.GetTableID(t)
	if err != nil {
		return nil, err
	}

	name := getStringField(t, 1)
	typeID := getIntField(t, 2)
	position := getIntField(t, 3)
	isPrimary := getBoolField(t, 4)
	isAutoInc := getBoolField(t, 5)
	nextAutoValue := getIntField(t, 6)

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

// ColumnMatch represents the result of finding an auto-increment column
// with its current state and metadata. Used to coordinate auto-increment
// value generation during INSERT operations.
type ColumnMatch struct {
	Tuple                *tuple.Tuple
	ColumnType           types.Type
	Position             int
	IsPrimary, IsAutoInc bool
	CurrentValue         int // Current next_auto_value from catalog
}

// FindLatestAutoIncrementColumn finds the latest version (highest next_auto_value) of an auto-increment column.
// This is MVCC-aware and handles multiple versions of the same column that may exist due to concurrent
// transactions updating the auto-increment counter.
//
// Parameters:
//   - iter: TupleIterator over CATALOG_COLUMNS (may contain multiple versions)
//   - tableID: The table to search within
//   - columnName: The column name to find
//
// Returns the ColumnMatch with the highest next_auto_value, or nil if no matching column found.
// This ensures we always use the most recent auto-increment state even with concurrent INSERTs.
func (ct *ColumnsTable) FindLatestAutoIncrementColumn(iter iterator.TupleIterator, tableID int, columnName string) (*ColumnMatch, error) {
	var result *ColumnMatch
	maxValue := -1

	for {
		hasNext, err := iter.HasNext()
		if err != nil || !hasNext {
			break
		}

		tup, err := iter.Next()
		if err != nil || tup == nil {
			break
		}

		colTableID := getIntField(tup, 0)
		colName := getStringField(tup, 1)

		if colTableID == tableID && colName == columnName {
			currentValue := getIntField(tup, 6)
			if currentValue > maxValue {
				result = &ColumnMatch{
					Tuple:        tup,
					ColumnType:   types.Type(getIntField(tup, 2)),
					Position:     getIntField(tup, 3),
					IsPrimary:    getBoolField(tup, 4),
					IsAutoInc:    getBoolField(tup, 5),
					CurrentValue: currentValue,
				}
				maxValue = currentValue
			}
		}
	}

	return result, nil
}
