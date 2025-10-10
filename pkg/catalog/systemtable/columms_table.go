package systemtable

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// ColumnInfo represents metadata for a single column during schema reconstruction.
// Used internally by SchemaLoader to build TupleDescription from catalog data.
type ColumnInfo struct {
	TableID       int
	Name          string
	FieldType     types.Type
	Position      int
	IsPrimary     bool
	IsAutoInc     bool
	NextAutoValue int
}

type ColumnsTable struct{}

// Schema returns the schema for the CATALOG_COLUMNS system table.
// Schema: (table_id INT, column_name STRING, type_id INT, position INT, is_primary_key BOOL, is_auto_increment BOOL, next_auto_value INT)
func (ct *ColumnsTable) Schema() *tuple.TupleDescription {
	types := []types.Type{
		types.IntType,
		types.StringType,
		types.IntType,
		types.IntType,
		types.BoolType,
		types.BoolType,
		types.IntType,
	}

	names := []string{
		"table_id",
		"column_name",
		"type_id",
		"position",
		"is_primary_key",
		"is_auto_increment",
		"next_auto_value",
	}

	desc, _ := tuple.NewTupleDesc(types, names)
	return desc
}

func (ct *ColumnsTable) ID() int {
	return ColumnsTableID
}

func (ct *ColumnsTable) TableName() string {
	return "CATALOG_COLUMNS"
}

func (ct *ColumnsTable) FileName() string {
	return "catalog_columns.dat"
}

func (ct *ColumnsTable) PrimaryKey() string {
	return ""
}

func (ct *ColumnsTable) CreateTuple(tableID int64, colName string, colType types.Type, position int64, isPrimary bool, isAutoInc bool) *tuple.Tuple {
	t := tuple.NewTuple(ct.Schema())
	t.SetField(0, types.NewIntField(tableID))
	t.SetField(1, types.NewStringField(colName, types.StringMaxSize))
	t.SetField(2, types.NewIntField(int64(colType)))
	t.SetField(3, types.NewIntField(position))
	t.SetField(4, types.NewBoolField(isPrimary))
	t.SetField(5, types.NewBoolField(isAutoInc))
	t.SetField(6, types.NewIntField(1)) // Start auto-increment at 1
	return t
}

func (ct *ColumnsTable) Parse(t *tuple.Tuple) (*ColumnInfo, error) {
	if t.TupleDesc.NumFields() != 7 {
		return nil, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
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

	col := &ColumnInfo{
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
