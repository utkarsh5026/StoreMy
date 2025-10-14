package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type ColumnsTable struct{}

// Schema returns the schema for the CATALOG_COLUMNS system table.
// Schema: (table_id INT, column_name STRING, type_id INT, position INT, is_primary_key BOOL, is_auto_increment BOOL, next_auto_value INT)
func (ct *ColumnsTable) Schema() *schema.Schema {
	return schema.NewSchemaBuilder(InvalidTableID, ct.TableName()).
		AddColumn("table_id", types.IntType).
		AddColumn("column_name", types.StringType).
		AddColumn("type_id", types.IntType).
		AddColumn("position", types.IntType).
		AddColumn("is_primary_key", types.BoolType).
		AddColumn("is_auto_increment", types.BoolType).
		AddColumn("next_auto_value", types.IntType).
		Build()
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

func (ct *ColumnsTable) TableIDIndex() int {
	return 0
}

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

func (ct *ColumnsTable) GetTableID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != 7 {
		return 0, fmt.Errorf("invalid tuple: expected 7 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	if tableID <= 0 {
		return 0, fmt.Errorf("invalid table_id %d: must be positive", tableID)
	}

	return tableID, nil
}

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

// UpdateAutoIncrementValue creates a new tuple with updated auto-increment value
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
type ColumnMatch struct {
	Tuple        *tuple.Tuple
	ColumnType   types.Type
	Position     int
	IsPrimary    bool
	IsAutoInc    bool
	CurrentValue int
}

// FindLatestAutoIncrementColumn finds the latest version (highest next_auto_value) of an auto-increment column
// This is MVCC-aware and handles multiple versions of the same column
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
