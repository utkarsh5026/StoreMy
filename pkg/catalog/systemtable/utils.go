package systemtable

import (
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func getIntField(tup *tuple.Tuple, index primitives.ColumnID) int {
	field, _ := tup.GetField(index)
	return int(field.(*types.IntField).Value)
}

func getUint64Field(tup *tuple.Tuple, index primitives.ColumnID) uint64 {
	field, _ := tup.GetField(index)
	return field.(*types.Uint64Field).Value
}
