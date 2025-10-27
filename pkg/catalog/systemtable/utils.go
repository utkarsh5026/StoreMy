package systemtable

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func getIntField(tup *tuple.Tuple, index int) int {
	field, _ := tup.GetField(index)
	return int(field.(*types.IntField).Value)
}

func getInt64Field(tup *tuple.Tuple, index int) int64 {
	field, _ := tup.GetField(index)
	return field.(*types.IntField).Value
}

func getStringField(tup *tuple.Tuple, index int) string {
	field, _ := tup.GetField(index)
	return field.String()
}

func getBoolField(tup *tuple.Tuple, index int) bool {
	field, _ := tup.GetField(index)
	return field.(*types.BoolField).Value
}

func getUint32Field(tup *tuple.Tuple, index int) uint32 {
	field, _ := tup.GetField(index)
	return field.(*types.Uint32Field).Value
}

func getUint64Field(tup *tuple.Tuple, index int) uint64 {
	field, _ := tup.GetField(index)
	return field.(*types.Uint64Field).Value
}

func isLessThan(a int, b int) bool {
	return a < b
}

func isLessThanZero(a int) bool {
	return isLessThan(a, 0)
}
