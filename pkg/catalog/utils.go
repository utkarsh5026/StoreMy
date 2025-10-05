package catalog

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func getIntField(tup *tuple.Tuple, index int) int {
	field, _ := tup.GetField(index)
	return int(field.(*types.IntField).Value)
}

func getStringField(tup *tuple.Tuple, index int) string {
	field, _ := tup.GetField(index)
	return field.String()
}

func getBoolField(tup *tuple.Tuple, index int) bool {
	field, _ := tup.GetField(index)
	return field.(*types.BoolField).Value
}
