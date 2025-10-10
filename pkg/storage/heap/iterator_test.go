package heap

import (
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

func mustCreateTupleDesc() *tuple.TupleDescription {
	types := []types.Type{types.IntType, types.StringType}
	fields := []string{"id", "name"}
	td, err := tuple.NewTupleDesc(types, fields)
	if err != nil {
		panic(err)
	}
	return td
}

func createTestTuple(td *tuple.TupleDescription, id int64, name string) *tuple.Tuple {
	t := tuple.NewTuple(td)
	t.SetField(0, types.NewIntField(id))
	t.SetField(1, types.NewStringField(name, 128))
	return t
}
