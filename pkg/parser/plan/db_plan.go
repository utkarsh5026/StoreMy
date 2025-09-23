package plan

import (
	"fmt"
	"storemy/pkg/types"
)

type DbPlan struct{}

type FilterNode struct {
	Table     string          // Table alias the filter applies to
	Field     string          // Field name being filtered
	Predicate types.Predicate // Comparison operator
	Constant  string          // Constant value to compare against
}

func NewFilterNode(table, field string, predicate types.Predicate, constant string) *FilterNode {
	return &FilterNode{
		Table:     table,
		Field:     field,
		Predicate: predicate,
		Constant:  constant,
	}
}

func (fn *FilterNode) String() string {
	return fmt.Sprintf("Filter[%s.%s %s %s]", fn.Table, fn.Field, fn.Predicate, fn.Constant)
}
