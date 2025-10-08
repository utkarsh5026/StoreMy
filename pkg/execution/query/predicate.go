package query

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// Predicate compares a tuple field to a constant value using a specified operation.
// It encapsulates the field index, comparison operation, and the constant operand
// to create a reusable filter condition for tuple evaluation.
type Predicate struct {
	fieldIndex int                  // Which field in the tuple to compare (0-based index)
	op         primitives.Predicate // The comparison operation to perform
	operand    types.Field          // The constant value to compare against
}

func NewPredicate(fieldIndex int, op primitives.Predicate, operand types.Field) *Predicate {
	return &Predicate{
		fieldIndex: fieldIndex,
		op:         op,
		operand:    operand,
	}
}

func (p *Predicate) Filter(t *tuple.Tuple) (bool, error) {
	field, err := t.GetField(p.fieldIndex)
	if err != nil {
		return false, err
	}

	if field == nil {
		return false, nil
	}

	return field.Compare(p.op, p.operand)
}

func (p *Predicate) String() string {
	return fmt.Sprintf("field[%d] %s %s", p.fieldIndex, p.op.String(), p.operand.String())
}
