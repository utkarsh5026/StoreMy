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
	fieldIndex primitives.ColumnID  // Which field in the tuple to compare (0-based index)
	op         primitives.Predicate // The comparison operation to perform
	operand    types.Field          // The constant value to compare against
}

func NewPredicate(fieldIndex primitives.ColumnID, op primitives.Predicate, operand types.Field) *Predicate {
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

// FieldIndex returns the index of the field within the tuple that this predicate operates on.
//
// Returns:
// - primitives.ColumnID: the zero-based field index to be evaluated by the predicate.
func (p *Predicate) FieldIndex() primitives.ColumnID {
	return p.fieldIndex
}

// Operation returns the comparison operation type used by this predicate (e.g., =, <, >).
//
// Returns:
// - primitives.Predicate: the comparison operator applied between the tuple field and the operand.
func (p *Predicate) Operation() primitives.Predicate {
	return p.op
}

// Value returns the constant operand this predicate compares the tuple field against.
//
// Returns:
// - types.Field: the constant value used for comparison in the predicate.
func (p *Predicate) Value() types.Field {
	return p.operand
}
