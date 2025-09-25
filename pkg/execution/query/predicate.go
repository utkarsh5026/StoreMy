package query

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

type PredicateOp int

const (
	Equals PredicateOp = iota
	LessThan
	GreaterThan
	LessThanOrEqual
	GreaterThanOrEqual
	NotEqual
	Like
)

func (op PredicateOp) String() string {
	switch op {
	case Equals:
		return "="
	case LessThan:
		return "<"
	case GreaterThan:
		return ">"
	case LessThanOrEqual:
		return "<="
	case GreaterThanOrEqual:
		return ">="
	case NotEqual:
		return "!="
	case Like:
		return "LIKE"
	default:
		return "UNKNOWN"
	}
}

// Predicate compares a tuple field to a constant value using a specified operation.
// It encapsulates the field index, comparison operation, and the constant operand
// to create a reusable filter condition for tuple evaluation.
type Predicate struct {
	fieldIndex int         // Which field in the tuple to compare (0-based index)
	op         PredicateOp // The comparison operation to perform
	operand    types.Field // The constant value to compare against
}

func NewPredicate(fieldIndex int, op PredicateOp, operand types.Field) *Predicate {
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

	typePred, err := GetPredicateFromOp(p.op)
	if err != nil {
		return false, err
	}
	return field.Compare(*typePred, p.operand)
}

func (p *Predicate) String() string {
	return fmt.Sprintf("field[%d] %s %s", p.fieldIndex, p.op.String(), p.operand.String())
}

func GetPredicateFromOp(op PredicateOp) (*types.Predicate, error) {
	var typePred types.Predicate
	switch op {
	case Equals:
		typePred = types.Equals
	case LessThan:
		typePred = types.LessThan
	case GreaterThan:
		typePred = types.GreaterThan
	case LessThanOrEqual:
		typePred = types.LessThanOrEqual
	case GreaterThanOrEqual:
		typePred = types.GreaterThanOrEqual
	case NotEqual:
		typePred = types.NotEqual
	case Like:
		typePred = types.Like
	default:
		return nil, fmt.Errorf("unsupported predicate operation: %v", op)
	}
	return &typePred, nil
}
