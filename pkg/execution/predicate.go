package execution

import (
	"fmt"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// PredicateOp represents the type of comparison operation to perform in a predicate.
// These operations define how a tuple field should be compared against a constant value.
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

// String returns a human-readable string representation of the predicate operation.
// This is useful for debugging and query plan visualization.
//
// Returns:
//   - string: The string representation of the operation (e.g., "=", "<", ">", etc.)
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

// NewPredicate creates a new predicate with the specified field index, operation, and operand.
// The predicate can be used to filter tuples by comparing the specified field against
// the constant operand using the given operation.
//
// Parameters:
//   - fieldIndex: The 0-based index of the field in the tuple to compare
//   - op: The comparison operation to perform (e.g., Equals, LessThan, etc.)
//   - operand: The constant value to compare the field against
//
// Returns:
//   - *Predicate: A new predicate instance configured with the specified parameters
func NewPredicate(fieldIndex int, op PredicateOp, operand types.Field) *Predicate {
	return &Predicate{
		fieldIndex: fieldIndex,
		op:         op,
		operand:    operand,
	}
}

// Filter evaluates this predicate against a tuple to determine if it satisfies the condition.
// This method extracts the field at the specified index from the tuple and compares it
// against the predicate's operand using the specified operation.
//
// Parameters:
//   - t: The tuple to evaluate against this predicate (cannot be nil)
//
// Returns:
//   - bool: True if the tuple satisfies the predicate condition, false otherwise
//   - error: An error if the field cannot be retrieved or the comparison fails
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

// String returns a human-readable string representation of the predicate.
// Returns:
//   - string: A formatted string showing the field index, operation, and operand
//     (e.g., "field[2] > 100")
func (p *Predicate) String() string {
	return fmt.Sprintf("field[%d] %s %s", p.fieldIndex, p.op.String(), p.operand.String())
}

// GetPredicateFromOp converts a PredicateOp enum value to the corresponding types.Predicate value.
// This function bridges the execution layer predicate operations with the type system
// comparison operations.
//
// Parameters:
//   - op: The predicate operation to convert
//
// Returns:
//   - *types.Predicate: A pointer to the corresponding types.Predicate value
//   - error: An error if the operation is not supported or recognized
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
