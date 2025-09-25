package join

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/tuple"
)

// JoinPredicate compares fields of two tuples using a predicate operation.
// It's used by the Join operator to determine which tuple pairs should be joined.
//
// The predicate operates on two tuples by comparing a field from the first tuple
// with a field from the second tuple using the specified comparison operation.
type JoinPredicate struct {
	field1 int               // Field index in the first (left) tuple
	field2 int               // Field index in the second (right) tuple
	op     query.PredicateOp // The comparison operation to apply
}

func NewJoinPredicate(field1, field2 int, op query.PredicateOp) (*JoinPredicate, error) {
	if field1 < 0 {
		return nil, fmt.Errorf("field1 index cannot be negative: %d", field1)
	}
	if field2 < 0 {
		return nil, fmt.Errorf("field2 index cannot be negative: %d", field2)
	}

	return &JoinPredicate{
		field1: field1,
		field2: field2,
		op:     op,
	}, nil
}

// Filter evaluates the join predicate against two tuples.
// It extracts the specified fields from each tuple and compares them using the predicate operation.
func (jp *JoinPredicate) Filter(t1, t2 *tuple.Tuple) (bool, error) {
	if t1 == nil || t2 == nil {
		return false, fmt.Errorf("tuples cannot be nil")
	}

	field1, err := t1.GetField(jp.field1)
	if err != nil {
		return false, fmt.Errorf("failed to get field %d from first tuple: %v", jp.field1, err)
	}

	field2, err := t2.GetField(jp.field2)
	if err != nil {
		return false, fmt.Errorf("failed to get field %d from second tuple: %v", jp.field2, err)
	}

	if field1 == nil || field2 == nil {
		return false, nil // Null fields don't match
	}

	typePred, err := query.GetPredicateFromOp(jp.op)
	if err != nil {
		return false, err
	}

	return field1.Compare(*typePred, field2)
}

// String returns a string representation of the join predicate for debugging.
func (jp *JoinPredicate) String() string {
	return fmt.Sprintf("JoinPredicate(field1=%d %s field2=%d)",
		jp.field1, jp.op.String(), jp.field2)
}

// GetOP returns the comparison operation of the join predicate.
func (jp *JoinPredicate) GetOP() query.PredicateOp {
	return jp.op
}

// GetField1 returns the index of the field in the first tuple used in the join predicate.
func (jp *JoinPredicate) GetField1() int {
	return jp.field1
}

// GetField2 returns the index of the field in the second tuple used in the join predicate.
func (jp *JoinPredicate) GetField2() int {
	return jp.field2
}
