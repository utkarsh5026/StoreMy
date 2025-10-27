package common

import (
	"fmt"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

const (
	DefaultHighCost = 1e6 // Arbitrary high cost for unsupported scenarios
)

// JoinAlgorithm defines the interface for all join implementations
type JoinAlgorithm interface {
	// Initialize prepares the join algorithm for execution
	Initialize() error

	// Next returns the next joined tuple or nil if no more tuples
	Next() (*tuple.Tuple, error)

	// Reset resets the join algorithm to start from the beginning
	Reset() error

	// Close releases resources used by the join algorithm
	Close() error

	// EstimateCost returns an estimated cost for this join algorithm
	EstimateCost() float64

	// SupportsPredicateType checks if this algorithm can handle the given predicate
	SupportsPredicateType(predicate JoinPredicate) bool
}

// JoinStatistics holds statistics about input relations for cost estimation
type JoinStatistics struct {
	LeftCardinality  int     // Number of tuples in left relation
	RightCardinality int     // Number of tuples in right relation
	LeftSize         int     // Size in pages of left relation
	RightSize        int     // Size in pages of right relation
	LeftSorted       bool    // Whether left input is sorted on join key
	RightSorted      bool    // Whether right input is sorted on join key
	MemorySize       int     // Available memory in pages
	Selectivity      float64 // Estimated join selectivity
}

// DefaultJoinStatistics returns default statistics for joins
func DefaultJoinStatistics() *JoinStatistics {
	return &JoinStatistics{
		LeftCardinality:  1000,
		RightCardinality: 1000,
		LeftSize:         10,
		RightSize:        10,
		MemorySize:       100,
		Selectivity:      0.1,
	}
}

// JoinPredicate compares fields of two tuples using a predicate operation.
// It's used by the Join operator to determine which tuple pairs should be joined.
//
// The predicate operates on two tuples by comparing a field from the first tuple
// with a field from the second tuple using the specified comparison operation.
type JoinPredicate interface {
	// Filter evaluates the join predicate against two tuples
	Filter(t1, t2 *tuple.Tuple) (bool, error)

	// GetOP returns the comparison operation of the join predicate
	GetOP() primitives.Predicate

	// GetField1 returns the index of the field in the first tuple
	GetLeftField() primitives.ColumnID

	// GetField2 returns the index of the field in the second tuple
	GetRightField() primitives.ColumnID

	// String returns a string representation for debugging
	String() string
}

// joinPredicate is the internal implementation of JoinPredicate
type joinPredicate struct {
	field1 primitives.ColumnID  // Field index in the first (left) tuple
	field2 primitives.ColumnID  // Field index in the second (right) tuple
	op     primitives.Predicate // The comparison operation to apply
}

// NewJoinPredicate creates a new join predicate
func NewJoinPredicate(field1, field2 primitives.ColumnID, op primitives.Predicate) (JoinPredicate, error) {
	return &joinPredicate{
		field1: field1,
		field2: field2,
		op:     op,
	}, nil
}

// Filter evaluates the join predicate against two tuples.
// It extracts the specified fields from each tuple and compares them using the predicate operation.
func (jp *joinPredicate) Filter(t1, t2 *tuple.Tuple) (bool, error) {
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

	return field1.Compare(jp.op, field2)
}

// String returns a string representation of the join predicate for debugging.
func (jp *joinPredicate) String() string {
	return fmt.Sprintf("JoinPredicate(field1=%d %s field2=%d)",
		jp.field1, jp.op.String(), jp.field2)
}

// GetOP returns the comparison operation of the join predicate.
func (jp *joinPredicate) GetOP() primitives.Predicate {
	return jp.op
}

// GetField1 returns the index of the field in the first tuple used in the join predicate.
func (jp *joinPredicate) GetLeftField() primitives.ColumnID {
	return jp.field1
}

// GetRightField returns the index of the field in the second tuple used in the join predicate.
func (jp *joinPredicate) GetRightField() primitives.ColumnID {
	return jp.field2
}
