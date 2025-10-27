package join

import (
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
)

// JoinAlgorithm defines the interface for all join implementations
// Re-export from internal for extensibility
type JoinAlgorithm = common.JoinAlgorithm

// JoinStatistics holds statistics about input relations for cost estimation
// Re-export from internal for public API
type JoinStatistics = common.JoinStatistics

// JoinPredicate compares fields of two tuples using a predicate operation.
// Re-export from internal for public API
type JoinPredicate = common.JoinPredicate

// NewJoinPredicate creates a new join predicate
func NewJoinPredicate(field1, field2 primitives.ColumnID, op primitives.Predicate) (JoinPredicate, error) {
	return common.NewJoinPredicate(field1, field2, op)
}

// Predicate is a function type that can be used as a join condition
type Predicate func(t1, t2 *tuple.Tuple) (bool, error)
