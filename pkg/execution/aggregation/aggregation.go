package aggregation

import (
	"storemy/pkg/execution/aggregation/internal/core"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// Re-export types and constants from internal/core
type AggregateOp = core.AggregateOp

const (
	NoGrouping = core.NoGrouping
	Min        = core.Min
	Max        = core.Max
	Sum        = core.Sum
	Avg        = core.Avg
	Count      = core.Count
	And        = core.And
	Or         = core.Or
)

// ParseAggregateOp converts an aggregate operation string to AggregateOp enum.
var ParseAggregateOp = core.ParseAggregateOp

// Aggregator interface defines the contract for aggregation operations
// This is the fundamental interface that all aggregators must implement
type Aggregator interface {
	// Merge processes a new tuple into the aggregate, grouping as specified
	// This is where the actual aggregation computation happens
	Merge(tup *tuple.Tuple) error

	// Iterator returns a DbIterator over the aggregate results
	// Results are tuples containing either (aggregateValue) or (groupValue, aggregateValue)
	Iterator() iterator.DbIterator

	// GetTupleDesc returns the tuple description for the aggregate results
	GetTupleDesc() *tuple.TupleDescription

	// InitializeDefault initializes the default group for non-grouped aggregates
	// This is used when there are no input tuples to ensure COUNT(*) returns 0
	InitializeDefault() error
}
