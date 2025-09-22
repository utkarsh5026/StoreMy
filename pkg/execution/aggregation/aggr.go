package aggregation

import (
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

const (
	// NoGrouping indicates that no grouping field is used in aggregation
	NoGrouping = -1
)

// AggregateOp represents the type of aggregation operation to perform
type AggregateOp int

const (
	Min AggregateOp = iota
	Max
	Sum
	Avg
	Count
	And
	Or
)

// String returns a string representation of the aggregation operation
func (op AggregateOp) String() string {
	switch op {
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	case Sum:
		return "SUM"
	case Avg:
		return "AVG"
	case Count:
		return "COUNT"
	case And:
		return "AND"
	case Or:
		return "OR"
	default:
		return "UNKNOWN"
	}
}

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
}
