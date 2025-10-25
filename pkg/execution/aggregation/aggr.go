package aggregation

import (
	"fmt"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
	"strings"
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

// ParseAggregateOp converts an aggregate operation string to AggregateOp enum.
func ParseAggregateOp(opStr string) (AggregateOp, error) {
	switch strings.ToUpper(opStr) {
	case "MIN":
		return Min, nil
	case "MAX":
		return Max, nil
	case "SUM":
		return Sum, nil
	case "AVG":
		return Avg, nil
	case "COUNT":
		return Count, nil
	case "AND":
		return And, nil
	case "OR":
		return Or, nil
	default:
		return 0, fmt.Errorf("unsupported aggregate operation: %s", opStr)
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

	// InitializeDefault initializes the default group for non-grouped aggregates
	// This is used when there are no input tuples to ensure COUNT(*) returns 0
	InitializeDefault() error
}
