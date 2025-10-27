package core

import (
	"fmt"
	"math"
	"strings"
)

const (
	// NoGrouping indicates that no grouping field is used in aggregation
	NoGrouping = math.MaxUint32
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
