package join

import "storemy/pkg/tuple"

const (
	DefaultHighCost = 1e6 // Arbitrary high cost for unsupported scenarios
)

// JoinAlgorithm defines the interface for all join implementations
type JoinAlgorithm interface {
	// Initialize prepares the join algorithm for execution
	Initialize() error

	// GetNext returns the next joined tuple or nil if no more tuples
	Next() (*tuple.Tuple, error)

	// Reset resets the join algorithm to start from the beginning
	Reset() error

	// Close releases resources used by the join algorithm
	Close() error

	// EstimateCost returns an estimated cost for this join algorithm
	EstimateCost() float64

	// SupportsPredicateType checks if this algorithm can handle the given predicate
	SupportsPredicateType(predicate *JoinPredicate) bool
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
