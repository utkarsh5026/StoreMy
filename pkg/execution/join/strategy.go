package join

import (
	"fmt"
	"storemy/pkg/iterator"
)

// JoinStrategy selects the best join algorithm based on statistics and predicate type
type JoinStrategy struct {
	algorithms []JoinAlgorithm
	stats      *JoinStatistics
}

// NewJoinStrategy creates a strategy selector with available join algorithms
func NewJoinStrategy(left, right iterator.DbIterator, pred *JoinPredicate, stats *JoinStatistics) *JoinStrategy {
	if stats == nil {
		stats = &JoinStatistics{
			LeftCardinality:  1000,
			RightCardinality: 1000,
			LeftSize:         10,
			RightSize:        10,
			MemorySize:       100,
			Selectivity:      0.1,
		}
	}

	strategy := &JoinStrategy{
		stats:      stats,
		algorithms: make([]JoinAlgorithm, 0),
	}

	strategy.algorithms = append(strategy.algorithms,
		NewHashJoin(left, right, pred, stats),
		NewSortMergeJoin(left, right, pred, stats),
		NewNestedLoopJoin(left, right, pred, stats),
	)

	return strategy
}

// SelectBestAlgorithm chooses the most efficient join algorithm
func (js *JoinStrategy) SelectBestAlgorithm(pred *JoinPredicate) (JoinAlgorithm, error) {
	var bestAlgorithm JoinAlgorithm
	bestCost := float64(^uint(0) >> 1)

	for _, alg := range js.algorithms {
		if !alg.SupportsPredicateType(pred) {
			continue
		}

		cost := alg.EstimateCost()
		if cost < bestCost {
			bestCost = cost
			bestAlgorithm = alg
		}
	}

	if bestAlgorithm == nil {
		return nil, fmt.Errorf("no suitable join algorithm found for predicate")
	}

	return bestAlgorithm, nil
}

// GetStatistics collects statistics about the input relations
func GetStatistics(left, right iterator.DbIterator) (*JoinStatistics, error) {
	stats := &JoinStatistics{
		LeftCardinality:  0,
		RightCardinality: 0,
		LeftSize:         0,
		RightSize:        0,
		MemorySize:       100, // Default memory pages
		Selectivity:      0.1, // Default selectivity
	}

	for {
		hasNext, err := left.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		_, err = left.Next()
		if err != nil {
			return nil, err
		}
		stats.LeftCardinality++
	}
	left.Rewind()

	for {
		hasNext, err := right.HasNext()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		_, err = right.Next()
		if err != nil {
			return nil, err
		}
		stats.RightCardinality++
	}
	right.Rewind()

	stats.LeftSize = (stats.LeftCardinality + 99) / 100
	stats.RightSize = (stats.RightCardinality + 99) / 100

	return stats, nil
}
