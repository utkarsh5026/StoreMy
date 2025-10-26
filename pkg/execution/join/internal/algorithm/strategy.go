package algorithm

import (
	"fmt"
	"storemy/pkg/execution/join/internal/common"
	"storemy/pkg/iterator"
	"storemy/pkg/tuple"
)

// JoinStrategy selects the best join algorithm based on statistics and predicate type
type JoinStrategy struct {
	algorithms []common.JoinAlgorithm
	stats      *common.JoinStatistics
}

// NewJoinStrategy creates a strategy selector with available join algorithms
func NewJoinStrategy(left, right iterator.DbIterator, pred common.JoinPredicate, stats *common.JoinStatistics) *JoinStrategy {
	if stats == nil {
		stats = common.DefaultJoinStatistics()
	}

	strategy := &JoinStrategy{
		stats:      stats,
		algorithms: make([]common.JoinAlgorithm, 0),
	}

	strategy.algorithms = append(strategy.algorithms,
		NewHashJoin(left, right, pred, stats),
		NewSortMergeJoin(left, right, pred, stats),
		NewNestedLoopJoin(left, right, pred, stats),
	)

	return strategy
}

// SelectBestAlgorithm chooses the most efficient join algorithm
func (js *JoinStrategy) SelectBestAlgorithm(pred common.JoinPredicate) (common.JoinAlgorithm, error) {
	var bestAlgorithm common.JoinAlgorithm
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
func GetStatistics(left, right iterator.DbIterator) (*common.JoinStatistics, error) {
	stats := &common.JoinStatistics{
		LeftCardinality:  0,
		RightCardinality: 0,
		LeftSize:         0,
		RightSize:        0,
		MemorySize:       100, // Default memory pages
		Selectivity:      0.1, // Default selectivity
	}

	leftCount, err := countTuples(left)
	if err != nil {
		return stats, err
	}
	stats.LeftCardinality = leftCount

	if err := left.Rewind(); err != nil {
		return stats, err
	}

	rightCount, err := countTuples(right)
	if err != nil {
		return stats, err
	}
	stats.RightCardinality = rightCount

	if err := right.Rewind(); err != nil {
		return stats, err
	}

	stats.LeftSize = (stats.LeftCardinality + 99) / 100
	stats.RightSize = (stats.RightCardinality + 99) / 100
	return stats, nil
}

func countTuples(iter iterator.DbIterator) (int, error) {
	return iterator.Reduce(iter, 0, func(i int, t *tuple.Tuple) (int, error) {
		return i + 1, nil
	})
}
