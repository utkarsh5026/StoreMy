package join

import (
	"storemy/pkg/catalog"
	"storemy/pkg/primitives"
)

const (
	DefaultCardinality = 1000
)

// CostEstimator provides cost estimation for join algorithms using catalog statistics
type CostEstimator struct {
	catalog *catalog.SystemCatalog
}

// NewCostEstimator creates a new cost estimator
func NewCostEstimator(cat *catalog.SystemCatalog) *CostEstimator {
	return &CostEstimator{
		catalog: cat,
	}
}

// BuildJoinStatistics creates JoinStatistics from catalog statistics for two tables
func (ce *CostEstimator) BuildJoinStatistics(
	tid *primitives.TransactionID, leftTableID, rightTableID, memoryPages int) (*JoinStatistics, error) {
	leftStats, err := ce.catalog.GetTableStatistics(tid, leftTableID)
	if err != nil {
		leftStats = &catalog.TableStatistics{
			TableID:     leftTableID,
			Cardinality: DefaultCardinality, // Conservative estimate
			PageCount:   10,
		}
	}

	rightStats, err := ce.catalog.GetTableStatistics(tid, rightTableID)
	if err != nil {
		rightStats = &catalog.TableStatistics{
			TableID:     rightTableID,
			Cardinality: DefaultCardinality,
			PageCount:   10,
		}
	}

	selectivity := ce.estimateSelectivity(leftStats, rightStats)
	return &JoinStatistics{
		LeftCardinality:  leftStats.Cardinality,
		RightCardinality: rightStats.Cardinality,
		LeftSize:         leftStats.PageCount,
		RightSize:        rightStats.PageCount,
		LeftSorted:       false,
		RightSorted:      false,
		MemorySize:       memoryPages,
		Selectivity:      selectivity,
	}, nil
}

// estimateSelectivity estimates join selectivity based on statistics
func (ce *CostEstimator) estimateSelectivity(left, right *catalog.TableStatistics) float64 {
	// Use the smaller distinct value count for selectivity estimation
	// Selectivity = 1 / max(distinctLeft, distinctRight)
	maxDistinct := left.DistinctValues
	if right.DistinctValues > maxDistinct {
		maxDistinct = right.DistinctValues
	}

	if maxDistinct == 0 {
		return 0.1
	}

	selectivity := 1.0 / float64(maxDistinct)

	if selectivity < 0.01 {
		selectivity = 0.01
	}
	if selectivity > 1.0 {
		selectivity = 1.0
	}

	return selectivity
}

// EstimateOutputCardinality estimates the number of tuples in join result
func (ce *CostEstimator) EstimateOutputCardinality(stats *JoinStatistics) int {
	return int(float64(stats.LeftCardinality*stats.RightCardinality) * stats.Selectivity)
}

// EstimateNestedLoopCost estimates the I/O cost for nested loop join
// Cost = LeftSize + (LeftCardinality * RightSize)
func (ce *CostEstimator) EstimateNestedLoopCost(stats *JoinStatistics) float64 {
	return float64(stats.LeftSize) + float64(stats.LeftCardinality*stats.RightSize)
}

// EstimateSortMergeCost estimates the I/O cost for sort-merge join
// Cost = 2 * LeftSize * log2(LeftSize) + 2 * RightSize * log2(RightSize) + LeftSize + RightSize
func (ce *CostEstimator) EstimateSortMergeCost(stats *JoinStatistics) float64 {
	leftSortCost := 0.0
	rightSortCost := 0.0

	if !stats.LeftSorted && stats.LeftSize > 1 {
		leftSortCost = 2.0 * float64(stats.LeftSize) * log2(float64(stats.LeftSize))
	}

	if !stats.RightSorted && stats.RightSize > 1 {
		rightSortCost = 2.0 * float64(stats.RightSize) * log2(float64(stats.RightSize))
	}

	mergeCost := float64(stats.LeftSize + stats.RightSize)
	return leftSortCost + rightSortCost + mergeCost
}

// EstimateHashJoinCost estimates the I/O cost for hash join
// Cost = 3 * (LeftSize + RightSize) if hash table fits in memory
// Cost = 2 * (LeftSize + RightSize) * (log_M-1(B/M) + 1) for partitioned hash join
func (ce *CostEstimator) EstimateHashJoinCost(stats *JoinStatistics) float64 {
	buildSize := min(stats.RightSize, stats.LeftSize)

	if buildSize <= stats.MemorySize {
		return 3.0 * float64(stats.LeftSize+stats.RightSize)
	}

	numPartitions := float64(buildSize) / float64(stats.MemorySize)
	numPartitions = max(2, numPartitions)

	return 2.0*float64(stats.LeftSize+stats.RightSize)*log2(numPartitions) +
		float64(stats.LeftSize+stats.RightSize)
}

// SelectBestAlgorithm recommends the best join algorithm based on statistics
func (ce *CostEstimator) SelectBestAlgorithm(stats *JoinStatistics) string {
	nlCost := ce.EstimateNestedLoopCost(stats)
	smCost := ce.EstimateSortMergeCost(stats)
	hjCost := ce.EstimateHashJoinCost(stats)

	minCost := nlCost
	best := "NestedLoop"

	if smCost < minCost {
		minCost = smCost
		best = "SortMerge"
	}

	if hjCost < minCost {
		best = "Hash"
	}

	return best
}

// log2 is a helper function to calculate log base 2
func log2(x float64) float64 {
	if x <= 1 {
		return 0
	}

	result := 0.0
	for x > 1 {
		x /= 2
		result++
	}
	return result
}
