package cardinality

import (
	"math"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/plan"
)

// estimateDistinct estimates output rows for a DISTINCT operation.
// Uses column statistics when available, or applies a default reduction factor.
func (ce *CardinalityEstimator) estimateDistinct(
	tx *transaction.TransactionContext,
	node *plan.DistinctNode,
) (int64, error) {
	childCard, err := ce.EstimatePlanCardinality(tx, node.Child)
	if err != nil {
		return 0, err
	}

	if childCard == 0 {
		return 0, nil
	}

	if len(node.DistinctExprs) > 0 {
		distinctCount := ce.estimateGroupByDistinctCount(tx, node.Child, node.DistinctExprs)
		return int64(math.Min(float64(distinctCount), float64(childCard))), nil
	}

	// DISTINCT on all columns: estimate based on typical duplicate ratio
	// Research shows that typical tables have 70-90% unique rows
	// We use 80% as a middle ground estimate
	defaultDistinctRatio := 0.8
	estimatedDistinct := int64(float64(childCard) * defaultDistinctRatio)

	// Ensure at least 1 row
	return int64(math.Max(1.0, float64(estimatedDistinct))), nil
}

// estimateUnionCardinality estimates output rows for a UNION operation.
// UNION ALL simply adds cardinalities, while UNION estimates overlap and deduplicates.
func (ce *CardinalityEstimator) estimateUnionCardinality(
	tx *transaction.TransactionContext,
	node *plan.UnionNode,
) (int64, error) {
	leftCard, err := ce.EstimatePlanCardinality(tx, node.LeftChild)
	if err != nil {
		return 0, err
	}

	rightCard, err := ce.EstimatePlanCardinality(tx, node.RightChild)
	if err != nil {
		return 0, err
	}

	if node.UnionAll {
		return leftCard + rightCard, nil
	}

	// UNION (with deduplication): estimate overlap and subtract
	// Use a heuristic approach based on the relative sizes
	totalCard := leftCard + rightCard
	if totalCard == 0 {
		return 0, nil
	}

	// Estimate overlap using inclusion-exclusion principle
	// Assume some overlap based on relative sizes
	// If sets are similar size, assume 10-20% overlap
	// If one is much smaller, assume higher containment
	minCard := math.Min(float64(leftCard), float64(rightCard))
	maxCard := math.Max(float64(leftCard), float64(rightCard))

	var overlapRatio float64
	if maxCard > 0 {
		sizeRatio := minCard / maxCard

		if sizeRatio < 0.1 {
			overlapRatio = 0.5
		} else if sizeRatio < 0.5 {
			overlapRatio = 0.3
		} else {
			overlapRatio = 0.15
		}
	} else {
		overlapRatio = 0.15
	}

	// Estimate overlap based on the smaller set
	estimatedOverlap := minCard * overlapRatio

	// Result = left + right - overlap
	result := float64(totalCard) - estimatedOverlap

	// Ensure result is between max(leftCard, rightCard) and leftCard + rightCard
	result = math.Max(maxCard, result)
	result = math.Min(float64(totalCard), result)

	return int64(math.Max(1.0, result)), nil
}
