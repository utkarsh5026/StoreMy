package cardinality

import (
	"math"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/plan"
)

// estimateJoin estimates output rows for a join node.
// Uses enhanced selectivity estimation for extra filters and applies
// correlation correction when multiple filters are present.
func (ce *CardinalityEstimator) estimateJoin(
	tx *transaction.TransactionContext,
	node *plan.JoinNode,
) (int64, error) {
	leftCard, err := ce.EstimatePlanCardinality(tx, node.LeftChild)
	if err != nil {
		return 0, err
	}

	rightCard, err := ce.EstimatePlanCardinality(tx, node.RightChild)
	if err != nil {
		return 0, err
	}

	if leftCard == 0 || rightCard == 0 {
		return 0, nil
	}

	baseCard := leftCard * rightCard
	joinSelectivity := ce.estimateJoinSelectivity(tx, node)

	filterSelectivity := 1.0
	if len(node.ExtraFilters) > 0 {
		selectivities := make([]float64, 0, len(node.ExtraFilters))

		tableID := findBaseTableID(node.LeftChild)
		if tableID == 0 {
			tableID = findBaseTableID(node.RightChild)
		}

		for i := range node.ExtraFilters {
			sel := ce.estimatePredicateSelectivity(tx, tableID, &node.ExtraFilters[i])
			selectivities = append(selectivities, sel)
		}

		filterSelectivity = applyCorrelationCorrection(selectivities)
	}

	totalSelectivity := joinSelectivity * filterSelectivity
	result := int64(float64(baseCard) * totalSelectivity)

	finalCard := math.Min(float64(result), float64(leftCard*rightCard))
	return int64(math.Max(1.0, finalCard)), nil
}

// estimateJoinSelectivity estimates the selectivity of the join condition
func (ce *CardinalityEstimator) estimateJoinSelectivity(
	tx *transaction.TransactionContext,
	node *plan.JoinNode,
) float64 {
	if node.LeftColumn == "" || node.RightColumn == "" {
		return 1.0
	}

	return ce.estimateEquiJoinSelectivity(tx, node)
}

// estimateEquiJoinSelectivity estimates selectivity for equi-joins with
// containment consideration. Uses a more sophisticated model that accounts
// for the possibility that one side's values may be a subset of the other.
func (ce *CardinalityEstimator) estimateEquiJoinSelectivity(
	tx *transaction.TransactionContext,
	node *plan.JoinNode,
) float64 {
	leftDistinct := ce.getColumnDistinctCount(tx, node.LeftChild, node.LeftColumn)
	rightDistinct := ce.getColumnDistinctCount(tx, node.RightChild, node.RightColumn)

	if leftDistinct == DefaultDistinctCount && rightDistinct == DefaultDistinctCount {
		return DefaultJoinSelectivity
	}

	if leftDistinct == 0 || rightDistinct == 0 {
		return DefaultJoinSelectivity
	}

	minDistinct := math.Min(float64(leftDistinct), float64(rightDistinct))
	maxDistinct := math.Max(float64(leftDistinct), float64(rightDistinct))

	// If one side has significantly fewer distinct values, it's likely
	// that the smaller set is contained within the larger set.
	// In this case, use the smaller distinct count for selectivity.
	// The threshold of 2x is based on empirical studies.
	if maxDistinct > minDistinct*2.0 {
		// Likely containment scenario: smaller side is subset of larger
		return 1.0 / minDistinct
	}

	// Similar distinct counts: assume partial overlap
	// Use maximum as conservative estimate
	return 1.0 / maxDistinct
}
