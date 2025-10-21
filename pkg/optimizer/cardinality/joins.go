package cardinality

import (
	"math"
	"storemy/pkg/plan"
)

// estimateJoin estimates output rows for a join node.
//
// Mathematical Model:
//
//	baseCardinality = leftRows × rightRows
//	outputRows = baseCardinality × joinSelectivity × filterSelectivity
//
// Formula:
//
//	outputRows = min(leftRows × rightRows × sel_join × ∏(sel_filter_i)^correlationFactor, leftRows × rightRows)
//
// Reasoning:
//   - Cross product (leftRows × rightRows) is the worst-case upper bound
//   - Join condition reduces this via joinSelectivity (typically 1/max(NDV_left, NDV_right))
//   - Extra filters further reduce output using correlation-corrected selectivity
//   - Final result cannot exceed cross product size (min constraint)
//   - Cannot produce fewer than 1 row to avoid zero estimates
//   - Correlation correction prevents over-aggressive filtering from compound predicates
//
// Join Selectivity Estimation:
//   - Equi-joins use distinct value counts (NDV) from both sides
//   - sel_join = 1/max(NDV_left, NDV_right) under uniform distribution assumption
//   - Containment model adjusts when one side has significantly fewer distinct values
//   - Falls back to DefaultJoinSelectivity when statistics unavailable
//
// Example:
//
//	Orders (10,000 rows) ⋈ Customers (1,000 rows) ON orders.customer_id = customers.id
//	Assume: 800 distinct customer_ids in orders, 1,000 distinct ids in customers
//	Base: 10,000 × 1,000 = 10,000,000 rows
//	Join selectivity: 1/1,000 = 0.001
//	Output: 10,000,000 × 0.001 = 10,000 rows
//	With filter status='active' (sel=0.3): 10,000 × 0.3 = 3,000 rows
func (ce *CardinalityEstimator) estimateJoin(node *plan.JoinNode) (int64, error) {
	leftCard, err := ce.EstimatePlanCardinality(node.LeftChild)
	if err != nil {
		return 0, err
	}

	rightCard, err := ce.EstimatePlanCardinality(node.RightChild)
	if err != nil {
		return 0, err
	}

	if leftCard == 0 || rightCard == 0 {
		return 0, nil
	}

	baseCard := leftCard * rightCard
	joinSelectivity := ce.estimateJoinSelectivity(node)

	filterSelectivity := 1.0
	if len(node.ExtraFilters) > 0 {
		selectivities := make([]float64, 0, len(node.ExtraFilters))
		tableID, found := findBaseTableID(node.LeftChild)
		if !found {
			tableID, _ = findBaseTableID(node.RightChild)
		}

		for i := range node.ExtraFilters {
			sel := ce.estimatePredicateSelectivity(tableID, &node.ExtraFilters[i])
			selectivities = append(selectivities, sel)
		}

		filterSelectivity = applyCorrelationCorrection(selectivities)
	}

	totalSelectivity := joinSelectivity * filterSelectivity
	result := int64(float64(baseCard) * totalSelectivity)

	finalCard := math.Min(float64(result), float64(leftCard*rightCard))
	return int64(math.Max(1.0, finalCard)), nil
}

// estimateJoinSelectivity estimates the selectivity of the join condition.
//
// Mathematical Model:
//   - For equi-joins: Uses distinct value statistics
//   - For cross joins: selectivity = 1.0 (no filtering)
//
// Reasoning:
//   - Equi-joins (A.x = B.y) filter based on value matching
//   - Cross joins have no condition, so all combinations pass
//   - Delegates to specialized estimators based on join type
//
// Example:
//   - Equi-join: orders.customer_id = customers.id → uses NDV-based estimation
//   - Cross join: SELECT * FROM A, B → selectivity = 1.0
func (ce *CardinalityEstimator) estimateJoinSelectivity(node *plan.JoinNode) float64 {
	if node.LeftColumn == "" || node.RightColumn == "" {
		return 1.0
	}

	return ce.estimateEquiJoinSelectivity(node)
}

// estimateEquiJoinSelectivity estimates selectivity for equi-joins using
// the containment principle and distinct value analysis.
//
// Mathematical Model:
//
//	Let NDV_L = distinct values in left column, NDV_R = distinct values in right column
//
//	Containment scenario (maxDistinct > 2 × minDistinct):
//	  selectivity = 1 / minDistinct
//
//	Partial overlap scenario (similar distinct counts):
//	  selectivity = 1 / maxDistinct
//
// Formula:
//
//	sel = 1 / min(NDV_L, NDV_R)  if maxDistinct > 2 × minDistinct
//	sel = 1 / max(NDV_L, NDV_R)  otherwise
//
// Reasoning:
//   - Standard equi-join assumption: uniform distribution of values
//   - Each distinct value on one side matches 1/NDV values on other side
//   - Containment detection: If one side has many more distinct values (>2× ratio),
//     assume smaller side is subset of larger side
//   - In containment: all values from smaller side will match, so use its NDV
//   - In partial overlap: use conservative (larger) NDV estimate
//   - Falls back to DefaultJoinSelectivity (0.1) without statistics
//
// Examples:
//
//  1. Containment scenario:
//     Orders: 10,000 distinct customer_ids
//     Customers: 1,000 distinct ids
//     Ratio: 10,000 / 1,000 = 10 > 2 (containment likely)
//     selectivity = 1/1,000 = 0.001
//
//  2. Partial overlap scenario:
//     Table A: 500 distinct values
//     Table B: 700 distinct values
//     Ratio: 700 / 500 = 1.4 < 2 (partial overlap)
//     selectivity = 1/700 ≈ 0.0014 (conservative)
//
//  3. No statistics:
//     selectivity = DefaultJoinSelectivity = 0.1
func (ce *CardinalityEstimator) estimateEquiJoinSelectivity(node *plan.JoinNode) float64 {
	leftDistinct := ce.getColumnDistinctCount(node.LeftChild, node.LeftColumn)
	rightDistinct := ce.getColumnDistinctCount(node.RightChild, node.RightColumn)

	if leftDistinct == DefaultDistinctCount && rightDistinct == DefaultDistinctCount {
		return DefaultJoinSelectivity
	}

	if leftDistinct == 0 || rightDistinct == 0 {
		return DefaultJoinSelectivity
	}

	minDistinct := math.Min(float64(leftDistinct), float64(rightDistinct))
	maxDistinct := math.Max(float64(leftDistinct), float64(rightDistinct))

	// Containment detection: If one side has significantly fewer distinct values,
	// it's likely that the smaller set is contained within the larger set.
	// In this case, use the smaller distinct count for selectivity.
	// The threshold of 2× is based on empirical studies and provides a balance
	// between detecting true containment and avoiding false positives.
	if maxDistinct > minDistinct*2.0 {
		// Likely containment scenario: smaller side is subset of larger
		// Every value from smaller side will match, so selectivity based on its NDV
		return 1.0 / minDistinct
	}

	// Similar distinct counts: assume partial overlap
	// Use maximum as conservative estimate (lower selectivity = fewer output rows)
	return 1.0 / maxDistinct
}
