package cardinality

import (
	"math"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/plan"
)

// estimateScan estimates output rows for a scan node.
// Uses enhanced selectivity estimation supporting NULL checks, LIKE patterns,
// IN clauses, and value-based estimates. Applies correlation correction when
// multiple predicates are present to avoid over-optimistic estimates.
func (ce *CardinalityEstimator) estimateScan(
	tx *transaction.TransactionContext,
	node *plan.ScanNode,
) (int64, error) {
	tableStats, err := ce.catalog.GetTableStatistics(tx, node.TableID)
	if err != nil || tableStats == nil {
		return DefaultTableCardinality, nil
	}

	tableCard := int64(tableStats.Cardinality)
	if len(node.Predicates) == 0 {
		return tableCard, nil
	}

	selectivities := make([]float64, 0, len(node.Predicates))
	for i := range node.Predicates {
		sel := ce.estimatePredicateSelectivity(tx, node.TableID, &node.Predicates[i])
		selectivities = append(selectivities, sel)
	}

	totalSelectivity := applyCorrelationCorrection(selectivities)
	return int64(float64(tableCard) * totalSelectivity), nil
}

// estimateFilter estimates output rows for a filter node.
// Uses enhanced selectivity estimation supporting NULL checks, LIKE patterns,
// IN clauses, and value-based estimates. Applies correlation correction when
// multiple predicates are present to avoid over-optimistic estimates.
func (ce *CardinalityEstimator) estimateFilter(
	tx *transaction.TransactionContext,
	node *plan.FilterNode,
) (int64, error) {
	childCard, err := ce.EstimatePlanCardinality(tx, node.Child)
	if err != nil {
		return 0, err
	}

	if len(node.Predicates) == 0 {
		return childCard, nil
	}

	tableID := findBaseTableID(node.Child)

	selectivities := make([]float64, 0, len(node.Predicates))
	for i := range node.Predicates {
		sel := ce.estimatePredicateSelectivity(tx, tableID, &node.Predicates[i])
		selectivities = append(selectivities, sel)
	}

	totalSelectivity := applyCorrelationCorrection(selectivities)
	return int64(float64(childCard) * totalSelectivity), nil
}

// estimateProject estimates output rows for a projection
// Projection doesn't change cardinality (only the schema)
func (ce *CardinalityEstimator) estimateProject(
	tx *transaction.TransactionContext,
	node *plan.ProjectNode,
) (int64, error) {
	return ce.EstimatePlanCardinality(tx, node.Child)
}

// estimateSort estimates output rows for a sort
// Sort doesn't change cardinality
func (ce *CardinalityEstimator) estimateSort(
	tx *transaction.TransactionContext,
	node *plan.SortNode,
) (int64, error) {
	return ce.EstimatePlanCardinality(tx, node.Child)
}

// estimateLimit estimates output rows for a LIMIT
func (ce *CardinalityEstimator) estimateLimit(
	tx *transaction.TransactionContext,
	node *plan.LimitNode,
) (int64, error) {
	childCard, err := ce.EstimatePlanCardinality(tx, node.Child)
	if err != nil {
		return 0, err
	}

	limitCard := int64(node.Limit)
	if limitCard <= 0 {
		return childCard, nil
	}

	effectiveCard := childCard - int64(node.Offset)
	if effectiveCard < 0 {
		return 0, nil
	}

	return int64(math.Min(float64(limitCard), float64(effectiveCard))), nil
}
