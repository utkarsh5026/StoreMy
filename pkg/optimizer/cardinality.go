package optimizer

import (
	"math"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/planner"
)

// Default constants for cardinality estimation
const (
	DefaultTableCardinality = 1000 // Default table size when no statistics
	DefaultDistinctCount    = 100  // Default distinct count
	DefaultJoinSelectivity  = 0.1  // 10% for non-equi-joins
	MinCardinality          = 1    // Minimum estimated cardinality
)

// CardinalityEstimator estimates the output cardinality (number of rows)
// of query plan nodes
type CardinalityEstimator struct {
	catalog              *catalog.SystemCatalog
	selectivityEstimator *SelectivityEstimator
}

// NewCardinalityEstimator creates a new cardinality estimator
func NewCardinalityEstimator(cat *catalog.SystemCatalog) *CardinalityEstimator {
	return &CardinalityEstimator{
		catalog:              cat,
		selectivityEstimator: NewSelectivityEstimator(cat),
	}
}

// EstimatePlanCardinality estimates the output cardinality for a plan node
func (ce *CardinalityEstimator) EstimatePlanCardinality(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
) int64 {
	if plan == nil {
		return 0
	}

	switch node := plan.(type) {
	case *planner.ScanNode:
		return ce.estimateScanCardinality(tx, node)
	case *planner.JoinNode:
		return ce.estimateJoinCardinality(tx, node)
	case *planner.FilterNode:
		return ce.estimateFilterCardinality(tx, node)
	case *planner.ProjectNode:
		return ce.estimateProjectCardinality(tx, node)
	case *planner.AggregateNode:
		return ce.estimateAggregateCardinality(tx, node)
	case *planner.SortNode:
		return ce.estimateSortCardinality(tx, node)
	case *planner.LimitNode:
		return ce.estimateLimitCardinality(tx, node)
	default:
		return 0
	}
}

// estimateScanCardinality estimates output rows for a scan node
func (ce *CardinalityEstimator) estimateScanCardinality(
	tx *transaction.TransactionContext,
	node *planner.ScanNode,
) int64 {
	tableStats, err := ce.catalog.GetTableStatistics(tx, node.TableID)
	if err != nil || tableStats == nil {
		return DefaultTableCardinality
	}

	baseCard := int64(tableStats.Cardinality)
	if len(node.Predicates) == 0 {
		return baseCard
	}

	totalSelectivity := 1.0
	for _, pred := range node.Predicates {
		sel := ce.selectivityEstimator.EstimatePredicateSelectivity(tx, pred.Predicate, node.TableID, pred.Column)
		totalSelectivity *= sel
	}

	return int64(float64(baseCard) * totalSelectivity)
}

// estimateJoinCardinality estimates output rows for a join node
func (ce *CardinalityEstimator) estimateJoinCardinality(
	tx *transaction.TransactionContext,
	node *planner.JoinNode,
) int64 {
	leftCard := ce.EstimatePlanCardinality(tx, node.LeftChild)
	rightCard := ce.EstimatePlanCardinality(tx, node.RightChild)

	if leftCard == 0 || rightCard == 0 {
		return 0
	}

	baseCard := leftCard * rightCard
	joinSelectivity := ce.estimateJoinSelectivity(tx, node)

	filterSelectivity := 1.0
	for _, filter := range node.ExtraFilters {
		sel := ce.selectivityEstimator.EstimatePredicateSelectivity(tx, filter.Predicate, 0, filter.Column)
		filterSelectivity *= sel
	}

	totalSelectivity := joinSelectivity * filterSelectivity
	result := int64(float64(baseCard) * totalSelectivity)

	return int64(math.Max(1.0, math.Min(float64(result), float64(leftCard*rightCard))))
}

// estimateJoinSelectivity estimates the selectivity of the join condition
func (ce *CardinalityEstimator) estimateJoinSelectivity(
	tx *transaction.TransactionContext,
	node *planner.JoinNode,
) float64 {
	if node.LeftColumn == "" || node.RightColumn == "" {
		return 1.0
	}

	return ce.estimateEquiJoinSelectivity(tx, node)
}

// estimateEquiJoinSelectivity estimates selectivity for equi-joins
func (ce *CardinalityEstimator) estimateEquiJoinSelectivity(
	tx *transaction.TransactionContext,
	node *planner.JoinNode,
) float64 {
	leftDistinct := ce.getColumnDistinctCount(tx, node.LeftChild, node.LeftColumn)
	rightDistinct := ce.getColumnDistinctCount(tx, node.RightChild, node.RightColumn)

	if leftDistinct == 0 || rightDistinct == 0 {
		return DefaultJoinSelectivity
	}

	maxDistinct := math.Max(float64(leftDistinct), float64(rightDistinct))
	return 1.0 / maxDistinct
}

// getColumnDistinctCount gets the distinct count for a column in a plan subtree
func (ce *CardinalityEstimator) getColumnDistinctCount(
	tx *transaction.TransactionContext,
	plan planner.PlanNode,
	columnName string,
) int64 {
	if scanNode, ok := plan.(*planner.ScanNode); ok {
		colStats, err := ce.catalog.GetColumnStatistics(tx, scanNode.TableID, columnName)
		if err == nil && colStats != nil {
			return colStats.DistinctCount
		}
	}

	for _, child := range plan.GetChildren() {
		distinct := ce.getColumnDistinctCount(tx, child, columnName)
		if distinct > 0 {
			return distinct
		}
	}

	return DefaultDistinctCount
}

// estimateFilterCardinality estimates output rows for a filter node
func (ce *CardinalityEstimator) estimateFilterCardinality(
	tx *transaction.TransactionContext,
	node *planner.FilterNode,
) int64 {
	childCard := ce.EstimatePlanCardinality(tx, node.Child)
	if len(node.Predicates) == 0 {
		return childCard
	}

	totalSelectivity := 1.0
	for _, pred := range node.Predicates {
		sel := ce.selectivityEstimator.EstimatePredicateSelectivity(tx, pred.Predicate, 0, pred.Column)
		totalSelectivity *= sel
	}

	return int64(float64(childCard) * totalSelectivity)
}

// estimateProjectCardinality estimates output rows for a projection
// Projection doesn't change cardinality (only the schema)
func (ce *CardinalityEstimator) estimateProjectCardinality(
	tx *transaction.TransactionContext,
	node *planner.ProjectNode,
) int64 {
	return ce.EstimatePlanCardinality(tx, node.Child)
}

// estimateAggregateCardinality estimates output rows for an aggregation
func (ce *CardinalityEstimator) estimateAggregateCardinality(
	tx *transaction.TransactionContext,
	node *planner.AggregateNode,
) int64 {
	childCard := ce.EstimatePlanCardinality(tx, node.Child)

	if len(node.GroupByExprs) == 0 {
		// No GROUP BY: single row output
		return 1
	}

	groupCount := int64(float64(childCard) * 0.1)
	return int64(math.Max(1.0, math.Min(float64(groupCount), float64(childCard))))
}

// estimateSortCardinality estimates output rows for a sort
// Sort doesn't change cardinality
func (ce *CardinalityEstimator) estimateSortCardinality(
	tx *transaction.TransactionContext,
	node *planner.SortNode,
) int64 {
	return ce.EstimatePlanCardinality(tx, node.Child)
}

// estimateLimitCardinality estimates output rows for a LIMIT
func (ce *CardinalityEstimator) estimateLimitCardinality(
	tx *transaction.TransactionContext,
	node *planner.LimitNode,
) int64 {
	childCard := ce.EstimatePlanCardinality(tx, node.Child)

	limitCard := int64(node.Limit)
	if limitCard <= 0 {
		return childCard
	}

	effectiveCard := childCard - int64(node.Offset)
	if effectiveCard < 0 {
		return 0
	}

	return int64(math.Min(float64(limitCard), float64(effectiveCard)))
}
