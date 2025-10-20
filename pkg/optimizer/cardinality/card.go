package cardinality

import (
	"fmt"
	"math"
	"storemy/pkg/catalog"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/selectivity"
	"storemy/pkg/plan"
	"storemy/pkg/types"
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
	catalog   *catalog.SystemCatalog
	estimator *selectivity.SelectivityEstimator
}

// NewCardinalityEstimator creates a new cardinality estimator
func NewCardinalityEstimator(cat *catalog.SystemCatalog) (*CardinalityEstimator, error) {
	if cat == nil {
		return nil, fmt.Errorf("cannot have nil CATALOG")
	}
	return &CardinalityEstimator{
		catalog:   cat,
		estimator: selectivity.NewSelectivityEstimator(cat),
	}, nil
}

// EstimatePlanCardinality estimates the output cardinality for a plan node
func (ce *CardinalityEstimator) EstimatePlanCardinality(
	tx *transaction.TransactionContext,
	planNode plan.PlanNode,
) (int64, error) {
	if planNode == nil {
		return 0, nil
	}

	if existingCard := planNode.GetCardinality(); existingCard > 0 {
		return existingCard, nil
	}

	switch node := planNode.(type) {
	case *plan.ScanNode:
		return ce.estimateScan(tx, node)
	case *plan.JoinNode:
		return ce.estimateJoin(tx, node)
	case *plan.FilterNode:
		return ce.estimateFilter(tx, node)
	case *plan.ProjectNode:
		return ce.estimateProject(tx, node)
	case *plan.AggregateNode:
		return ce.estimateAggr(tx, node)
	case *plan.SortNode:
		return ce.estimateSort(tx, node)
	case *plan.LimitNode:
		return ce.estimateLimit(tx, node)
	case *plan.DistinctNode:
		return ce.estimateDistinct(tx, node)
	case *plan.UnionNode:
		return ce.estimateUnionCardinality(tx, node)
	default:
		return 0, nil
	}
}

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

// getColumnDistinctCount gets the distinct count for a column in a plan subtree
func (ce *CardinalityEstimator) getColumnDistinctCount(
	tx *transaction.TransactionContext,
	planNode plan.PlanNode,
	columnName string,
) int64 {
	if planNode == nil {
		return DefaultDistinctCount
	}

	if scanNode, ok := planNode.(*plan.ScanNode); ok {
		if ce.catalog != nil {
			colStats, err := ce.catalog.GetColumnStatistics(tx, scanNode.TableID, columnName)
			if err == nil && colStats != nil {
				return colStats.DistinctCount
			}
		}
	}

	for _, child := range planNode.GetChildren() {
		distinct := ce.getColumnDistinctCount(tx, child, columnName)
		if distinct > 0 {
			return distinct
		}
	}

	return DefaultDistinctCount
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

	tableID := ce.findBaseTableID(node.Child)

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

// estimateAggr estimates output rows for an aggregation.
// Uses actual distinct count statistics for GROUP BY columns when available,
// falling back to heuristics when statistics are unavailable.
func (ce *CardinalityEstimator) estimateAggr(
	tx *transaction.TransactionContext,
	node *plan.AggregateNode,
) (int64, error) {
	childCard, err := ce.EstimatePlanCardinality(tx, node.Child)
	if err != nil {
		return 0, err
	}

	if len(node.GroupByExprs) == 0 {
		return 1, nil
	}

	groupCard := ce.estimateGroupByDistinctCount(tx, node.Child, node.GroupByExprs)
	result := int64(math.Min(float64(groupCard), float64(childCard)))

	return int64(math.Max(1.0, float64(result))), nil
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

// estimateGroupByDistinctCount estimates the distinct count for a GROUP BY operation.
// For single columns, it attempts to use actual statistics. For multiple columns,
// it multiplies distinct counts assuming independence.
func (ce *CardinalityEstimator) estimateGroupByDistinctCount(
	tx *transaction.TransactionContext,
	planNode plan.PlanNode,
	groupByExprs []string,
) int64 {
	if len(groupByExprs) == 0 {
		return 1
	}

	// For single column, try to get actual distinct count
	if len(groupByExprs) == 1 {
		distinct := ce.getColumnDistinctCount(tx, planNode, groupByExprs[0])
		if distinct > 0 {
			return distinct
		}
		return DefaultDistinctCount
	}

	// Multi-column: multiply distinct counts (assumes independence)
	// This is an approximation; real systems use multi-column statistics
	totalDistinct := int64(1)
	foundStats := false

	for _, expr := range groupByExprs {
		distinct := ce.getColumnDistinctCount(tx, planNode, expr)
		if distinct > 0 {
			totalDistinct *= distinct
			foundStats = true
		} else {
			// Use a conservative multiplier for unknown columns
			totalDistinct *= DefaultDistinctCount
		}

		// Cap at a reasonable maximum to avoid overflow
		// and unrealistic estimates
		if totalDistinct > 1e9 {
			totalDistinct = 1e9
			break
		}
	}

	if !foundStats {
		// No statistics found, return conservative estimate
		// based on number of group-by columns
		return int64(math.Pow(float64(DefaultDistinctCount), math.Min(float64(len(groupByExprs)), 2.0)))
	}

	return totalDistinct
}

// applyCorrelationCorrection applies a correction factor for multiple predicates
// to account for possible correlation between predicates. This prevents
// over-optimistic selectivity estimates when predicates are correlated.
//
// Uses exponential backoff based on the number of predicates:
// correction = product^(1 / (1 + log(n)))
// where n is the number of predicates and product is their naive product.
func applyCorrelationCorrection(selectivities []float64) float64 {
	if len(selectivities) == 0 {
		return 1.0
	}

	if len(selectivities) == 1 {
		return selectivities[0]
	}

	// Calculate naive product (assumes independence)
	product := 1.0
	for _, sel := range selectivities {
		product *= sel
	}

	// Apply exponential backoff to account for correlation
	// The more predicates we have, the more likely they are correlated
	n := float64(len(selectivities))
	exponent := 1.0 / (1.0 + math.Log(n))
	corrected := math.Pow(product, exponent)

	// Return the maximum of product and corrected value
	// (corrected is always >= product due to exponent < 1)
	return math.Max(product, corrected)
}

// findBaseTableID walks the plan tree to find the base table ID.
// Returns 0 if no base table is found (e.g., for joins or complex plans).
func (ce *CardinalityEstimator) findBaseTableID(planNode plan.PlanNode) int {
	if planNode == nil {
		return 0
	}

	switch node := planNode.(type) {
	case *plan.ScanNode:
		return node.TableID
	case *plan.FilterNode:
		return ce.findBaseTableID(node.Child)
	case *plan.ProjectNode:
		return ce.findBaseTableID(node.Child)
	case *plan.AggregateNode:
		return ce.findBaseTableID(node.Child)
	case *plan.SortNode:
		return ce.findBaseTableID(node.Child)
	case *plan.LimitNode:
		return ce.findBaseTableID(node.Child)
	case *plan.DistinctNode:
		return ce.findBaseTableID(node.Child)
	default:
		// For joins, unions, or other multi-child nodes, return 0
		return 0
	}
}

// getColumnType retrieves the type of a column from a table in the catalog.
// Returns the column type if found, or IntType as default.
func (ce *CardinalityEstimator) getColumnType(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
) types.Type {
	// If catalog is not initialized, return default type
	if ce.catalog == nil {
		return types.IntType
	}

	// Try to get column statistics and infer type from min/max values
	colStats, err := ce.catalog.GetColumnStatistics(tx, tableID, columnName)
	if err == nil && colStats != nil {
		if colStats.MinValue != nil {
			return colStats.MinValue.Type()
		}
		if colStats.MaxValue != nil {
			return colStats.MaxValue.Type()
		}
	}

	// Try to get table metadata and load schema
	tableMetadata, err := ce.catalog.GetTableMetadataByID(tx, tableID)
	if err == nil && tableMetadata != nil {
		schema, err := ce.catalog.LoadTableSchema(tx, tableID, tableMetadata.TableName)
		if err == nil && schema != nil {
			// Search for column in schema
			for _, col := range schema.Columns {
				if col.Name == columnName {
					return col.FieldType
				}
			}
		}
	}

	// Default to IntType if we can't determine the type
	return types.IntType
}

// parsePredicateValue converts a string value to a types.Field using the column type.
// Returns nil if the value cannot be parsed.
func (ce *CardinalityEstimator) parsePredicateValue(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
	valueStr string,
) types.Field {
	if valueStr == "" {
		return nil
	}

	columnType := ce.getColumnType(tx, tableID, columnName)
	field, err := types.CreateFieldFromConstant(columnType, valueStr)
	if err != nil {
		return nil
	}

	return field
}

// estimatePredicateSelectivity estimates selectivity for a single predicate
// using the most appropriate method based on predicate type and available information.
func (ce *CardinalityEstimator) estimatePredicateSelectivity(
	tx *transaction.TransactionContext,
	tableID int,
	pred *plan.PredicateInfo,
) float64 {
	est := ce.estimator
	switch pred.Type {
	case plan.NullCheckPredicate:
		return est.EstimateNull(tx, tableID, pred.Column, pred.IsNull)

	case plan.LikePredicate:
		return est.EstimateLike(pred.Value)

	case plan.InPredicate:
		return est.EstimateIn(tx, tableID, pred.Column, len(pred.Values))

	case plan.StandardPredicate:
		if pred.Value != "" {
			field := ce.parsePredicateValue(tx, tableID, pred.Column, pred.Value)
			if field != nil {
				return est.EstimateWithValue(
					tx, pred.Predicate, tableID, pred.Column, field,
				)
			}
		}

		return est.EstimatePredicateSelectivity(
			tx, pred.Predicate, tableID, pred.Column,
		)

	default:
		return est.EstimatePredicateSelectivity(
			tx, pred.Predicate, tableID, pred.Column,
		)
	}
}
