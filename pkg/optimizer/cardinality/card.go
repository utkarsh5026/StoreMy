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

type (
	TxCtx = *transaction.TransactionContext
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
	case *plan.IntersectNode:
		return ce.estimateIntersectCardinality(tx, node)
	case *plan.ExceptNode:
		return ce.estimateExceptCardinality(tx, node)
	case *plan.UnionNode:
		return ce.estimateUnionCardinality(tx, node)
	default:
		return 0, nil
	}
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

	product := 1.0
	for _, sel := range selectivities {
		product *= sel
	}

	n := float64(len(selectivities))
	exponent := 1.0 / (1.0 + math.Log(n))
	corrected := math.Pow(product, exponent)

	return math.Max(product, corrected)
}

func (ce *CardinalityEstimator) calculateSelectivity(tx TxCtx, predicates []plan.PredicateInfo, tableID int, baseCard int64) int64 {
	selectivities := make([]float64, 0, len(predicates))
	for i := range predicates {
		sel := ce.estimatePredicateSelectivity(tx, tableID, &predicates[i])
		selectivities = append(selectivities, sel)
	}

	totalSelectivity := applyCorrelationCorrection(selectivities)
	return int64(float64(baseCard) * totalSelectivity)
}

// findBaseTableID walks the plan tree to find the base table ID.
// Returns 0 if no base table is found (e.g., for joins or complex plans).
func findBaseTableID(planNode plan.PlanNode) int {
	if planNode == nil {
		return 0
	}

	switch node := planNode.(type) {
	case *plan.ScanNode:
		return node.TableID
	case *plan.FilterNode:
		return findBaseTableID(node.Child)
	case *plan.ProjectNode:
		return findBaseTableID(node.Child)
	case *plan.AggregateNode:
		return findBaseTableID(node.Child)
	case *plan.SortNode:
		return findBaseTableID(node.Child)
	case *plan.LimitNode:
		return findBaseTableID(node.Child)
	case *plan.DistinctNode:
		return findBaseTableID(node.Child)
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
	if ce.catalog == nil {
		return types.IntType
	}

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
	columnName,
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
