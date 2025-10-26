package cardinality

import (
	"fmt"
	"math"
	"storemy/pkg/catalog/catalogmanager"
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
// of query plan nodes. It works in conjunction with SelectivityEstimator
// to provide cost-based query optimization.
//
// The estimator uses various techniques:
//   - Table statistics from the catalog manager (row counts, distinct values)
//   - Predicate selectivity estimation (via SelectivityEstimator)
//   - Join cardinality formulas (based on join type and predicates)
//   - Correlation correction for multiple predicates
//
// All estimates require a transaction context for accessing catalog statistics.
type CardinalityEstimator struct {
	catalog *catalogmanager.CatalogManager
	tx      *transaction.TransactionContext
}

// NewCardinalityEstimator creates a new cardinality estimator.
//
// Parameters:
//   - cat: CatalogManager for accessing table/column statistics
//   - tx: Transaction context for accessing catalog statistics
//
// Returns error if catalog is nil.
func NewCardinalityEstimator(cat *catalogmanager.CatalogManager, tx *transaction.TransactionContext) (*CardinalityEstimator, error) {
	if cat == nil {
		return nil, fmt.Errorf("cannot have nil CATALOG")
	}
	return &CardinalityEstimator{
		catalog: cat,
		tx:      tx,
	}, nil
}

// EstimatePlanCardinality estimates the output cardinality for a plan node.
// Returns the cached cardinality if already computed, otherwise recursively
// estimates based on the node type.
//
// Supported plan node types:
//   - ScanNode: Base table cardinality from statistics
//   - JoinNode: Product of inputs × join selectivity
//   - FilterNode: Input cardinality × predicate selectivity
//   - ProjectNode: Pass-through from child
//   - AggregateNode: GROUP BY distinct count estimation
//   - SortNode: Pass-through from child
//   - LimitNode: Min(limit, child cardinality)
//   - DistinctNode: Estimated distinct values
//   - Set operations (Union/Intersect/Except)
//
// Parameters:
//   - planNode: Plan node to estimate
//
// Returns estimated cardinality or error if estimation fails.
func (ce *CardinalityEstimator) EstimatePlanCardinality(planNode plan.PlanNode) (int64, error) {
	if planNode == nil {
		return 0, nil
	}

	if existingCard := planNode.GetCardinality(); existingCard > 0 {
		return existingCard, nil
	}

	switch node := planNode.(type) {
	case *plan.ScanNode:
		return ce.estimateScan(node)
	case *plan.JoinNode:
		return ce.estimateJoin(node)
	case *plan.FilterNode:
		return ce.estimateFilter(node)
	case *plan.ProjectNode:
		return ce.estimateProject(node)
	case *plan.AggregateNode:
		return ce.estimateAggr(node)
	case *plan.SortNode:
		return ce.estimateSort(node)
	case *plan.LimitNode:
		return ce.estimateLimit(node)
	case *plan.DistinctNode:
		return ce.estimateDistinct(node)
	case *plan.IntersectNode:
		return ce.estimateIntersectCardinality(node)
	case *plan.ExceptNode:
		return ce.estimateExceptCardinality(node)
	case *plan.UnionNode:
		return ce.estimateUnionCardinality(node)
	default:
		return 0, nil
	}
}

// getColumnDistinctCount retrieves the distinct count for a column by searching
// the plan tree for scan nodes with statistics. This is used for GROUP BY and
// DISTINCT cardinality estimation.
//
// Parameters:
//   - planNode: Root of plan subtree to search
//   - columnName: Name of column to get distinct count for
//
// Returns distinct count from statistics, or DefaultDistinctCount if unavailable.
func (ce *CardinalityEstimator) getColumnDistinctCount(planNode plan.PlanNode, columnName string) int64 {
	if planNode == nil {
		return DefaultDistinctCount
	}

	if scanNode, ok := planNode.(*plan.ScanNode); ok {
		if ce.catalog != nil {
			colStats, err := ce.catalog.GetColumnStatistics(ce.tx, scanNode.TableID, columnName)
			if err == nil && colStats != nil {
				return colStats.DistinctCount
			}
		}
	}

	for _, child := range planNode.GetChildren() {
		distinct := ce.getColumnDistinctCount(child, columnName)
		if distinct > 0 {
			return distinct
		}
	}

	return DefaultDistinctCount
}

// estimateAggr estimates output rows for an aggregation operation.
// Uses actual distinct count statistics for GROUP BY columns when available,
// falling back to heuristics when statistics are unavailable.
//
// Estimation logic:
//   - No GROUP BY: Returns 1 (single aggregated row)
//   - Single column GROUP BY: Uses column distinct count from statistics
//   - Multiple column GROUP BY: Multiplies distinct counts (assumes independence)
//   - Result capped at input cardinality (aggregation can't increase rows)
//
// Parameters:
//   - node: Aggregate plan node to estimate
//
// Returns estimated output cardinality, minimum 1.
func (ce *CardinalityEstimator) estimateAggr(node *plan.AggregateNode) (int64, error) {
	childCard, err := ce.EstimatePlanCardinality(node.Child)
	if err != nil {
		return 0, err
	}

	if len(node.GroupByExprs) == 0 {
		return 1, nil
	}

	groupCard := ce.estimateGroupByDistinctCount(node.Child, node.GroupByExprs)
	result := int64(math.Min(float64(groupCard), float64(childCard)))

	return int64(math.Max(1.0, float64(result))), nil
}

// estimateGroupByDistinctCount estimates the distinct count for a GROUP BY operation.
// For single columns, it attempts to use actual statistics. For multiple columns,
// it multiplies distinct counts assuming independence (a common simplification).
//
// Multi-column estimation formula:
//
//	distinct(A, B, C) ≈ distinct(A) × distinct(B) × distinct(C)
//
// This assumes column independence, which may overestimate in practice.
// Real systems use multi-column statistics or sampling for better accuracy.
//
// Parameters:
//   - planNode: Child plan node to analyze
//   - groupByExprs: List of GROUP BY column names
//
// Returns estimated distinct group count, capped at 1 billion to prevent overflow.
func (ce *CardinalityEstimator) estimateGroupByDistinctCount(planNode plan.PlanNode, groupByExprs []string) int64 {
	if len(groupByExprs) == 0 {
		return 1
	}

	// For single column, try to get actual distinct count
	if len(groupByExprs) == 1 {
		distinct := ce.getColumnDistinctCount(planNode, groupByExprs[0])
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
		distinct := ce.getColumnDistinctCount(planNode, expr)
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
// Correction formula:
//
//	corrected = product^(1 / (1 + log(n)))
//
// where:
//   - product = naive product of individual selectivities
//   - n = number of predicates
//
// The correction increases with more predicates, making estimates more conservative.
// Returns the corrected selectivity, bounded by the naive product (never more optimistic).
//
// Example:
//   - 2 predicates with 0.1 selectivity each
//   - Naive: 0.1 × 0.1 = 0.01 (1%)
//   - Corrected: 0.01^(1/(1+log(2))) ≈ 0.046 (4.6%)
//
// Parameters:
//   - selectivities: Array of individual predicate selectivities
//
// Returns corrected combined selectivity (1.0 for empty input).
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

// calculateSelectivity computes the combined selectivity of multiple predicates
// with correlation correction, then applies it to the base cardinality.
//
// This is the main entry point for filter selectivity calculation, combining:
//  1. Individual predicate selectivity estimation
//  2. Correlation correction for multiple predicates
//  3. Application to base cardinality
//
// Parameters:
//   - predicates: Array of predicates to evaluate
//   - tableID: Base table ID for statistics lookup
//   - baseCard: Input cardinality before filtering
//
// Returns estimated output cardinality after applying all predicates.
func (ce *CardinalityEstimator) calculateSelectivity(predicates []plan.PredicateInfo, tableID int, baseCard int64) int64 {
	selectivities := make([]float64, 0, len(predicates))
	for i := range predicates {
		sel := ce.estimatePredicateSelectivity(tableID, &predicates[i])
		selectivities = append(selectivities, sel)
	}

	totalSelectivity := applyCorrelationCorrection(selectivities)
	return int64(float64(baseCard) * totalSelectivity)
}

// findBaseTableID walks the plan tree to find the base table ID by recursively
// descending through single-child operators until reaching a ScanNode.
//
// This is used to associate predicates with their source table for statistics lookup.
//
// Supported traversal through:
//   - FilterNode, ProjectNode, AggregateNode, SortNode, LimitNode, DistinctNode
//
// Parameters:
//   - planNode: Root of plan subtree to search
//
// Returns:
//   - tableID: The table ID from the first ScanNode found (0 if not found)
//   - found: true if a ScanNode was found, false for nil/multi-child nodes
func findBaseTableID(planNode plan.PlanNode) (tableID int, found bool) {
	if planNode == nil {
		return 0, false
	}

	switch node := planNode.(type) {
	case *plan.ScanNode:
		return node.TableID, true
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
		return 0, false
	}
}

// getColumnType retrieves the type of a column from a table in the catalog.
// Used for parsing predicate values with correct type information.
//
// Type lookup order:
//  1. Column statistics (from MinValue/MaxValue type)
//  2. Table schema metadata
//
// Parameters:
//   - tableID: ID of table containing the column
//   - columnName: Name of column to look up
//
// Returns column type and an error if the catalog is unavailable or the column cannot be found.
func (ce *CardinalityEstimator) getColumnType(tableID int, columnName string) (types.Type, error) {
	if ce.catalog == nil {
		return 0, fmt.Errorf("catalog is nil")
	}

	colStats, err := ce.catalog.GetColumnStatistics(ce.tx, tableID, columnName)
	if err == nil && colStats != nil {
		if colStats.MinValue != nil {
			return colStats.MinValue.Type(), nil
		}
		if colStats.MaxValue != nil {
			return colStats.MaxValue.Type(), nil
		}
	}

	tableMetadata, err := ce.catalog.GetTableMetadataByID(ce.tx, tableID)
	if err != nil {
		return 0, fmt.Errorf("failed to get table metadata for table %d: %w", tableID, err)
	}
	if tableMetadata == nil {
		return 0, fmt.Errorf("table metadata not found for table %d", tableID)
	}

	schema, err := ce.catalog.LoadTableSchema(ce.tx, tableID, tableMetadata.TableName)
	if err != nil {
		return 0, fmt.Errorf("failed to load table schema for table %d: %w", tableID, err)
	}
	if schema == nil {
		return 0, fmt.Errorf("table schema not found for table %d", tableID)
	}

	for _, col := range schema.Columns {
		if col.Name == columnName {
			return col.FieldType, nil
		}
	}

	return 0, fmt.Errorf("column %s not found in table %d", columnName, tableID)
}

// parsePredicateValue converts a string value to a types.Field using the column type.
// This enables type-aware selectivity estimation for predicates like "age > 25".
//
// Parameters:
//   - tableID: Table ID for type lookup
//   - columnName: Column name for type lookup
//   - valueStr: String representation of value to parse
//
// Returns parsed Field with correct type, or error if type lookup or parsing fails.
func (ce *CardinalityEstimator) parsePredicateValue(tableID int, columnName, valueStr string) (types.Field, error) {
	if valueStr == "" {
		return nil, fmt.Errorf("value string is empty")
	}

	columnType, err := ce.getColumnType(tableID, columnName)
	if err != nil {
		return nil, fmt.Errorf("failed to get column type: %w", err)
	}

	field, err := types.CreateFieldFromConstant(columnType, valueStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create field from constant: %w", err)
	}

	return field, nil
}

// estimatePredicateSelectivity estimates selectivity for a single predicate
// using the most appropriate method based on predicate type and available information.
//
// Dispatches to specialized estimators for:
//   - NullCheckPredicate: IS NULL / IS NOT NULL (uses null count statistics)
//   - LikePredicate: LIKE pattern matching (uses pattern analysis)
//   - InPredicate: IN (...) lists (uses list length)
//   - StandardPredicate: =, <, >, <=, >= (uses histogram or range statistics)
//
// For StandardPredicate, attempts to parse the value for value-based estimation,
// falling back to predicate-only estimation if parsing fails.
//
// Parameters:
//   - tableID: Base table ID for statistics
//   - pred: Predicate information including type, column, operator, and value
//
// Returns selectivity estimate between 0.0 and 1.0.
func (ce *CardinalityEstimator) estimatePredicateSelectivity(tableID int, pred *plan.PredicateInfo) float64 {
	est := selectivity.NewSelectivityEstimator(ce.catalog, ce.tx)
	switch pred.Type {
	case plan.NullCheckPredicate:
		return est.EstimateNull(tableID, pred.Column, pred.IsNull)

	case plan.LikePredicate:
		return est.EstimateLike(pred.Value)

	case plan.InPredicate:
		return est.EstimateIn(tableID, pred.Column, len(pred.Values))

	case plan.StandardPredicate:
		if pred.Value != "" {
			field, err := ce.parsePredicateValue(tableID, pred.Column, pred.Value)
			if err == nil {
				return est.EstimateWithValue(
					pred.Predicate, tableID, pred.Column, field,
				)
			}
			// If parsing fails, fall back to predicate-only estimation
		}

		return est.EstimatePredicateSelectivity(
			pred.Predicate, tableID, pred.Column,
		)

	default:
		return est.EstimatePredicateSelectivity(
			pred.Predicate, tableID, pred.Column,
		)
	}
}
