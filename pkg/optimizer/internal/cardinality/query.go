package cardinality

import (
	"math"
	"storemy/pkg/plan"
)

// estimateScan estimates output rows for a scan node.
//
// Mathematical Model:
//   - Base cardinality: Uses catalog statistics for table row count
//   - Single predicate selectivity: sel = P(predicate is true)
//   - Multiple predicates: Applies correlation correction to avoid independence assumption
//
// Formula:
//
//	outputRows = tableRows × ∏(selectivity_i)^correlationFactor
//
// Reasoning:
//   - Naive multiplication (sel₁ × sel₂ × ...) assumes predicate independence
//   - Real predicates often correlate (e.g., age > 30 AND salary > 50k)
//   - Correlation correction uses geometric mean to avoid over-aggressive filtering
//   - Without statistics, falls back to conservative DefaultTableCardinality
//
// Example:
//
//	Table with 10,000 rows, predicates: age > 30 (sel=0.6), city='NYC' (sel=0.1)
//	Naive: 10000 × 0.6 × 0.1 = 600 rows
//	With correlation correction: ~1000-1500 rows (more realistic)
func (ce *CardinalityEstimator) estimateScan(node *plan.ScanNode) (Cardinality, error) {
	tableStats, err := ce.catalog.GetTableStatistics(ce.tx, node.TableID)
	if err != nil || tableStats == nil {
		return DefaultTableCardinality, nil
	}

	tableCard := Cardinality(tableStats.Cardinality) // #nosec G115
	if len(node.Predicates) == 0 {
		return tableCard, nil
	}

	return ce.calculateSelectivity(node.Predicates, node.TableID, tableCard), nil
}

// estimateFilter estimates output rows for a filter node.
//
// Mathematical Model:
//   - Inherits child cardinality as base
//   - Applies same selectivity mathematics as estimateScan
//   - Propagates estimates up the query tree
//
// Formula:
//
//	outputRows = childRows × ∏(selectivity_i)^correlationFactor
//
// Reasoning:
//   - Filters reduce rows but don't change fundamental estimation approach
//   - Child cardinality already reflects upstream operations (joins, scans)
//   - Correlation correction prevents compounding selectivity errors
//   - Attempts to find base table for better statistics lookup
//
// Example:
//
//	Child outputs 5,000 rows, filter: status='active' (sel=0.3)
//	Output estimate: 5000 × 0.3 = 1,500 rows
func (ce *CardinalityEstimator) estimateFilter(node *plan.FilterNode) (Cardinality, error) {
	childCard, err := ce.EstimatePlanCardinality(node.Child)
	if err != nil {
		return 0, err
	}

	if len(node.Predicates) == 0 {
		return childCard, nil
	}

	tableID, _ := findBaseTableID(node.Child)
	return ce.calculateSelectivity(node.Predicates, tableID, childCard), nil
}

// estimateProject estimates output rows for a projection.
//
// Mathematical Model:
//
//	outputRows = childRows (no change)
//
// Reasoning:
//   - Projection is a schema operation, not a row-filtering operation
//   - Projects selected columns but preserves all input rows
//   - Cardinality remains unchanged through projection
//   - Only affects tuple width/schema, not tuple count
//
// Example:
//
//	SELECT id, name FROM users → same row count as input
func (ce *CardinalityEstimator) estimateProject(node *plan.ProjectNode) (Cardinality, error) {
	return ce.EstimatePlanCardinality(node.Child)
}

// estimateSort estimates output rows for a sort operation.
//
// Mathematical Model:
//
//	outputRows = childRows (no change)
//
// Reasoning:
//   - Sort reorders rows but doesn't filter them
//   - Cardinality invariant under sorting
//   - Affects physical ordering, not logical row count
//   - Important for cost estimation (O(n log n)) but not cardinality
//
// Example:
//
//	ORDER BY salary DESC → same row count, different order
func (ce *CardinalityEstimator) estimateSort(node *plan.SortNode) (Cardinality, error) {
	return ce.EstimatePlanCardinality(node.Child)
}

// estimateLimit estimates output rows for a LIMIT clause.
//
// Mathematical Model:
//
//	effectiveRows = childRows - offset
//	outputRows = min(limit, max(0, effectiveRows))
//
// Reasoning:
//   - LIMIT caps maximum output rows
//   - OFFSET skips initial rows before limiting
//   - Cannot produce negative rows (max with 0)
//   - Cannot produce more rows than available after offset
//   - Critical for pagination query optimization
//
// Examples:
//  1. Child: 100 rows, LIMIT 10, OFFSET 0 → 10 rows
//  2. Child: 100 rows, LIMIT 50, OFFSET 80 → 20 rows (100-80)
//  3. Child: 100 rows, LIMIT 10, OFFSET 200 → 0 rows
func (ce *CardinalityEstimator) estimateLimit(node *plan.LimitNode) (Cardinality, error) {
	childCard, err := ce.EstimatePlanCardinality(node.Child)
	if err != nil {
		return 0, err
	}

	limitCard := Cardinality(node.Limit)
	if limitCard <= 0 {
		return childCard, nil
	}

	effectiveCard := childCard - Cardinality(node.Offset)
	if effectiveCard < 0 {
		return 0, nil
	}

	return Cardinality(math.Min(float64(limitCard), float64(effectiveCard))), nil
}
