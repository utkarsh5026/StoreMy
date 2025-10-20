package cardinality

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/selectivity"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"testing"
)

// TestCorrelationCorrection tests the correlation correction for multiple predicates
func TestCorrelationCorrection(t *testing.T) {
	tests := []struct {
		name          string
		selectivities []float64
		expectMin     float64 // Corrected should be >= naive product
		expectMax     float64 // Corrected should be <= 1.0
		description   string
	}{
		{
			name:          "Single predicate",
			selectivities: []float64{0.1},
			expectMin:     0.1,
			expectMax:     0.1,
			description:   "Single predicate returns same selectivity",
		},
		{
			name:          "Two independent predicates",
			selectivities: []float64{0.1, 0.2},
			expectMin:     0.02, // 0.1 * 0.2 = 0.02 (naive)
			expectMax:     0.15, // Corrected with exponential backoff
			description:   "Two predicates should apply backoff",
		},
		{
			name:          "Three correlated predicates",
			selectivities: []float64{0.1, 0.1, 0.1},
			expectMin:     0.001, // 0.1^3 = 0.001 (naive)
			expectMax:     0.05,  // Significant correction expected
			description:   "Three similar predicates likely correlated",
		},
		{
			name:          "Many predicates",
			selectivities: []float64{0.5, 0.5, 0.5, 0.5, 0.5},
			expectMin:     0.03125, // 0.5^5 = 0.03125 (naive)
			expectMax:     0.3,     // Heavy correction for many predicates
			description:   "Five predicates should have strong correction",
		},
		{
			name:          "Empty predicates",
			selectivities: []float64{},
			expectMin:     1.0,
			expectMax:     1.0,
			description:   "No predicates returns 1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyCorrelationCorrection(tt.selectivities)

			if result < tt.expectMin || result > tt.expectMax {
				t.Errorf("%s: got %.6f, expected range [%.6f, %.6f]",
					tt.description, result, tt.expectMin, tt.expectMax)
			}

			// Verify result is in valid range
			if result < 0.0 || result > 1.0 {
				t.Errorf("Result out of valid range: %.6f", result)
			}

			t.Logf("%s: selectivity = %.6f (expected [%.6f, %.6f])",
				tt.description, result, tt.expectMin, tt.expectMax)
		})
	}
}

// TestCorrelationCorrectionMathProperties tests mathematical properties
func TestCorrelationCorrectionMathProperties(t *testing.T) {
	t.Run("Corrected >= Naive Product", func(t *testing.T) {
		// Correction should always be >= naive product (less optimistic)
		testCases := [][]float64{
			{0.1, 0.1},
			{0.1, 0.2, 0.3},
			{0.05, 0.05, 0.05, 0.05},
		}

		for _, sels := range testCases {
			corrected := applyCorrelationCorrection(sels)
			naive := 1.0
			for _, s := range sels {
				naive *= s
			}

			if corrected < naive-0.0001 { // Allow tiny floating point error
				t.Errorf("Corrected (%.6f) should be >= naive (%.6f) for %v",
					corrected, naive, sels)
			}
		}
	})

	t.Run("Monotonicity - More Predicates = More Correction", func(t *testing.T) {
		// With same selectivities, more predicates should give less optimistic result
		sel2 := applyCorrelationCorrection([]float64{0.1, 0.1})
		sel3 := applyCorrelationCorrection([]float64{0.1, 0.1, 0.1})
		sel4 := applyCorrelationCorrection([]float64{0.1, 0.1, 0.1, 0.1})

		// Calculate naive products for comparison
		naive2 := 0.01
		naive3 := 0.001
		naive4 := 0.0001

		// Correction factor should increase with more predicates
		factor2 := sel2 / naive2
		factor3 := sel3 / naive3
		factor4 := sel4 / naive4

		if factor3 <= factor2 || factor4 <= factor3 {
			t.Errorf("Correction factor should increase: factor2=%.2f, factor3=%.2f, factor4=%.2f",
				factor2, factor3, factor4)
		}

		t.Logf("Correction factors: 2 preds=%.2f, 3 preds=%.2f, 4 preds=%.2f",
			factor2, factor3, factor4)
	})
}

// TestFindBaseTableID tests the plan tree traversal for finding base table
func TestFindBaseTableID(t *testing.T) {
	t.Run("Direct Scan Node", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 42}
		result := findBaseTableID(scan)
		if result != 42 {
			t.Errorf("Expected table ID 42, got %d", result)
		}
	})

	t.Run("Filter -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 99}
		filter := &plan.FilterNode{Child: scan}
		result := findBaseTableID(filter)
		if result != 99 {
			t.Errorf("Expected table ID 99, got %d", result)
		}
	})

	t.Run("Project -> Filter -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 123}
		filter := &plan.FilterNode{Child: scan}
		project := &plan.ProjectNode{Child: filter}
		result := findBaseTableID(project)
		if result != 123 {
			t.Errorf("Expected table ID 123, got %d", result)
		}
	})

	t.Run("Sort -> Limit -> Aggregate -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 77}
		agg := &plan.AggregateNode{Child: scan}
		limit := &plan.LimitNode{Child: agg}
		sort := &plan.SortNode{Child: limit}
		result := findBaseTableID(sort)
		if result != 77 {
			t.Errorf("Expected table ID 77, got %d", result)
		}
	})

	t.Run("Distinct -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 55}
		distinct := &plan.DistinctNode{Child: scan}
		result := findBaseTableID(distinct)
		if result != 55 {
			t.Errorf("Expected table ID 55, got %d", result)
		}
	})

	t.Run("Join Node Returns 0", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: 1}
		rightScan := &plan.ScanNode{TableID: 2}
		join := &plan.JoinNode{
			LeftChild:  leftScan,
			RightChild: rightScan,
		}
		result := findBaseTableID(join)
		if result != 0 {
			t.Errorf("Expected table ID 0 for join, got %d", result)
		}
	})

	t.Run("Nil Node Returns 0", func(t *testing.T) {
		result := findBaseTableID(nil)
		if result != 0 {
			t.Errorf("Expected table ID 0 for nil, got %d", result)
		}
	})
}

// TestDistinctCardinality tests DISTINCT operation cardinality estimation
func TestDistinctCardinality(t *testing.T) {
	// Use nil catalog for basic tests (doesn't require stats)
	ce := &CardinalityEstimator{
		catalog:   nil,
		estimator: nil,
	}
	tx := &transaction.TransactionContext{}

	t.Run("DISTINCT All Columns", func(t *testing.T) {
		// Child produces 1000 rows
		// Use a simple node that doesn't require catalog
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		// DISTINCT on all columns (no specific columns)
		distinct := &plan.DistinctNode{
			Child:         child,
			DistinctExprs: []string{}, // Empty = all columns
		}

		result, err := ce.estimateDistinct(tx, distinct)
		if err != nil {
			t.Fatalf("estimateDistinct error: %v", err)
		}

		// Should apply 80% reduction (typical duplicate ratio)
		expectedMin := int64(700) // 70% of 1000
		expectedMax := int64(900) // 90% of 1000

		if result < expectedMin || result > expectedMax {
			t.Errorf("Expected DISTINCT cardinality in range [%d, %d], got %d",
				expectedMin, expectedMax, result)
		}

		t.Logf("DISTINCT all columns: input=1000, output=%d (%.1f%%)",
			result, float64(result)/10.0)
	})

	t.Run("DISTINCT Never Exceeds Input", func(t *testing.T) {
		testCases := []int64{10, 100, 1000, 10000}

		for _, childCard := range testCases {
			child := &plan.ProjectNode{}
			child.SetCardinality(childCard)

			distinct := &plan.DistinctNode{
				Child:         child,
				DistinctExprs: []string{},
			}

			result, err := ce.estimateDistinct(tx, distinct)
			if err != nil {
				t.Fatalf("estimateDistinct error: %v", err)
			}

			if result > childCard {
				t.Errorf("DISTINCT cardinality %d exceeds input %d",
					result, childCard)
			}
		}
	})

	t.Run("DISTINCT On Empty Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(0)

		distinct := &plan.DistinctNode{
			Child:         child,
			DistinctExprs: []string{},
		}

		result, err := ce.estimateDistinct(tx, distinct)
		if err != nil {
			t.Fatalf("estimateDistinct error: %v", err)
		}

		if result != 0 {
			t.Errorf("Expected 0 for empty input, got %d", result)
		}
	})

	t.Run("DISTINCT Minimum 1 Row", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1)

		distinct := &plan.DistinctNode{
			Child:         child,
			DistinctExprs: []string{},
		}

		result, err := ce.estimateDistinct(tx, distinct)
		if err != nil {
			t.Fatalf("estimateDistinct error: %v", err)
		}

		if result < 1 {
			t.Errorf("Expected at least 1 row, got %d", result)
		}
	})
}

// TestUnionCardinality tests UNION and UNION ALL cardinality estimation
func TestUnionCardinality(t *testing.T) {
	ce := &CardinalityEstimator{}
	tx := &transaction.TransactionContext{}

	t.Run("UNION ALL Simple Addition", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(1000)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(500)

		union := &plan.UnionNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
			UnionAll:   true,
		}

		result, err := ce.estimateUnionCardinality(tx, union)
		if err != nil {
			t.Fatalf("estimateUnionCardinality error: %v", err)
		}

		expected := int64(1500)
		if result != expected {
			t.Errorf("UNION ALL: expected %d, got %d", expected, result)
		}
	})

	t.Run("UNION With Deduplication - Similar Sizes", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(1000)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(900)

		union := &plan.UnionNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
			UnionAll:   false, // UNION with dedup
		}

		result, err := ce.estimateUnionCardinality(tx, union)
		if err != nil {
			t.Fatalf("estimateUnionCardinality error: %v", err)
		}

		// Similar sizes: ~15% overlap expected
		// Result should be between max(1000, 900) and 1000+900
		minExpected := int64(1000)     // At least the larger set
		maxExpected := int64(1900)     // At most the sum
		typicalExpected := int64(1700) // ~1900 - 15% of 900

		if result < minExpected || result > maxExpected {
			t.Errorf("UNION similar sizes: expected range [%d, %d], got %d",
				minExpected, maxExpected, result)
		}

		t.Logf("UNION similar sizes: left=1000, right=900, result=%d (expected ~%d)",
			result, typicalExpected)
	})

	t.Run("UNION With Deduplication - Small vs Large", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(10000)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(500) // 5% of left

		union := &plan.UnionNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
			UnionAll:   false,
		}

		result, err := ce.estimateUnionCardinality(tx, union)
		if err != nil {
			t.Fatalf("estimateUnionCardinality error: %v", err)
		}

		// Small set likely mostly contained in large set
		// Result should be closer to 10000 than to 10500
		minExpected := int64(10000) // At least the larger set
		maxExpected := int64(10500) // At most the sum

		if result < minExpected || result > maxExpected {
			t.Errorf("UNION small vs large: expected range [%d, %d], got %d",
				minExpected, maxExpected, result)
		}

		// Result should be significantly less than sum due to overlap
		if result > int64(10300) {
			t.Errorf("UNION should account for containment: got %d, expected < 10300",
				result)
		}

		t.Logf("UNION containment: left=10000, right=500, result=%d", result)
	})

	t.Run("UNION With Zero Input", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(0)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(100)

		union := &plan.UnionNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
			UnionAll:   false,
		}

		result, err := ce.estimateUnionCardinality(tx, union)
		if err != nil {
			t.Fatalf("estimateUnionCardinality error: %v", err)
		}

		if result < 100 || result > 100 {
			t.Errorf("UNION with zero: expected 100, got %d", result)
		}
	})
}

// TestAggregateCardinality tests improved aggregate cardinality estimation
func TestAggregateCardinality(t *testing.T) {
	ce := &CardinalityEstimator{
		catalog: nil,
	}
	tx := &transaction.TransactionContext{}

	t.Run("No GROUP BY Returns 1", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(10000)

		agg := &plan.AggregateNode{
			Child:        child,
			GroupByExprs: []string{}, // No GROUP BY
		}

		result, err := ce.estimateAggr(tx, agg)
		if err != nil {
			t.Fatalf("estimateAggr error: %v", err)
		}

		if result != 1 {
			t.Errorf("Aggregate without GROUP BY: expected 1, got %d", result)
		}
	})

	t.Run("GROUP BY Never Exceeds Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		agg := &plan.AggregateNode{
			Child:        child,
			GroupByExprs: []string{"status"},
		}

		result, err := ce.estimateAggr(tx, agg)
		if err != nil {
			t.Fatalf("estimateAggr error: %v", err)
		}

		if result > 1000 {
			t.Errorf("GROUP BY cardinality %d exceeds input 1000", result)
		}
	})

	t.Run("GROUP BY Minimum 1", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1)

		agg := &plan.AggregateNode{
			Child:        child,
			GroupByExprs: []string{"id"},
		}

		result, err := ce.estimateAggr(tx, agg)
		if err != nil {
			t.Fatalf("estimateAggr error: %v", err)
		}

		if result < 1 {
			t.Errorf("GROUP BY should return at least 1, got %d", result)
		}
	})

	t.Run("Multiple GROUP BY Columns", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(10000)

		agg := &plan.AggregateNode{
			Child:        child,
			GroupByExprs: []string{"col1", "col2", "col3"},
		}

		result, err := ce.estimateAggr(tx, agg)
		if err != nil {
			t.Fatalf("estimateAggr error: %v", err)
		}

		// Should use product of distinct counts (with default fallback)
		// But bounded by input cardinality
		if result < 1 || result > 10000 {
			t.Errorf("Multi-column GROUP BY out of valid range: %d", result)
		}

		t.Logf("Multi-column GROUP BY: input=10000, output=%d", result)
	})
}

// TestJoinCardinalityWithContainment tests improved join selectivity
func TestJoinCardinalityWithContainment(t *testing.T) {
	ce := &CardinalityEstimator{
		catalog:   nil,
		estimator: selectivity.NewSelectivityEstimator(nil),
	}
	tx := &transaction.TransactionContext{}

	t.Run("Join Bounded By Cartesian Product", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(100)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(200)

		join := &plan.JoinNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
		}

		result, err := ce.estimateJoin(tx, join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		cartesian := int64(100 * 200)
		if result > cartesian {
			t.Errorf("Join cardinality %d exceeds cartesian product %d",
				result, cartesian)
		}
	})

	t.Run("Join Minimum 1", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(10)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(10)

		join := &plan.JoinNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
		}

		result, err := ce.estimateJoin(tx, join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		if result < 1 {
			t.Errorf("Join should return at least 1, got %d", result)
		}
	})

	t.Run("Join With Zero Input Returns 0", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(0)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(100)

		join := &plan.JoinNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
		}

		result, err := ce.estimateJoin(tx, join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		if result != 0 {
			t.Errorf("Join with zero input: expected 0, got %d", result)
		}
	})

	t.Run("Join With Extra Filters", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: 1}
		leftScan.SetCardinality(1000)

		rightScan := &plan.ScanNode{TableID: 2}
		rightScan.SetCardinality(500)

		join := &plan.JoinNode{
			LeftChild:  leftScan,
			RightChild: rightScan,
			ExtraFilters: []plan.PredicateInfo{
				{
					Column:    "status",
					Predicate: primitives.Equals,
					Value:     "active",
					Type:      plan.StandardPredicate,
				},
			},
		}

		resultWithFilter, err := ce.estimateJoin(tx, join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		// Result with filter should be less than without filter
		joinNoFilter := &plan.JoinNode{
			LeftChild:  leftScan,
			RightChild: rightScan,
		}
		resultNoFilter, err := ce.estimateJoin(tx, joinNoFilter)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		if resultWithFilter >= resultNoFilter {
			t.Errorf("Join with filter (%d) should be < join without filter (%d)",
				resultWithFilter, resultNoFilter)
		}

		t.Logf("Join: no filter=%d, with filter=%d (reduction=%.1f%%)",
			resultNoFilter, resultWithFilter,
			(1.0-float64(resultWithFilter)/float64(resultNoFilter))*100)
	})
}

// TestLimitCardinality tests LIMIT/OFFSET cardinality
func TestLimitCardinality(t *testing.T) {
	ce := &CardinalityEstimator{}
	tx := &transaction.TransactionContext{}

	t.Run("LIMIT Less Than Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  100,
			Offset: 0,
		}

		result, err := ce.estimateLimit(tx, limit)
		if err != nil {
			t.Fatalf("estimateLimit error: %v", err)
		}

		if result != 100 {
			t.Errorf("LIMIT 100: expected 100, got %d", result)
		}
	})

	t.Run("LIMIT Greater Than Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(50)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  100,
			Offset: 0,
		}

		result, err := ce.estimateLimit(tx, limit)
		if err != nil {
			t.Fatalf("estimateLimit error: %v", err)
		}

		if result != 50 {
			t.Errorf("LIMIT 100 on 50 rows: expected 50, got %d", result)
		}
	})

	t.Run("LIMIT With OFFSET", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  100,
			Offset: 200,
		}

		result, err := ce.estimateLimit(tx, limit)
		if err != nil {
			t.Fatalf("estimateLimit error: %v", err)
		}

		if result != 100 {
			t.Errorf("LIMIT 100 OFFSET 200: expected 100, got %d", result)
		}
	})

	t.Run("OFFSET Exceeds Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(100)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  50,
			Offset: 200,
		}

		result, err := ce.estimateLimit(tx, limit)
		if err != nil {
			t.Fatalf("estimateLimit error: %v", err)
		}

		if result != 0 {
			t.Errorf("OFFSET exceeds input: expected 0, got %d", result)
		}
	})

	t.Run("Zero LIMIT Returns All", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  0, // 0 means no limit
			Offset: 0,
		}

		result, err := ce.estimateLimit(tx, limit)
		if err != nil {
			t.Fatalf("estimateLimit error: %v", err)
		}

		if result != 1000 {
			t.Errorf("LIMIT 0 (no limit): expected 1000, got %d", result)
		}
	})
}

// TestScanCardinalityBounds tests scan cardinality bounds
func TestScanCardinalityBounds(t *testing.T) {
	t.Run("Scan Cardinality Non-Negative", func(t *testing.T) {
		ce := &CardinalityEstimator{
			catalog:   nil, // Use nil instead of uninitialized catalog
			estimator: selectivity.NewSelectivityEstimator(nil),
		}
		tx := &transaction.TransactionContext{}

		scan := &plan.ScanNode{
			TableID: 999, // Non-existent table
			Predicates: []plan.PredicateInfo{
				{
					Column:    "x",
					Predicate: primitives.Equals,
					Value:     "test",
					Type:      plan.StandardPredicate,
				},
			},
		}

		result, err := ce.estimateScan(tx, scan)
		if err != nil {
			t.Fatalf("estimateScan error: %v", err)
		}

		if result < 0 {
			t.Errorf("Scan cardinality should be non-negative, got %d", result)
		}

		// Should use default when table not found
		if result != DefaultTableCardinality {
			t.Logf("Scan on non-existent table: got %d (expected default %d)",
				result, DefaultTableCardinality)
		}
	})
}

// TestGroupByDistinctCountEstimation tests GROUP BY distinct count helper
func TestGroupByDistinctCountEstimation(t *testing.T) {
	ce := &CardinalityEstimator{
		catalog: nil,
	}
	tx := &transaction.TransactionContext{}

	t.Run("Empty GROUP BY Returns 1", func(t *testing.T) {
		child := &plan.ProjectNode{}
		result := ce.estimateGroupByDistinctCount(tx, child, []string{})

		if result != 1 {
			t.Errorf("Empty GROUP BY: expected 1, got %d", result)
		}
	})

	t.Run("Multiple Columns Uses Product", func(t *testing.T) {
		child := &plan.ProjectNode{}
		result := ce.estimateGroupByDistinctCount(tx, child, []string{"col1", "col2"})

		// Should use default distinct count product
		expectedMin := int64(DefaultDistinctCount) // At least one column's distinct
		expectedMax := int64(DefaultDistinctCount * DefaultDistinctCount)

		if result < expectedMin || result > expectedMax {
			t.Errorf("Multi-column GROUP BY: expected range [%d, %d], got %d",
				expectedMin, expectedMax, result)
		}
	})

	t.Run("Overflow Protection", func(t *testing.T) {
		// Many columns should not overflow
		child := &plan.ProjectNode{}
		manyColumns := make([]string, 20)
		for i := 0; i < 20; i++ {
			manyColumns[i] = "col" + string(rune(i))
		}

		result := ce.estimateGroupByDistinctCount(tx, child, manyColumns)

		// Should be capped at reasonable maximum
		maxAllowed := int64(1e9)
		if result > maxAllowed {
			t.Errorf("GROUP BY distinct count overflow: got %d, max %d",
				result, maxAllowed)
		}
	})
}

// TestEquiJoinSelectivityContainment tests improved join selectivity with containment
func TestEquiJoinSelectivityContainment(t *testing.T) {
	ce := &CardinalityEstimator{
		catalog: nil,
	}
	tx := &transaction.TransactionContext{}

	t.Run("Containment Scenario", func(t *testing.T) {
		// Small distinct count (100) vs large distinct count (10000)
		// Should use smaller distinct count (containment)
		leftScan := &plan.ScanNode{TableID: 1}
		rightScan := &plan.ScanNode{TableID: 2}

		join := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "left_id",
			RightColumn: "right_id",
		}

		// Can't test exact values without mock catalog,
		// but we can test the logic doesn't panic
		sel := ce.estimateEquiJoinSelectivity(tx, join)

		if sel < 0.0 || sel > 1.0 {
			t.Errorf("Join selectivity out of range: %.6f", sel)
		}

		t.Logf("Join selectivity (containment scenario): %.6f", sel)
	})

	t.Run("Similar Sizes Scenario", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: 1}
		rightScan := &plan.ScanNode{TableID: 2}

		join := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "id",
			RightColumn: "id",
		}

		sel := ce.estimateEquiJoinSelectivity(tx, join)

		if sel < 0.0 || sel > 1.0 {
			t.Errorf("Join selectivity out of range: %.6f", sel)
		}

		// With no statistics, should use default
		if sel != DefaultJoinSelectivity {
			t.Logf("Join selectivity (no stats): %.6f (expected default %.2f)",
				sel, DefaultJoinSelectivity)
		}
	})

	t.Run("Zero Distinct Count Returns Default", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: 999}
		rightScan := &plan.ScanNode{TableID: 998}

		join := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "nonexistent",
			RightColumn: "nonexistent",
		}

		sel := ce.estimateEquiJoinSelectivity(tx, join)

		if sel != DefaultJoinSelectivity {
			t.Errorf("Expected default selectivity %.2f, got %.6f",
				DefaultJoinSelectivity, sel)
		}
	})
}

// TestCardinalityMathematicalProperties tests mathematical invariants
func TestCardinalityMathematicalProperties(t *testing.T) {
	ce := &CardinalityEstimator{
		catalog:   nil,
		estimator: selectivity.NewSelectivityEstimator(nil),
	}
	tx := &transaction.TransactionContext{}

	t.Run("Projection Preserves Cardinality", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(12345)

		project := &plan.ProjectNode{
			Child:   child,
			Columns: []string{"col1", "col2"},
		}

		result, err := ce.estimateProject(tx, project)
		if err != nil {
			t.Fatalf("estimateProject error: %v", err)
		}

		if result != 12345 {
			t.Errorf("Projection should preserve cardinality: expected 12345, got %d",
				result)
		}
	})

	t.Run("Sort Preserves Cardinality", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(9999)

		sort := &plan.SortNode{
			Child:    child,
			SortKeys: []string{"col1"},
		}

		result, err := ce.estimateSort(tx, sort)
		if err != nil {
			t.Fatalf("estimateSort error: %v", err)
		}

		if result != 9999 {
			t.Errorf("Sort should preserve cardinality: expected 9999, got %d",
				result)
		}
	})

	t.Run("Filter Reduces Cardinality", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 1}
		scan.SetCardinality(1000)

		filter := &plan.FilterNode{
			Child: scan,
			Predicates: []plan.PredicateInfo{
				{
					Column:    "status",
					Predicate: primitives.Equals,
					Value:     "active",
					Type:      plan.StandardPredicate,
				},
			},
		}

		result, err := ce.estimateFilter(tx, filter)
		if err != nil {
			t.Fatalf("estimateFilter error: %v", err)
		}

		if result > 1000 {
			t.Errorf("Filter should not increase cardinality: got %d > 1000", result)
		}

		if result < 0 {
			t.Errorf("Filter cardinality should be non-negative: got %d", result)
		}

		t.Logf("Filter reduces cardinality: 1000 -> %d (%.1f%%)",
			result, float64(result)/10.0)
	})

	t.Run("All Estimates Are Non-Negative", func(t *testing.T) {
		// Test various node types return non-negative cardinalities
		child := &plan.ProjectNode{}
		child.SetCardinality(100)

		testNodes := []plan.PlanNode{
			&plan.ProjectNode{Child: child},
			&plan.SortNode{Child: child},
			&plan.LimitNode{Child: child, Limit: 10},
			&plan.DistinctNode{Child: child},
			&plan.AggregateNode{Child: child, GroupByExprs: []string{"col"}},
		}

		for _, node := range testNodes {
			result, err := ce.EstimatePlanCardinality(tx, node)
			if err != nil {
				t.Fatalf("EstimatePlanCardinality error: %v", err)
			}
			if result < 0 {
				t.Errorf("Node %T returned negative cardinality: %d",
					node, result)
			}
		}
	})
}

// Benchmark tests
func BenchmarkCorrelationCorrection(b *testing.B) {
	selectivities := []float64{0.1, 0.2, 0.15, 0.25, 0.3}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = applyCorrelationCorrection(selectivities)
	}
}

func BenchmarkFindBaseTableID(b *testing.B) {
	// Create a deep plan tree
	scan := &plan.ScanNode{TableID: 1}
	filter := &plan.FilterNode{Child: scan}
	project := &plan.ProjectNode{Child: filter}
	sort := &plan.SortNode{Child: project}
	limit := &plan.LimitNode{Child: sort}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = findBaseTableID(limit)
	}
}

func BenchmarkEstimateJoinCardinality(b *testing.B) {
	ce := &CardinalityEstimator{
		catalog:   nil,
		estimator: selectivity.NewSelectivityEstimator(nil),
	}
	tx := &transaction.TransactionContext{}

	leftScan := &plan.ScanNode{TableID: 1}
	leftScan.SetCardinality(10000)

	rightScan := &plan.ScanNode{TableID: 2}
	rightScan.SetCardinality(5000)

	join := &plan.JoinNode{
		LeftChild:   leftScan,
		RightChild:  rightScan,
		LeftColumn:  "id",
		RightColumn: "user_id",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ce.estimateJoin(tx, join)
	}
}
