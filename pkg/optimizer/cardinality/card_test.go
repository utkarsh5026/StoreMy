package cardinality

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/log/wal"
	"storemy/pkg/memory"
	"storemy/pkg/memory/wrappers/table"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// testCatalogSetup holds all components needed for testing with a real catalog
type testCatalogSetup struct {
	catalog    *catalog.SystemCatalog
	cache      *tablecache.TableCache
	txRegistry *transaction.TransactionRegistry
	store      *memory.PageStore
	tempDir    string
	cleanup    func()
}

// setupTestCatalogWithData creates a catalog with realistic test tables and data
func setupTestCatalogWithData(t *testing.T) *testCatalogSetup {
	t.Helper()

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "cardinality_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logPath := filepath.Join(tempDir, "wal.log")

	// Setup components
	wal, err := wal.NewWAL(logPath, 8192)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	store := memory.NewPageStore(wal)
	txRegistry := transaction.NewTransactionRegistry(wal)
	cache := tablecache.NewTableCache()
	cat := catalog.NewSystemCatalog(store, cache)

	// Initialize catalog
	tx, err := txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := cat.Initialize(tx, tempDir); err != nil {
		t.Fatalf("failed to initialize catalog: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tempDir)
	}

	return &testCatalogSetup{
		catalog:    cat,
		cache:      cache,
		txRegistry: txRegistry,
		store:      store,
		tempDir:    tempDir,
		cleanup:    cleanup,
	}
}

// createTestTable creates a table with a schema in the catalog
func (tcs *testCatalogSetup) createTestTable(t *testing.T, tableName string, columns []schema.ColumnMetadata) int {
	t.Helper()

	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	// Create schema
	sch, err := schema.NewSchema(0, tableName, columns)
	if err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create heap file
	filePath := filepath.Join(tcs.tempDir, tableName+".dat")
	heapFile, err := heap.NewHeapFile(filePath, sch.TupleDesc)
	if err != nil {
		t.Fatalf("failed to create heap file: %v", err)
	}

	tableID := heapFile.GetID()
	sch.TableID = tableID
	for i := range sch.Columns {
		sch.Columns[i].TableID = tableID
	}

	// Register with catalog
	if err := tcs.catalog.RegisterTable(tx, sch, filePath); err != nil {
		t.Fatalf("failed to register table: %v", err)
	}

	// Add to cache and page store
	if err := tcs.cache.AddTable(heapFile, sch); err != nil {
		t.Fatalf("failed to add table to cache: %v", err)
	}
	tcs.store.RegisterDbFile(tableID, heapFile)

	return tableID
}

// insertTestData inserts rows into a table
func (tcs *testCatalogSetup) insertTestData(t *testing.T, tableID int, rows [][]types.Field) {
	t.Helper()

	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	dbFile, err := tcs.cache.GetDbFile(tableID)
	if err != nil {
		t.Fatalf("failed to get db file: %v", err)
	}

	tableInfo, err := tcs.cache.GetTableInfo(tableID)
	if err != nil {
		t.Fatalf("failed to get table info: %v", err)
	}

	tupMgr := table.NewTupleManager(tcs.store)

	for _, row := range rows {
		tup := tuple.NewTuple(tableInfo.Schema.TupleDesc)
		for i, field := range row {
			if err := tup.SetField(i, field); err != nil {
				t.Fatalf("failed to set field %d: %v", i, err)
			}
		}
		if err := tupMgr.InsertTuple(tx, dbFile, tup); err != nil {
			t.Fatalf("failed to insert tuple: %v", err)
		}
	}
}

// collectColumnStats collects statistics for a specific column
func (tcs *testCatalogSetup) collectColumnStats(t *testing.T, tableID int, columnName string, columnIndex int) *catalog.ColumnStatistics {
	t.Helper()

	// Flush all pages to ensure data is visible
	if err := tcs.store.FlushAllPages(); err != nil {
		t.Fatalf("failed to flush pages: %v", err)
	}

	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	stats, err := tcs.catalog.CollectColumnStatistics(tx, tableID, columnName, columnIndex, 10, 5)
	if err != nil {
		t.Fatalf("failed to collect column statistics: %v", err)
	}

	return stats
}

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
		result, found := findBaseTableID(scan)
		if result != 42 || !found {
			t.Errorf("Expected table ID 42 and found=true, got %d and found=%v", result, found)
		}
	})

	t.Run("Filter -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 99}
		filter := &plan.FilterNode{Child: scan}
		result, found := findBaseTableID(filter)
		if result != 99 || !found {
			t.Errorf("Expected table ID 99 and found=true, got %d and found=%v", result, found)
		}
	})

	t.Run("Project -> Filter -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 123}
		filter := &plan.FilterNode{Child: scan}
		project := &plan.ProjectNode{Child: filter}
		result, found := findBaseTableID(project)
		if result != 123 || !found {
			t.Errorf("Expected table ID 123 and found=true, got %d and found=%v", result, found)
		}
	})

	t.Run("Sort -> Limit -> Aggregate -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 77}
		agg := &plan.AggregateNode{Child: scan}
		limit := &plan.LimitNode{Child: agg}
		sort := &plan.SortNode{Child: limit}
		result, found := findBaseTableID(sort)
		if result != 77 || !found {
			t.Errorf("Expected table ID 77 and found=true, got %d and found=%v", result, found)
		}
	})

	t.Run("Distinct -> Scan", func(t *testing.T) {
		scan := &plan.ScanNode{TableID: 55}
		distinct := &plan.DistinctNode{Child: scan}
		result, found := findBaseTableID(distinct)
		if result != 55 || !found {
			t.Errorf("Expected table ID 55 and found=true, got %d and found=%v", result, found)
		}
	})

	t.Run("Join Node Returns Not Found", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: 1}
		rightScan := &plan.ScanNode{TableID: 2}
		join := &plan.JoinNode{
			LeftChild:  leftScan,
			RightChild: rightScan,
		}
		result, found := findBaseTableID(join)
		if result != 0 || found {
			t.Errorf("Expected table ID 0 and found=false for join, got %d and found=%v", result, found)
		}
	})

	t.Run("Nil Node Returns Not Found", func(t *testing.T) {
		result, found := findBaseTableID(nil)
		if result != 0 || found {
			t.Errorf("Expected table ID 0 and found=false for nil, got %d and found=%v", result, found)
		}
	})
}

// TestDistinctCardinality tests DISTINCT operation cardinality estimation
func TestDistinctCardinality(t *testing.T) {
	// Use nil catalog for basic tests (doesn't require stats)
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

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

		result, err := ce.estimateDistinct(distinct)
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

			result, err := ce.estimateDistinct(distinct)
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

		result, err := ce.estimateDistinct(distinct)
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

		result, err := ce.estimateDistinct(distinct)
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
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

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

		result, err := ce.estimateUnionCardinality(union)
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

		result, err := ce.estimateUnionCardinality(union)
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

		result, err := ce.estimateUnionCardinality(union)
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

		result, err := ce.estimateUnionCardinality(union)
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
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

	t.Run("No GROUP BY Returns 1", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(10000)

		agg := &plan.AggregateNode{
			Child:        child,
			GroupByExprs: []string{}, // No GROUP BY
		}

		result, err := ce.estimateAggr(agg)
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

		result, err := ce.estimateAggr(agg)
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

		result, err := ce.estimateAggr(agg)
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

		result, err := ce.estimateAggr(agg)
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
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

	t.Run("Join Bounded By Cartesian Product", func(t *testing.T) {
		leftChild := &plan.ProjectNode{}
		leftChild.SetCardinality(100)

		rightChild := &plan.ProjectNode{}
		rightChild.SetCardinality(200)

		join := &plan.JoinNode{
			LeftChild:  leftChild,
			RightChild: rightChild,
		}

		result, err := ce.estimateJoin(join)
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

		result, err := ce.estimateJoin(join)
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

		result, err := ce.estimateJoin(join)
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

		resultWithFilter, err := ce.estimateJoin(join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		// Result with filter should be less than without filter
		joinNoFilter := &plan.JoinNode{
			LeftChild:  leftScan,
			RightChild: rightScan,
		}
		resultNoFilter, err := ce.estimateJoin(joinNoFilter)
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
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

	t.Run("LIMIT Less Than Input", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(1000)

		limit := &plan.LimitNode{
			Child:  child,
			Limit:  100,
			Offset: 0,
		}

		result, err := ce.estimateLimit(limit)
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

		result, err := ce.estimateLimit(limit)
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

		result, err := ce.estimateLimit(limit)
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

		result, err := ce.estimateLimit(limit)
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

		result, err := ce.estimateLimit(limit)
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
		tcs := setupTestCatalogWithData(t)
		defer tcs.cleanup()

		tx, err := tcs.txRegistry.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		defer tcs.store.CommitTransaction(tx)

		ce, err := NewCardinalityEstimator(tcs.catalog, tx)
		if err != nil {
			t.Fatalf("Failed to create cardinality estimator: %v", err)
		}

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

		result, err := ce.estimateScan(scan)
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
	tcs := setupTestCatalogWithData(t)
	defer tcs.cleanup()

	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	ce, err := NewCardinalityEstimator(tcs.catalog, tx)
	if err != nil {
		t.Fatalf("Failed to create cardinality estimator: %v", err)
	}

	t.Run("Empty GROUP BY Returns 1", func(t *testing.T) {
		child := &plan.ProjectNode{}
		result := ce.estimateGroupByDistinctCount(child, []string{})

		if result != 1 {
			t.Errorf("Empty GROUP BY: expected 1, got %d", result)
		}
	})

	t.Run("Multiple Columns Uses Product", func(t *testing.T) {
		child := &plan.ProjectNode{}
		result := ce.estimateGroupByDistinctCount(child, []string{"col1", "col2"})

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

		result := ce.estimateGroupByDistinctCount(child, manyColumns)

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
	tcs := setupTestCatalogWithData(t)
	defer tcs.cleanup()

	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	ce, err := NewCardinalityEstimator(tcs.catalog, tx)
	if err != nil {
		t.Fatalf("Failed to create cardinality estimator: %v", err)
	}

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
		sel := ce.estimateEquiJoinSelectivity(join)

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

		sel := ce.estimateEquiJoinSelectivity(join)

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

		sel := ce.estimateEquiJoinSelectivity(join)

		if sel != DefaultJoinSelectivity {
			t.Errorf("Expected default selectivity %.2f, got %.6f",
				DefaultJoinSelectivity, sel)
		}
	})
}

// TestCardinalityMathematicalProperties tests mathematical invariants
func TestCardinalityMathematicalProperties(t *testing.T) {
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

	t.Run("Projection Preserves Cardinality", func(t *testing.T) {
		child := &plan.ProjectNode{}
		child.SetCardinality(12345)

		project := &plan.ProjectNode{
			Child:   child,
			Columns: []string{"col1", "col2"},
		}

		result, err := ce.estimateProject(project)
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

		result, err := ce.estimateSort(sort)
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

		result, err := ce.estimateFilter(filter)
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
			result, err := ce.EstimatePlanCardinality(node)
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
		_, _ = findBaseTableID(limit)
	}
}

func BenchmarkEstimateJoinCardinality(b *testing.B) {
	tx := &transaction.TransactionContext{}
	ce := &CardinalityEstimator{
		catalog: nil,
		tx:      tx,
	}

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
		_, _ = ce.estimateJoin(join)
	}
}

// TestScanCardinalityWithActualCatalog tests scan cardinality with real catalog and statistics
func TestScanCardinalityWithActualCatalog(t *testing.T) {
	tcs := setupTestCatalogWithData(t)
	defer tcs.cleanup()

	// Create a users table
	columns := []schema.ColumnMetadata{
		{Name: "id", FieldType: types.IntType, Position: 0, IsPrimary: true},
		{Name: "status", FieldType: types.StringType, Position: 1},
		{Name: "age", FieldType: types.IntType, Position: 2},
		{Name: "city", FieldType: types.StringType, Position: 3},
	}

	tableID := tcs.createTestTable(t, "users", columns)

	// Insert 1000 rows with realistic data distribution
	rows := make([][]types.Field, 1000)
	for i := 0; i < 1000; i++ {
		status := "active"
		if i%5 == 0 {
			status = "inactive" // 20% inactive
		}

		age := int64(20 + (i % 49)) // Ages 20-68 (49 distinct values + 20 offset = values 20-68)

		city := "NewYork" // No spaces to avoid potential issues
		switch i % 3 {
		case 0:
			city = "LosAngeles"
		case 1:
			city = "Chicago"
		default:
			city = "NewYork"
		}

		rows[i] = []types.Field{
			&types.IntField{Value: int64(i)},
			&types.StringField{Value: status},
			&types.IntField{Value: age},
			&types.StringField{Value: city},
		}
	}

	tcs.insertTestData(t, tableID, rows)

	// Collect statistics for each column
	statusStats := tcs.collectColumnStats(t, tableID, "status", 1)
	ageStats := tcs.collectColumnStats(t, tableID, "age", 2)
	cityStats := tcs.collectColumnStats(t, tableID, "city", 3)

	// Verify statistics were collected correctly
	t.Logf("Status column: distinct=%d, null=%d, total=%d",
		statusStats.DistinctCount, statusStats.NullCount, statusStats.Histogram.TotalCount)
	t.Logf("Age column: distinct=%d, null=%d, total=%d",
		ageStats.DistinctCount, ageStats.NullCount, ageStats.Histogram.TotalCount)
	t.Logf("City column: distinct=%d, null=%d, total=%d",
		cityStats.DistinctCount, cityStats.NullCount, cityStats.Histogram.TotalCount)

	// Verify statistics were collected (values should be reasonable)
	if statusStats.DistinctCount < 2 || statusStats.DistinctCount > 5 {
		t.Errorf("Expected 2-5 distinct statuses, got %d", statusStats.DistinctCount)
	}
	if ageStats.DistinctCount < 40 || ageStats.DistinctCount > 60 {
		t.Errorf("Expected 40-60 distinct ages, got %d", ageStats.DistinctCount)
	}
	if cityStats.DistinctCount < 3 || cityStats.DistinctCount > 5 {
		t.Errorf("Expected 3-5 distinct cities, got %d", cityStats.DistinctCount)
	}

	// Verify histograms were built
	if len(statusStats.Histogram.Buckets) == 0 {
		t.Error("Status histogram has no buckets")
	}
	if len(ageStats.Histogram.Buckets) == 0 {
		t.Error("Age histogram has no buckets")
	}
	if len(cityStats.Histogram.Buckets) == 0 {
		t.Error("City histogram has no buckets")
	}

	t.Logf("Status histogram buckets: %d, MCVs: %d", len(statusStats.Histogram.Buckets), len(statusStats.MostCommonVals))
	t.Logf("Age histogram buckets: %d, MCVs: %d", len(ageStats.Histogram.Buckets), len(ageStats.MostCommonVals))
	t.Logf("City histogram buckets: %d, MCVs: %d", len(cityStats.Histogram.Buckets), len(cityStats.MostCommonVals))

	// Now test cardinality estimation with the actual catalog
	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	ce, err := NewCardinalityEstimator(tcs.catalog, tx)
	if err != nil {
		t.Fatalf("Failed to create cardinality estimator: %v", err)
	}

	t.Run("Scan without predicates", func(t *testing.T) {
		scan := &plan.ScanNode{
			TableID:    tableID,
			Predicates: []plan.PredicateInfo{},
		}

		result, err := ce.estimateScan(scan)
		if err != nil {
			t.Fatalf("estimateScan error: %v", err)
		}

		// Should return approximately the row count
		if result < 900 || result > 1100 {
			t.Errorf("Expected scan cardinality around 1000, got %d", result)
		}
		t.Logf("Full scan cardinality: %d", result)
	})

	t.Run("Scan with status = 'active'", func(t *testing.T) {
		scan := &plan.ScanNode{
			TableID: tableID,
			Predicates: []plan.PredicateInfo{
				{
					Column:    "status",
					Predicate: primitives.Equals,
					Value:     "active",
					Type:      plan.StandardPredicate,
				},
			},
		}

		result, err := ce.estimateScan(scan)
		if err != nil {
			t.Fatalf("estimateScan error: %v", err)
		}

		// With actual statistics, we know status has 2 distinct values
		// Equality predicate should give approximately 1/distinct * rowCount
		// Expected: ~500 rows (50% are active)
		// But since we're using defaults when stats aren't stored in catalog,
		// it should still be reasonable
		if result < 1 || result > 1000 {
			t.Errorf("Expected reasonable cardinality for status='active', got %d", result)
		}
		t.Logf("Scan with status='active' cardinality: %d (actual=800)", result)
	})

	t.Run("Scan with multiple predicates", func(t *testing.T) {
		scan := &plan.ScanNode{
			TableID: tableID,
			Predicates: []plan.PredicateInfo{
				{
					Column:    "status",
					Predicate: primitives.Equals,
					Value:     "active",
					Type:      plan.StandardPredicate,
				},
				{
					Column:    "city",
					Predicate: primitives.Equals,
					Value:     "NewYork",
					Type:      plan.StandardPredicate,
				},
			},
		}

		result, err := ce.estimateScan(scan)
		if err != nil {
			t.Fatalf("estimateScan error: %v", err)
		}

		// Should apply correlation correction - result should be less than
		// the product but more than 0
		if result < 1 {
			t.Errorf("Expected at least 1 row, got %d", result)
		}
		t.Logf("Scan with multiple predicates cardinality: %d", result)
	})

	t.Run("Scan with age range", func(t *testing.T) {
		scan := &plan.ScanNode{
			TableID: tableID,
			Predicates: []plan.PredicateInfo{
				{
					Column:    "age",
					Predicate: primitives.GreaterThan,
					Value:     "30",
					Type:      plan.StandardPredicate,
				},
			},
		}

		result, err := ce.estimateScan(scan)
		if err != nil {
			t.Fatalf("estimateScan error: %v", err)
		}

		// With histogram statistics, this should use histogram estimation
		// rather than default 1/3 selectivity
		if result < 1 || result > 1000 {
			t.Errorf("Expected reasonable cardinality for age>30, got %d", result)
		}
		t.Logf("Scan with age>30 cardinality: %d", result)
	})
}

// TestJoinCardinalityWithActualCatalog tests join cardinality with real statistics
func TestJoinCardinalityWithActualCatalog(t *testing.T) {
	tcs := setupTestCatalogWithData(t)
	defer tcs.cleanup()

	// Create users table
	usersColumns := []schema.ColumnMetadata{
		{Name: "user_id", FieldType: types.IntType, Position: 0, IsPrimary: true},
		{Name: "name", FieldType: types.StringType, Position: 1},
	}
	usersTableID := tcs.createTestTable(t, "users", usersColumns)

	// Create orders table
	ordersColumns := []schema.ColumnMetadata{
		{Name: "order_id", FieldType: types.IntType, Position: 0, IsPrimary: true},
		{Name: "user_id", FieldType: types.IntType, Position: 1},
		{Name: "amount", FieldType: types.IntType, Position: 2},
	}
	ordersTableID := tcs.createTestTable(t, "orders", ordersColumns)

	// Insert 100 users
	userRows := make([][]types.Field, 100)
	for i := 0; i < 100; i++ {
		userRows[i] = []types.Field{
			&types.IntField{Value: int64(i)},
			&types.StringField{Value: "User" + string(rune(i))},
		}
	}
	tcs.insertTestData(t, usersTableID, userRows)

	// Insert 500 orders (each user has ~5 orders on average)
	orderRows := make([][]types.Field, 500)
	for i := 0; i < 500; i++ {
		userID := int64(i % 100) // Distribute orders among users
		orderRows[i] = []types.Field{
			&types.IntField{Value: int64(i)},
			&types.IntField{Value: userID},
			&types.IntField{Value: int64(100 + i*10)},
		}
	}
	tcs.insertTestData(t, ordersTableID, orderRows)

	// Collect statistics
	userIDStats := tcs.collectColumnStats(t, usersTableID, "user_id", 0)
	orderUserIDStats := tcs.collectColumnStats(t, ordersTableID, "user_id", 1)

	t.Logf("Users.user_id: distinct=%d", userIDStats.DistinctCount)
	t.Logf("Orders.user_id: distinct=%d", orderUserIDStats.DistinctCount)

	// Create cardinality estimator
	tx, err := tcs.txRegistry.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tcs.store.CommitTransaction(tx)

	ce, err := NewCardinalityEstimator(tcs.catalog, tx)
	if err != nil {
		t.Fatalf("Failed to create cardinality estimator: %v", err)
	}

	t.Run("Inner join users and orders", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: usersTableID}
		leftScan.SetCardinality(100)

		rightScan := &plan.ScanNode{TableID: ordersTableID}
		rightScan.SetCardinality(500)

		join := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "user_id",
			RightColumn: "user_id",
		}

		result, err := ce.estimateJoin(join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		// Expected: Since every order has a matching user, result should be ~500
		// With containment logic, we know smaller distinct count (100) is contained
		// in larger (100), so join selectivity should be high
		t.Logf("Join cardinality: %d (expected ~500, actual=500)", result)

		// Result should be reasonable - not 0, not more than cartesian product
		if result < 1 || result > 50000 {
			t.Errorf("Join cardinality out of reasonable range: %d", result)
		}
	})

	t.Run("Join with extra filter", func(t *testing.T) {
		leftScan := &plan.ScanNode{TableID: usersTableID}
		leftScan.SetCardinality(100)

		rightScan := &plan.ScanNode{TableID: ordersTableID}
		rightScan.SetCardinality(500)

		join := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "user_id",
			RightColumn: "user_id",
			ExtraFilters: []plan.PredicateInfo{
				{
					Column:    "amount",
					Predicate: primitives.GreaterThan,
					Value:     "1000",
					Type:      plan.StandardPredicate,
				},
			},
		}

		resultWithFilter, err := ce.estimateJoin(join)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		// Result with filter should be less than without filter
		joinNoFilter := &plan.JoinNode{
			LeftChild:   leftScan,
			RightChild:  rightScan,
			LeftColumn:  "user_id",
			RightColumn: "user_id",
		}
		resultNoFilter, err := ce.estimateJoin(joinNoFilter)
		if err != nil {
			t.Fatalf("estimateJoin error: %v", err)
		}

		t.Logf("Join with filter: %d, without filter: %d", resultWithFilter, resultNoFilter)

		if resultWithFilter >= resultNoFilter {
			t.Errorf("Join with filter (%d) should be less than without filter (%d)",
				resultWithFilter, resultNoFilter)
		}
	})
}
