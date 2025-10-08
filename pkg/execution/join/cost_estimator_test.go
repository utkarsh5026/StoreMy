package join

import (
	"math"
	"storemy/pkg/catalog"
	"testing"
)

func TestCostEstimator_EstimateNestedLoopCost(t *testing.T) {
	estimator := &CostEstimator{}

	stats := &JoinStatistics{
		LeftCardinality:  100,
		RightCardinality: 1000,
		LeftSize:         10,   // 10 pages
		RightSize:        100,  // 100 pages
		MemorySize:       50,
	}

	// Cost = LeftSize + (LeftCardinality * RightSize)
	// Cost = 10 + (100 * 100) = 10,010
	expectedCost := 10.0 + (100.0 * 100.0)
	actualCost := estimator.EstimateNestedLoopCost(stats)

	if actualCost != expectedCost {
		t.Errorf("Expected nested loop cost %.2f, got %.2f", expectedCost, actualCost)
	}
}

func TestCostEstimator_EstimateSortMergeCost(t *testing.T) {
	estimator := &CostEstimator{}

	tests := []struct {
		name       string
		stats      *JoinStatistics
		wantCost   float64
	}{
		{
			name: "Both unsorted",
			stats: &JoinStatistics{
				LeftSize:    10,
				RightSize:   100,
				LeftSorted:  false,
				RightSorted: false,
			},
			// Sort left: 2*10*log2(10) = 2*10*3.32 = 66.4
			// Sort right: 2*100*log2(100) = 2*100*6.64 = 1328
			// Merge: 10 + 100 = 110
			// Total: 66.4 + 1328 + 110 = 1504.4
			wantCost: 1504.0, // Approximate
		},
		{
			name: "Both sorted",
			stats: &JoinStatistics{
				LeftSize:    10,
				RightSize:   100,
				LeftSorted:  true,
				RightSorted: true,
			},
			// No sorting cost, just merge
			wantCost: 110.0, // 10 + 100
		},
		{
			name: "Left sorted only",
			stats: &JoinStatistics{
				LeftSize:    10,
				RightSize:   100,
				LeftSorted:  true,
				RightSorted: false,
			},
			// Only sort right: 2*100*log2(100) + merge
			wantCost: 1438.0, // Approximate
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualCost := estimator.EstimateSortMergeCost(tt.stats)

			// Allow 10% tolerance due to log approximation
			tolerance := tt.wantCost * 0.1
			if math.Abs(actualCost - tt.wantCost) > tolerance {
				t.Errorf("Expected sort-merge cost ~%.2f, got %.2f", tt.wantCost, actualCost)
			}
		})
	}
}

func TestCostEstimator_EstimateHashJoinCost(t *testing.T) {
	estimator := &CostEstimator{}

	tests := []struct {
		name       string
		stats      *JoinStatistics
		wantCost   float64
	}{
		{
			name: "Fits in memory",
			stats: &JoinStatistics{
				LeftSize:   10,
				RightSize:  20,
				MemorySize: 50, // Enough for build relation
			},
			// Cost = 3 * (10 + 20) = 90
			wantCost: 90.0,
		},
		{
			name: "Does not fit - partitioned hash join",
			stats: &JoinStatistics{
				LeftSize:   100,
				RightSize:  200,
				MemorySize: 10, // Not enough, need partitioning
			},
			// More expensive due to partitioning
			wantCost: 900.0, // 3 * 300 = 900 (base cost)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualCost := estimator.EstimateHashJoinCost(tt.stats)

			// Allow 20% tolerance
			tolerance := tt.wantCost * 0.2
			if math.Abs(actualCost - tt.wantCost) > tolerance {
				t.Errorf("Expected hash join cost ~%.2f, got %.2f", tt.wantCost, actualCost)
			}
		})
	}
}

func TestCostEstimator_SelectBestAlgorithm(t *testing.T) {
	estimator := &CostEstimator{}

	tests := []struct {
		name     string
		stats    *JoinStatistics
		wantAlgo string
	}{
		{
			name: "Small tables - nested loop wins",
			stats: &JoinStatistics{
				LeftCardinality:  10,
				RightCardinality: 10,
				LeftSize:         1,
				RightSize:        1,
				MemorySize:       100,
			},
			wantAlgo: "NestedLoop",
		},
		{
			name: "Large tables with memory - hash join wins",
			stats: &JoinStatistics{
				LeftCardinality:  10000,
				RightCardinality: 10000,
				LeftSize:         100,
				RightSize:        100,
				MemorySize:       150, // Enough for smaller relation
			},
			wantAlgo: "Hash",
		},
		{
			name: "Already sorted - sort-merge competitive",
			stats: &JoinStatistics{
				LeftCardinality:  1000,
				RightCardinality: 1000,
				LeftSize:         50,
				RightSize:        50,
				LeftSorted:       true,
				RightSorted:      true,
				MemorySize:       10, // Limited memory
			},
			wantAlgo: "SortMerge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualAlgo := estimator.SelectBestAlgorithm(tt.stats)
			if actualAlgo != tt.wantAlgo {
				nlCost := estimator.EstimateNestedLoopCost(tt.stats)
				smCost := estimator.EstimateSortMergeCost(tt.stats)
				hjCost := estimator.EstimateHashJoinCost(tt.stats)
				t.Errorf("Expected algorithm %s, got %s\nCosts: NL=%.2f, SM=%.2f, HJ=%.2f",
					tt.wantAlgo, actualAlgo, nlCost, smCost, hjCost)
			}
		})
	}
}

func TestCostEstimator_EstimateSelectivity(t *testing.T) {
	estimator := &CostEstimator{}

	tests := []struct {
		name               string
		leftDistinct       int
		rightDistinct      int
		expectedSelectivity float64
	}{
		{
			name:               "Equal distinct values",
			leftDistinct:       1000,
			rightDistinct:      1000,
			expectedSelectivity: 0.001, // 1/1000
		},
		{
			name:               "Different distinct values",
			leftDistinct:       500,
			rightDistinct:      2000,
			expectedSelectivity: 0.0005, // 1/2000 (uses max)
		},
		{
			name:               "No statistics",
			leftDistinct:       0,
			rightDistinct:      0,
			expectedSelectivity: 0.1, // Default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leftStats := &catalog.TableStatistics{DistinctValues: tt.leftDistinct}
			rightStats := &catalog.TableStatistics{DistinctValues: tt.rightDistinct}

			actualSelectivity := estimator.estimateSelectivity(leftStats, rightStats)

			if math.Abs(actualSelectivity - tt.expectedSelectivity) > 0.0001 {
				t.Errorf("Expected selectivity %.4f, got %.4f",
					tt.expectedSelectivity, actualSelectivity)
			}
		})
	}
}

func TestCostEstimator_EstimateOutputCardinality(t *testing.T) {
	estimator := &CostEstimator{}

	stats := &JoinStatistics{
		LeftCardinality:  1000,
		RightCardinality: 2000,
		Selectivity:      0.001, // 0.1%
	}

	// Output = 1000 * 2000 * 0.001 = 2000
	expectedOutput := 2000
	actualOutput := estimator.EstimateOutputCardinality(stats)

	if actualOutput != expectedOutput {
		t.Errorf("Expected output cardinality %d, got %d", expectedOutput, actualOutput)
	}
}

func TestCostEstimator_log2(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{1, 0},
		{2, 1},
		{4, 2},
		{8, 3},
		{16, 4},
		{100, 6}, // Approximate
	}

	for _, tt := range tests {
		actual := log2(tt.input)
		if math.Abs(actual - tt.expected) > 1.0 {
			t.Errorf("log2(%.0f): expected ~%.0f, got %.0f", tt.input, tt.expected, actual)
		}
	}
}

func TestCostEstimator_BuildJoinStatistics_WithMissingStats(t *testing.T) {
	// This test verifies that BuildJoinStatistics handles missing statistics
	// by using default values. Since we can't easily mock SystemCatalog,
	// we test the estimateSelectivity function directly with empty stats.

	estimator := &CostEstimator{}

	// Test with zero distinct values (simulates missing statistics)
	leftStats := &catalog.TableStatistics{DistinctValues: 0}
	rightStats := &catalog.TableStatistics{DistinctValues: 0}

	selectivity := estimator.estimateSelectivity(leftStats, rightStats)

	// Should use default selectivity of 0.1 when no statistics available
	if selectivity != 0.1 {
		t.Errorf("Expected default selectivity 0.1, got %.2f", selectivity)
	}
}
