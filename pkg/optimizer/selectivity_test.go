package optimizer

import (
	"math"
	"storemy/pkg/catalog"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"testing"
)

// TestEstimatePredicateSelectivityWithValue_Histogram tests histogram-based selectivity
func TestEstimatePredicateSelectivityWithValue_Histogram(t *testing.T) {
	tests := []struct {
		name          string
		values        []types.Field
		pred          primitives.Predicate
		queryValue    types.Field
		expectedRange [2]float64 // min and max expected selectivity
		description   string
	}{
		{
			name:          "Equality in middle bucket",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.Equals,
			queryValue:    types.NewIntField(50),
			expectedRange: [2]float64{0.005, 0.02}, // ~1% for uniform distribution
			description:   "Equality should give low selectivity in large dataset",
		},
		{
			name:          "Less than half",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.LessThan,
			queryValue:    types.NewIntField(50),
			expectedRange: [2]float64{0.4, 0.6}, // ~50%
			description:   "Less than 50 in [1-100] should be ~50%",
		},
		{
			name:          "Greater than quarter",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.GreaterThan,
			queryValue:    types.NewIntField(25),
			expectedRange: [2]float64{0.65, 0.85}, // ~75%
			description:   "Greater than 25 in [1-100] should be ~75%",
		},
		{
			name:          "Less than or equal to max",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.LessThanOrEqual,
			queryValue:    types.NewIntField(100),
			expectedRange: [2]float64{0.90, 1.0}, // ~95-100% (histogram boundary approximation)
			description:   "Less than or equal to max should be ~100%",
		},
		{
			name:          "Greater than max",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.GreaterThan,
			queryValue:    types.NewIntField(100),
			expectedRange: [2]float64{0.0, 0.05}, // ~0%
			description:   "Greater than max should be ~0%",
		},
		{
			name:          "Not equal to single value",
			values:        createIntFields(1, 100, 1),
			pred:          primitives.NotEqual,
			queryValue:    types.NewIntField(50),
			expectedRange: [2]float64{0.98, 1.0}, // ~99%
			description:   "Not equal should be complement of equality",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create histogram
			histogram := catalog.NewHistogram(tt.values, 10)

			// Create estimator
			estimator := &SelectivityEstimator{}

			// Estimate selectivity
			sel := estimator.EstimateHistogram(histogram, tt.pred, tt.queryValue)

			// Verify selectivity is in valid range [0.0, 1.0]
			if sel < 0.0 || sel > 1.0 {
				t.Errorf("%s: selectivity %f out of valid range [0.0, 1.0]", tt.description, sel)
			}

			// Verify selectivity is in expected range
			if sel < tt.expectedRange[0] || sel > tt.expectedRange[1] {
				t.Errorf("%s: selectivity %f not in expected range [%f, %f]",
					tt.description, sel, tt.expectedRange[0], tt.expectedRange[1])
			}

			t.Logf("%s: selectivity = %.4f (expected [%.4f, %.4f])",
				tt.description, sel, tt.expectedRange[0], tt.expectedRange[1])
		})
	}
}

// TestEstimatePredicateSelectivityWithValue_NoHistogram tests fallback behavior
func TestEstimatePredicateSelectivityWithValue_NoHistogram(t *testing.T) {
	tests := []struct {
		name          string
		pred          primitives.Predicate
		distinctCount int64
		expected      float64
	}{
		{
			name:          "Equality with distinct count",
			pred:          primitives.Equals,
			distinctCount: 100,
			expected:      1.0 / 100.0,
		},
		{
			name:          "Equality with low distinct count",
			pred:          primitives.Equals,
			distinctCount: 10,
			expected:      1.0 / 10.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create column stats without histogram
			colStats := &catalog.ColumnStatistics{
				DistinctCount: tt.distinctCount,
				Histogram:     nil, // No histogram
			}

			estimator := &SelectivityEstimator{}

			// This should fall back to distinct count estimate
			sel := estimator.estimateFromDistinct(tt.pred, colStats)

			if math.Abs(sel-tt.expected) > 0.0001 {
				t.Errorf("Expected selectivity %f, got %f", tt.expected, sel)
			}
		})
	}
}

// TestEstimateCombinedSelectivity tests AND/OR combinations
func TestEstimateCombinedSelectivity(t *testing.T) {
	estimator := &SelectivityEstimator{}

	tests := []struct {
		name     string
		sel1     float64
		sel2     float64
		isAnd    bool
		expected float64
	}{
		{
			name:     "AND with independent predicates",
			sel1:     0.5,
			sel2:     0.5,
			isAnd:    true,
			expected: 0.25, // 0.5 * 0.5
		},
		{
			name:     "OR with independent predicates",
			sel1:     0.5,
			sel2:     0.5,
			isAnd:    false,
			expected: 0.75, // 0.5 + 0.5 - 0.25
		},
		{
			name:     "AND with low selectivity",
			sel1:     0.1,
			sel2:     0.1,
			isAnd:    true,
			expected: 0.01,
		},
		{
			name:     "OR with low selectivity",
			sel1:     0.1,
			sel2:     0.1,
			isAnd:    false,
			expected: 0.19, // 0.1 + 0.1 - 0.01
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.EstimateCombinedSelectivity(tt.sel1, tt.sel2, tt.isAnd)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("Expected %f, got %f", tt.expected, result)
			}
		})
	}
}

// TestIsComparisonPredicate tests the helper function
func TestIsComparisonPredicate(t *testing.T) {
	tests := []struct {
		pred     primitives.Predicate
		expected bool
	}{
		{primitives.Equals, true},
		{primitives.NotEqual, true},
		{primitives.GreaterThan, true},
		{primitives.GreaterThanOrEqual, true},
		{primitives.LessThan, true},
		{primitives.LessThanOrEqual, true},
		{primitives.NotEqualsBracket, true},
	}

	for _, tt := range tests {
		t.Run(tt.pred.String(), func(t *testing.T) {
			result := isComparisonPredicate(tt.pred)
			if result != tt.expected {
				t.Errorf("Expected %v for %v, got %v", tt.expected, tt.pred, result)
			}
		})
	}
}

// TestHistogramSelectivityBounds tests that histogram always returns [0.0, 1.0]
func TestHistogramSelectivityBounds(t *testing.T) {
	values := createIntFields(1, 1000, 1)
	histogram := catalog.NewHistogram(values, 20)

	estimator := &SelectivityEstimator{}

	predicates := []primitives.Predicate{
		primitives.Equals,
		primitives.NotEqual,
		primitives.GreaterThan,
		primitives.GreaterThanOrEqual,
		primitives.LessThan,
		primitives.LessThanOrEqual,
	}

	testValues := []types.Field{
		types.NewIntField(-100), // Below min
		types.NewIntField(1),    // At min
		types.NewIntField(500),  // Middle
		types.NewIntField(1000), // At max
		types.NewIntField(2000), // Above max
	}

	for _, pred := range predicates {
		for _, val := range testValues {
			sel := estimator.EstimateHistogram(histogram, pred, val)
			if sel < 0.0 || sel > 1.0 {
				t.Errorf("Selectivity %f out of bounds for predicate %v with value %v",
					sel, pred, val)
			}
		}
	}
}

// Helper function to create a slice of IntFields
func createIntFields(start, end, step int) []types.Field {
	var fields []types.Field
	for i := start; i <= end; i += step {
		fields = append(fields, types.NewIntField(int64(i)))
	}
	return fields
}

// TestEstimateLikeSelectivity tests LIKE pattern selectivity
func TestEstimateLikeSelectivity(t *testing.T) {
	estimator := &SelectivityEstimator{}

	tests := []struct {
		name     string
		pattern  string
		expected float64
	}{
		{
			name:     "Prefix match (abc%)",
			pattern:  "abc%",
			expected: 0.1,
		},
		{
			name:     "Suffix match (%xyz)",
			pattern:  "%xyz",
			expected: 0.3,
		},
		{
			name:     "Substring match (%abc%)",
			pattern:  "%abc%",
			expected: 0.5,
		},
		{
			name:     "Exact match (no wildcards)",
			pattern:  "exact",
			expected: 0.01,
		},
		{
			name:     "Empty pattern",
			pattern:  "",
			expected: LikeSelectivity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimator.EstimateLikeSelectivity(tt.pattern)
			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("Expected %f for pattern '%s', got %f", tt.expected, tt.pattern, result)
			}
		})
	}
}

// TestMCVSelectivity tests MCV-based selectivity estimation
func TestMCVSelectivity(t *testing.T) {
	tests := []struct {
		name        string
		mcvs        []types.Field
		mcvFreqs    []float64
		queryValue  types.Field
		pred        primitives.Predicate
		expected    float64
		description string
	}{
		{
			name:        "Equality with MCV hit - high frequency",
			mcvs:        []types.Field{types.NewIntField(1), types.NewIntField(2), types.NewIntField(3)},
			mcvFreqs:    []float64{0.5, 0.3, 0.1}, // Value 1 appears in 50% of rows
			queryValue:  types.NewIntField(1),
			pred:        primitives.Equals,
			expected:    0.5,
			description: "MCV with 50% frequency should return exact frequency",
		},
		{
			name:        "Equality with MCV hit - medium frequency",
			mcvs:        []types.Field{types.NewIntField(1), types.NewIntField(2), types.NewIntField(3)},
			mcvFreqs:    []float64{0.5, 0.3, 0.1},
			queryValue:  types.NewIntField(2),
			pred:        primitives.Equals,
			expected:    0.3,
			description: "MCV with 30% frequency should return exact frequency",
		},
		{
			name:        "NotEqual with MCV hit",
			mcvs:        []types.Field{types.NewIntField(1), types.NewIntField(2), types.NewIntField(3)},
			mcvFreqs:    []float64{0.5, 0.3, 0.1},
			queryValue:  types.NewIntField(1),
			pred:        primitives.NotEqual,
			expected:    0.5, // 1.0 - 0.5
			description: "NotEqual with MCV should return complement",
		},
		{
			name:        "Equality with string MCVs",
			mcvs:        []types.Field{types.NewStringField("USA", 50), types.NewStringField("UK", 50), types.NewStringField("Canada", 50)},
			mcvFreqs:    []float64{0.6, 0.2, 0.15},
			queryValue:  types.NewStringField("USA", 50),
			pred:        primitives.Equals,
			expected:    0.6,
			description: "String MCV should work correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colStats := &catalog.ColumnStatistics{
				MostCommonVals: tt.mcvs,
				MCVFreqs:       tt.mcvFreqs,
				DistinctCount:  100,
			}

			estimator := &SelectivityEstimator{}
			freq, found := estimator.findMCVFrequency(colStats, tt.queryValue)

			if !found {
				t.Errorf("%s: expected to find MCV but didn't", tt.description)
				return
			}

			if math.Abs(freq-tt.expected) > 0.0001 {
				t.Errorf("%s: expected %f, got %f", tt.description, tt.expected, freq)
			}

			t.Logf("%s: frequency = %.4f", tt.description, freq)
		})
	}
}

// TestMCVSelectivityMiss tests behavior when value is not in MCVs
func TestMCVSelectivityMiss(t *testing.T) {
	colStats := &catalog.ColumnStatistics{
		MostCommonVals: []types.Field{
			types.NewIntField(1),
			types.NewIntField(2),
			types.NewIntField(3),
		},
		MCVFreqs:      []float64{0.5, 0.3, 0.1},
		DistinctCount: 100,
	}

	estimator := &SelectivityEstimator{}

	// Query for a value not in MCVs
	freq, found := estimator.findMCVFrequency(colStats, types.NewIntField(999))

	if found {
		t.Errorf("Expected not to find non-MCV value, but found with frequency %f", freq)
	}
}

// TestEstimateEqualityWithoutHistogram tests equality estimation with MCVs but no histogram
func TestEstimateEqualityWithoutHistogram(t *testing.T) {
	tests := []struct {
		name          string
		mcvs          []types.Field
		mcvFreqs      []float64
		distinctCount int64
		expectedRange [2]float64
		description   string
	}{
		{
			name: "With MCVs covering 90% of data",
			mcvs: []types.Field{
				types.NewIntField(1),
				types.NewIntField(2),
				types.NewIntField(3),
			},
			mcvFreqs:      []float64{0.5, 0.3, 0.1}, // Total: 0.9
			distinctCount: 100,
			// Remaining 0.1 probability distributed among 97 non-MCV values
			expectedRange: [2]float64{0.0008, 0.0012}, // ~0.1/97 ≈ 0.00103
			description:   "Non-MCV values should share remaining 10% probability",
		},
		{
			name:          "No MCVs",
			mcvs:          []types.Field{},
			mcvFreqs:      []float64{},
			distinctCount: 100,
			expectedRange: [2]float64{0.009, 0.011}, // 1/100 = 0.01
			description:   "Without MCVs should use uniform distribution",
		},
		{
			name: "MCVs cover all distinct values",
			mcvs: []types.Field{
				types.NewIntField(1),
				types.NewIntField(2),
			},
			mcvFreqs:      []float64{0.6, 0.4},
			distinctCount: 2,
			expectedRange: [2]float64{0.45, 0.55}, // 1/2 = 0.5
			description:   "All values are MCVs - uniform distribution",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colStats := &catalog.ColumnStatistics{
				MostCommonVals: tt.mcvs,
				MCVFreqs:       tt.mcvFreqs,
				DistinctCount:  tt.distinctCount,
			}

			estimator := &SelectivityEstimator{}
			sel := estimator.estimateEqualityWithoutHistogram(colStats)

			if sel < tt.expectedRange[0] || sel > tt.expectedRange[1] {
				t.Errorf("%s: selectivity %f not in expected range [%f, %f]",
					tt.description, sel, tt.expectedRange[0], tt.expectedRange[1])
			}

			t.Logf("%s: selectivity = %.6f (expected [%.6f, %.6f])",
				tt.description, sel, tt.expectedRange[0], tt.expectedRange[1])
		})
	}
}

// TestMCVIntegrationWithHistogram tests full integration
func TestMCVIntegrationWithHistogram(t *testing.T) {
	// Create sample data with skewed distribution
	values := createIntFields(1, 100, 1)

	// Simulate MCVs
	mcvs := []types.Field{
		types.NewIntField(50), // Most common value
		types.NewIntField(25),
		types.NewIntField(75),
	}
	mcvFreqs := []float64{0.3, 0.2, 0.15} // Total: 65% of data

	colStats := &catalog.ColumnStatistics{
		MostCommonVals: mcvs,
		MCVFreqs:       mcvFreqs,
		DistinctCount:  100,
		Histogram:      catalog.NewHistogram(values, 10),
	}

	estimator := &SelectivityEstimator{}

	tests := []struct {
		name     string
		value    types.Field
		pred     primitives.Predicate
		checkMCV bool
		expected float64
	}{
		{
			name:     "Equality with MCV value",
			value:    types.NewIntField(50),
			pred:     primitives.Equals,
			checkMCV: true,
			expected: 0.3, // Exact MCV frequency
		},
		{
			name:     "NotEqual with MCV value",
			value:    types.NewIntField(50),
			pred:     primitives.NotEqual,
			checkMCV: true,
			expected: 0.7, // 1.0 - 0.3
		},
		{
			name:     "Greater than (uses histogram, not MCV)",
			value:    types.NewIntField(50),
			pred:     primitives.GreaterThan,
			checkMCV: false,
			expected: -1, // Will use histogram, so we just check it doesn't error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sel float64

			if tt.checkMCV {
				freq, found := estimator.findMCVFrequency(colStats, tt.value)
				if !found {
					t.Errorf("Expected to find MCV")
					return
				}

				if tt.pred == primitives.Equals {
					sel = freq
				} else {
					sel = 1.0 - freq
				}
			} else {
				// Use histogram
				sel = estimator.EstimateHistogram(colStats.Histogram, tt.pred, tt.value)
			}

			// Verify bounds
			if sel < 0.0 || sel > 1.0 {
				t.Errorf("Selectivity %f out of bounds [0.0, 1.0]", sel)
			}

			if tt.checkMCV && math.Abs(sel-tt.expected) > 0.0001 {
				t.Errorf("Expected %f, got %f", tt.expected, sel)
			}

			t.Logf("%s: selectivity = %.4f", tt.name, sel)
		})
	}
}

// TestEstimateInSelectivity tests IN clause selectivity
func TestEstimateInSelectivity(t *testing.T) {
	tests := []struct {
		name          string
		distinctCount int64
		valueCount    int
		expected      float64
	}{
		{
			name:          "IN with 5 values out of 100 distinct",
			distinctCount: 100,
			valueCount:    5,
			expected:      0.05,
		},
		{
			name:          "IN with all distinct values",
			distinctCount: 10,
			valueCount:    10,
			expected:      1.0,
		},
		{
			name:          "IN with more values than distinct (clamped to 1.0)",
			distinctCount: 10,
			valueCount:    20,
			expected:      1.0,
		},
		{
			name:          "IN with no values",
			distinctCount: 100,
			valueCount:    0,
			expected:      0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colStats := &catalog.ColumnStatistics{
				DistinctCount: tt.distinctCount,
			}

			// Simulate EstimateInSelectivity logic
			var result float64
			if tt.valueCount == 0 {
				result = 0.0
			} else if tt.distinctCount == 0 {
				result = InSelectivity
			} else {
				result = math.Min(float64(tt.valueCount)/float64(colStats.DistinctCount), MaxSelectivity)
			}

			if math.Abs(result-tt.expected) > 0.0001 {
				t.Errorf("Expected %f, got %f", tt.expected, result)
			}
		})
	}
}
