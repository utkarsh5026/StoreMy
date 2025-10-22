package statistics

import (
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"testing"
)

// Helper function to create integer fields
func intField(v int64) types.Field {
	return &types.IntField{Value: v}
}

// Helper function to create float fields
func floatField(v float64) types.Field {
	return &types.Float64Field{Value: v}
}

func TestNewHistogram_Empty(t *testing.T) {
	hist := NewHistogram([]types.Field{}, 10)

	if hist.TotalCount != 0 {
		t.Errorf("Expected TotalCount to be 0, got %d", hist.TotalCount)
	}

	if len(hist.Buckets) != 0 {
		t.Errorf("Expected 0 buckets, got %d", len(hist.Buckets))
	}
}

func TestNewHistogram_SingleValue(t *testing.T) {
	values := []types.Field{intField(42)}
	hist := NewHistogram(values, 10)

	if hist.TotalCount != 1 {
		t.Errorf("Expected TotalCount to be 1, got %d", hist.TotalCount)
	}

	if len(hist.Buckets) != 1 {
		t.Errorf("Expected 1 bucket, got %d", len(hist.Buckets))
	}

	bucket := hist.Buckets[0]
	if bucket.Count != 1 {
		t.Errorf("Expected bucket count to be 1, got %d", bucket.Count)
	}

	if bucket.DistinctCount != 1 {
		t.Errorf("Expected distinct count to be 1, got %d", bucket.DistinctCount)
	}
}

func TestNewHistogram_IntegerValues(t *testing.T) {
	// Create values from 1 to 100
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	if hist.TotalCount != 100 {
		t.Errorf("Expected TotalCount to be 100, got %d", hist.TotalCount)
	}

	if len(hist.Buckets) != 10 {
		t.Errorf("Expected 10 buckets, got %d", len(hist.Buckets))
	}

	// Check that each bucket has 10 values
	for i, bucket := range hist.Buckets {
		if bucket.Count != 10 {
			t.Errorf("Bucket %d: Expected count 10, got %d", i, bucket.Count)
		}

		if bucket.BucketFraction != 0.1 {
			t.Errorf("Bucket %d: Expected fraction 0.1, got %f", i, bucket.BucketFraction)
		}

		if bucket.DistinctCount != 10 {
			t.Errorf("Bucket %d: Expected 10 distinct values, got %d", i, bucket.DistinctCount)
		}
	}
}

func TestNewHistogram_DuplicateValues(t *testing.T) {
	// Create 100 values but only 5 distinct (20 copies each)
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i/20 + 1)) // Values: 1,1,1,...,2,2,2,...,5,5,5
	}

	hist := NewHistogram(values, 5)

	if hist.TotalCount != 100 {
		t.Errorf("Expected TotalCount to be 100, got %d", hist.TotalCount)
	}

	// Each bucket should ideally have one distinct value
	totalDistinct := int64(0)
	for _, bucket := range hist.Buckets {
		totalDistinct += bucket.DistinctCount
	}

	if totalDistinct != 5 {
		t.Errorf("Expected 5 total distinct values across buckets, got %d", totalDistinct)
	}
}

func TestNewHistogram_FewerValuesThanBuckets(t *testing.T) {
	values := []types.Field{intField(1), intField(2), intField(3)}
	hist := NewHistogram(values, 10)

	if hist.TotalCount != 3 {
		t.Errorf("Expected TotalCount to be 3, got %d", hist.TotalCount)
	}

	// Should create 3 buckets, not 10
	if len(hist.Buckets) != 3 {
		t.Errorf("Expected 3 buckets, got %d", len(hist.Buckets))
	}
}

func TestHistogramBucket_Contains(t *testing.T) {
	bucket := HistogramBucket{
		LowerBound:    intField(10),
		UpperBound:    intField(20),
		DistinctCount: 11,
		Count:         11,
	}

	tests := []struct {
		value    types.Field
		expected bool
	}{
		{intField(5), false},  // Below range
		{intField(10), true},  // Lower bound
		{intField(15), true},  // Middle
		{intField(20), true},  // Upper bound
		{intField(25), false}, // Above range
	}

	for _, tt := range tests {
		result := bucket.Contains(tt.value)
		if result != tt.expected {
			intVal := tt.value.(*types.IntField).Value
			t.Errorf("Contains(%d) = %v, expected %v", intVal, result, tt.expected)
		}
	}
}

func TestEstimateSelectivity_Equality(t *testing.T) {
	// Create histogram with values 1-100
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	// Test equality selectivity
	selectivity := hist.EstimateSelectivity(primitives.Equals, intField(50))

	// With 10 buckets and 100 values, each bucket has 10 distinct values
	// Selectivity should be (1/10) * 0.1 = 0.01
	expected := 0.01
	tolerance := 0.001

	if selectivity < expected-tolerance || selectivity > expected+tolerance {
		t.Errorf("EstimateSelectivity(=, 50) = %f, expected ~%f", selectivity, expected)
	}

	// Test value not in range
	selectivity = hist.EstimateSelectivity(primitives.Equals, intField(150))
	if selectivity != 0.0 {
		t.Errorf("EstimateSelectivity(=, 150) = %f, expected 0.0", selectivity)
	}
}

func TestEstimateSelectivity_GreaterThan(t *testing.T) {
	// Create histogram with values 1-100
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	tests := []struct {
		value       types.Field
		expectedMin float64
		expectedMax float64
		description string
	}{
		{intField(0), 0.99, 1.0, "all values > 0"},
		{intField(50), 0.45, 0.55, "half values > 50"},
		{intField(100), 0.0, 0.01, "no values > 100"},
	}

	for _, tt := range tests {
		selectivity := hist.EstimateSelectivity(primitives.GreaterThan, tt.value)
		if selectivity < tt.expectedMin || selectivity > tt.expectedMax {
			t.Errorf("%s: EstimateSelectivity(>, %v) = %f, expected between %f and %f",
				tt.description, tt.value, selectivity, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestEstimateSelectivity_GreaterThanOrEqual(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	// >= 50 should be slightly higher than > 50
	selectivityGT := hist.EstimateSelectivity(primitives.GreaterThan, intField(50))
	selectivityGTE := hist.EstimateSelectivity(primitives.GreaterThanOrEqual, intField(50))

	if selectivityGTE < selectivityGT {
		t.Errorf("Selectivity >= should be at least as high as >, got %f and %f",
			selectivityGTE, selectivityGT)
	}
}

func TestEstimateSelectivity_LessThan(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	tests := []struct {
		value       types.Field
		expectedMin float64
		expectedMax float64
		description string
	}{
		{intField(0), 0.0, 0.01, "no values < 0"},
		{intField(50), 0.45, 0.55, "half values < 50"},
		{intField(101), 0.99, 1.0, "all values < 101"},
	}

	for _, tt := range tests {
		selectivity := hist.EstimateSelectivity(primitives.LessThan, tt.value)
		if selectivity < tt.expectedMin || selectivity > tt.expectedMax {
			t.Errorf("%s: EstimateSelectivity(<, %v) = %f, expected between %f and %f",
				tt.description, tt.value, selectivity, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestEstimateSelectivity_LessThanOrEqual(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	// <= 50 should be slightly higher than < 50
	selectivityLT := hist.EstimateSelectivity(primitives.LessThan, intField(50))
	selectivityLTE := hist.EstimateSelectivity(primitives.LessThanOrEqual, intField(50))

	if selectivityLTE < selectivityLT {
		t.Errorf("Selectivity <= should be at least as high as <, got %f and %f",
			selectivityLTE, selectivityLT)
	}
}

func TestEstimateSelectivity_NotEqual(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	selectivityEQ := hist.EstimateSelectivity(primitives.Equals, intField(50))
	selectivityNE := hist.EstimateSelectivity(primitives.NotEqual, intField(50))

	// Selectivity(!=) should be 1 - Selectivity(=)
	expected := 1.0 - selectivityEQ
	tolerance := 0.001

	if selectivityNE < expected-tolerance || selectivityNE > expected+tolerance {
		t.Errorf("EstimateSelectivity(!=, 50) = %f, expected %f (1 - %f)",
			selectivityNE, expected, selectivityEQ)
	}
}

func TestEstimateSelectivity_FloatValues(t *testing.T) {
	// Create histogram with float values 0.0 to 10.0
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = floatField(float64(i) / 10.0)
	}

	hist := NewHistogram(values, 10)

	// Test equality
	selectivity := hist.EstimateSelectivity(primitives.Equals, floatField(5.0))
	if selectivity <= 0.0 || selectivity > 0.2 {
		t.Errorf("EstimateSelectivity(=, 5.0) = %f, expected small positive value", selectivity)
	}

	// Test greater than
	selectivity = hist.EstimateSelectivity(primitives.GreaterThan, floatField(5.0))
	if selectivity < 0.4 || selectivity > 0.6 {
		t.Errorf("EstimateSelectivity(>, 5.0) = %f, expected ~0.5", selectivity)
	}

	// Test less than
	selectivity = hist.EstimateSelectivity(primitives.LessThan, floatField(5.0))
	if selectivity < 0.4 || selectivity > 0.6 {
		t.Errorf("EstimateSelectivity(<, 5.0) = %f, expected ~0.5", selectivity)
	}
}

func TestEstimateSelectivity_EmptyHistogram(t *testing.T) {
	hist := NewHistogram([]types.Field{}, 10)

	selectivity := hist.EstimateSelectivity(primitives.Equals, intField(50))
	if selectivity != 0.0 {
		t.Errorf("Empty histogram should return 0.0 selectivity, got %f", selectivity)
	}

	selectivity = hist.EstimateSelectivity(primitives.GreaterThan, intField(50))
	if selectivity != 0.0 {
		t.Errorf("Empty histogram should return 0.0 selectivity, got %f", selectivity)
	}
}

func TestEstimateSelectivity_UniformDistribution(t *testing.T) {
	// Create uniform distribution with 10 copies of values 1-10
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i%10 + 1))
	}

	hist := NewHistogram(values, 10)

	// Test that selectivity estimates are reasonable
	selectivity := hist.EstimateSelectivity(primitives.GreaterThan, intField(5))
	if selectivity < 0.3 || selectivity > 0.7 {
		t.Errorf("EstimateSelectivity(>, 5) on uniform distribution = %f, expected ~0.5", selectivity)
	}
}

func TestEstimateSelectivity_SkewedDistribution(t *testing.T) {
	// Create skewed distribution: 90 copies of 1, then 1-10
	values := make([]types.Field, 100)
	for i := 0; i < 90; i++ {
		values[i] = intField(1)
	}
	for i := 90; i < 100; i++ {
		values[i] = intField(int64(i - 89))
	}

	hist := NewHistogram(values, 10)

	// Most values are 1, so > 1 should have low selectivity
	selectivity := hist.EstimateSelectivity(primitives.GreaterThan, intField(1))
	if selectivity < 0.0 || selectivity > 0.2 {
		t.Errorf("EstimateSelectivity(>, 1) on skewed distribution = %f, expected < 0.2", selectivity)
	}

	// = 1 should have reasonable selectivity (equi-depth histograms don't perfectly capture skew)
	// With 10 buckets and 90% of values being 1, we expect moderate selectivity
	selectivity = hist.EstimateSelectivity(primitives.Equals, intField(1))
	if selectivity < 0.05 || selectivity > 0.5 {
		t.Errorf("EstimateSelectivity(=, 1) on skewed distribution = %f, expected between 0.05 and 0.5", selectivity)
	}
}

func TestCountDistinct(t *testing.T) {
	tests := []struct {
		values   []types.Field
		expected int
	}{
		{[]types.Field{}, 0},
		{[]types.Field{intField(1)}, 1},
		{[]types.Field{intField(1), intField(1)}, 1},
		{[]types.Field{intField(1), intField(2)}, 2},
		{[]types.Field{intField(1), intField(1), intField(2), intField(2)}, 2},
		{[]types.Field{intField(1), intField(2), intField(3), intField(4), intField(5)}, 5},
	}

	for _, tt := range tests {
		result := countDistinct(tt.values)
		if result != tt.expected {
			t.Errorf("countDistinct(%v) = %d, expected %d", tt.values, result, tt.expected)
		}
	}
}

func TestEstimateFractionInBucket_IntField(t *testing.T) {
	hist := &Histogram{}
	bucket := HistogramBucket{
		LowerBound:    intField(0),
		UpperBound:    intField(100),
		DistinctCount: 101,
		Count:         101,
	}

	tests := []struct {
		value       *types.IntField
		inclusive   bool
		greaterThan bool
		expectedMin float64
		expectedMax float64
	}{
		{&types.IntField{Value: 50}, true, true, 0.45, 0.55},   // >= 50
		{&types.IntField{Value: 50}, false, true, 0.45, 0.55},  // > 50
		{&types.IntField{Value: 50}, true, false, 0.45, 0.55},  // <= 50
		{&types.IntField{Value: 50}, false, false, 0.45, 0.55}, // < 50
		{&types.IntField{Value: 0}, true, true, 0.95, 1.05},    // >= 0 (all values)
		{&types.IntField{Value: 100}, true, false, 0.95, 1.05}, // <= 100 (all values)
	}

	for _, tt := range tests {
		result := hist.estimateIntFraction(bucket, tt.value, tt.inclusive, tt.greaterThan)
		if result < tt.expectedMin || result > tt.expectedMax {
			t.Errorf("estimateIntFraction(%d, incl=%v, gt=%v) = %f, expected between %f and %f",
				tt.value.Value, tt.inclusive, tt.greaterThan, result, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestEstimateFractionInBucket_FloatField(t *testing.T) {
	hist := &Histogram{}
	bucket := HistogramBucket{
		LowerBound:    floatField(0.0),
		UpperBound:    floatField(100.0),
		DistinctCount: 101,
		Count:         101,
	}

	tests := []struct {
		value       *types.Float64Field
		inclusive   bool
		greaterThan bool
		expectedMin float64
		expectedMax float64
	}{
		{&types.Float64Field{Value: 50.0}, true, true, 0.45, 0.55},   // >= 50
		{&types.Float64Field{Value: 50.0}, false, true, 0.45, 0.55},  // > 50
		{&types.Float64Field{Value: 50.0}, true, false, 0.45, 0.55},  // <= 50
		{&types.Float64Field{Value: 50.0}, false, false, 0.45, 0.55}, // < 50
		{&types.Float64Field{Value: 25.0}, true, false, 0.2, 0.3},    // <= 25
		{&types.Float64Field{Value: 75.0}, true, true, 0.2, 0.3},     // >= 75
	}

	for _, tt := range tests {
		result := hist.estimateFloatFraction(bucket, tt.value, tt.inclusive, tt.greaterThan)
		if result < tt.expectedMin || result > tt.expectedMax {
			t.Errorf("estimateFloatFraction(%f, incl=%v, gt=%v) = %f, expected between %f and %f",
				tt.value.Value, tt.inclusive, tt.greaterThan, result, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestEstimateFractionInBucket_SingleValue(t *testing.T) {
	hist := &Histogram{}
	// Bucket with all same values
	bucket := HistogramBucket{
		LowerBound:    intField(42),
		UpperBound:    intField(42),
		DistinctCount: 1,
		Count:         100,
	}

	// For equality, should return 1.0
	result := hist.estimateIntFraction(bucket, &types.IntField{Value: 42}, true, true)
	if result != 1.0 {
		t.Errorf("estimateIntFraction for single value (inclusive, >=) = %f, expected 1.0", result)
	}

	// For > 42, should return 0.0
	result = hist.estimateIntFraction(bucket, &types.IntField{Value: 42}, false, true)
	if result != 0.0 {
		t.Errorf("estimateIntFraction for single value (exclusive, >) = %f, expected 0.0", result)
	}
}

func TestHistogram_BucketFractions(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	// Sum of all bucket fractions should be 1.0
	totalFraction := 0.0
	for _, bucket := range hist.Buckets {
		totalFraction += bucket.BucketFraction
	}

	if totalFraction < 0.99 || totalFraction > 1.01 {
		t.Errorf("Sum of bucket fractions = %f, expected ~1.0", totalFraction)
	}
}

func TestHistogram_BoundaryConditions(t *testing.T) {
	values := make([]types.Field, 100)
	for i := 0; i < 100; i++ {
		values[i] = intField(int64(i + 1))
	}

	hist := NewHistogram(values, 10)

	// Test selectivity at exact bucket boundaries
	// First bucket should have lower bound = 1, upper bound = 10
	firstBucket := hist.Buckets[0]

	lower := firstBucket.LowerBound.(*types.IntField).Value
	upper := firstBucket.UpperBound.(*types.IntField).Value

	// Value at lower boundary should be in bucket
	if !firstBucket.Contains(intField(lower)) {
		t.Errorf("Bucket should contain its lower bound value %d", lower)
	}

	// Value at upper boundary should be in bucket
	if !firstBucket.Contains(intField(upper)) {
		t.Errorf("Bucket should contain its upper bound value %d", upper)
	}
}
