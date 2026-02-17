package statistics

import (
	"math"
	"sort"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
)

// Histogram represents the distribution of values in a column
// Uses equi-depth (equi-height) histogram for better selectivity estimation
type Histogram struct {
	Buckets    []HistogramBucket // Histogram buckets
	TotalCount int64             // Total number of values (excluding NULLs)
}

// HistogramBucket represents a single bucket in the histogram
type HistogramBucket struct {
	LowerBound     types.Field // Lower bound (inclusive)
	UpperBound     types.Field // Upper bound (inclusive)
	DistinctCount  int64       // Number of distinct values in bucket
	Count          int64       // Total number of values in bucket
	BucketFraction float64     // Fraction of total values in this bucket
}

// NewHistogram creates a new histogram from a sorted list of values
// bucketCount: desired number of buckets (typically 10-100)
func NewHistogram(values []types.Field, bucketCount int) *Histogram {
	if len(values) == 0 {
		return &Histogram{
			Buckets:    []HistogramBucket{},
			TotalCount: 0,
		}
	}

	// Sort values for equi-depth histogram
	sort.Slice(values, func(i, j int) bool {
		result, _ := values[i].Compare(primitives.LessThan, values[j])
		return result
	})

	totalCount := int64(len(values))

	// Determine actual bucket count (may be less than requested if few values)
	actualBucketCount := bucketCount
	if int64(actualBucketCount) > totalCount {
		actualBucketCount = int(totalCount)
	}

	buckets := make([]HistogramBucket, 0, actualBucketCount)
	valuesPerBucket := totalCount / int64(actualBucketCount)

	for i := 0; i < actualBucketCount; i++ {
		startIdx := i * int(valuesPerBucket)
		endIdx := startIdx + int(valuesPerBucket)

		// Last bucket gets any remaining values
		if i == actualBucketCount-1 {
			endIdx = len(values)
		}

		if startIdx >= len(values) {
			break
		}

		bucketValues := values[startIdx:endIdx]
		distinct := countDistinct(bucketValues)

		bucket := HistogramBucket{
			LowerBound:     bucketValues[0],
			UpperBound:     bucketValues[len(bucketValues)-1],
			DistinctCount:  int64(distinct),
			Count:          int64(len(bucketValues)),
			BucketFraction: float64(len(bucketValues)) / float64(totalCount),
		}
		buckets = append(buckets, bucket)
	}

	return &Histogram{
		Buckets:    buckets,
		TotalCount: totalCount,
	}
}

// EstimateSelectivity estimates the selectivity of a predicate on this histogram
// Returns a value between 0.0 and 1.0 representing the fraction of rows that match
func (h *Histogram) EstimateSelectivity(pred primitives.Predicate, value types.Field) float64 {
	if h.TotalCount == 0 || len(h.Buckets) == 0 {
		return 0.0
	}

	switch pred {
	case primitives.Equals:
		return h.estimateEqualitySelectivity(value)
	case primitives.GreaterThan:
		return h.estimateGreaterThanSelectivity(value, false)
	case primitives.GreaterThanOrEqual:
		return h.estimateGreaterThanSelectivity(value, true)
	case primitives.LessThan:
		return h.estimateLessThanSelectivity(value, false)
	case primitives.LessThanOrEqual:
		return h.estimateLessThanSelectivity(value, true)
	case primitives.NotEqual, primitives.NotEqualsBracket:
		return 1.0 - h.estimateEqualitySelectivity(value)
	default:
		return 0.1
	}
}

// estimateEqualitySelectivity estimates selectivity for equality predicates
func (h *Histogram) estimateEqualitySelectivity(value types.Field) float64 {
	// Find the bucket containing this value
	for _, bucket := range h.Buckets {
		if bucket.Contains(value) {
			// Assume uniform distribution within bucket
			if bucket.DistinctCount == 0 {
				return 0.0
			}
			// Selectivity = (1 / distinct_in_bucket) * (bucket_count / total_count)
			return (1.0 / float64(bucket.DistinctCount)) * bucket.BucketFraction
		}
	}

	// Value not in any bucket
	return 0.0
}

// estimateGreaterThanSelectivity estimates selectivity for > and >= predicates
func (h *Histogram) estimateGreaterThanSelectivity(value types.Field, inclusive bool) float64 {
	selectivity := 0.0

	for _, bucket := range h.Buckets {
		ltUpper, _ := value.Compare(primitives.LessThan, bucket.UpperBound)
		gtLower, _ := value.Compare(primitives.GreaterThan, bucket.LowerBound)

		switch {
		case ltUpper:
			// value < bucket.UpperBound: all values in bucket are >= value
			selectivity += bucket.BucketFraction
		case gtLower:
			// value > bucket.LowerBound: no values in bucket satisfy predicate
			continue
		default:
			// value is within bucket: estimate fraction
			// Assume uniform distribution within bucket
			fraction := h.estimateFractionInBucket(bucket, value, inclusive, true)
			selectivity += bucket.BucketFraction * fraction
		}
	}

	return math.Min(selectivity, 1.0)
}

// estimateLessThanSelectivity estimates selectivity for < and <= predicates
func (h *Histogram) estimateLessThanSelectivity(value types.Field, inclusive bool) float64 {
	selectivity := 0.0

	for _, bucket := range h.Buckets {
		ltLower, _ := value.Compare(primitives.LessThan, bucket.LowerBound)
		gtUpper, _ := value.Compare(primitives.GreaterThan, bucket.UpperBound)

		switch {
		case ltLower:
			// value < bucket.LowerBound: no values in bucket satisfy predicate
			continue
		case gtUpper:
			// value > bucket.UpperBound: all values in bucket are <= value
			selectivity += bucket.BucketFraction
		default:
			// value is within bucket: estimate fraction
			fraction := h.estimateFractionInBucket(bucket, value, inclusive, false)
			selectivity += bucket.BucketFraction * fraction
		}
	}

	return math.Min(selectivity, 1.0)
}

// estimateFractionInBucket estimates what fraction of a bucket satisfies the predicate
// greaterThan: true for > and >=, false for < and <=
func (h *Histogram) estimateFractionInBucket(bucket HistogramBucket, value types.Field, inclusive, greaterThan bool) float64 {
	switch v := value.(type) {
	case *types.IntField:
		return h.estimateIntFraction(bucket, v, inclusive, greaterThan)
	case *types.Float64Field:
		return h.estimateFloatFraction(bucket, v, inclusive, greaterThan)
	default:
		return 0.5
	}
}

// estimateIntFraction estimates fraction for integer fields using linear interpolation
func (h *Histogram) estimateIntFraction(bucket HistogramBucket, value *types.IntField, inclusive, greaterThan bool) float64 {
	lower, okLower := bucket.LowerBound.(*types.IntField)
	upper, okUpper := bucket.UpperBound.(*types.IntField)

	if !okLower || !okUpper {
		return 0.5 // Type mismatch, use default
	}

	bucketRange := float64(upper.Value - lower.Value)
	if bucketRange == 0 {
		// All values in bucket are the same
		if inclusive && value.Value == lower.Value {
			return 1.0
		}
		return 0.0
	}

	// Position of value within bucket (0.0 to 1.0)
	position := float64(value.Value-lower.Value) / bucketRange

	if greaterThan {
		// Fraction of values > or >= value
		if inclusive {
			return 1.0 - position
		}
		return 1.0 - position
	} else {
		// Fraction of values < or <= value
		if inclusive {
			return position
		}
		return position
	}
}

// estimateFloatFraction estimates fraction for float fields using linear interpolation
func (h *Histogram) estimateFloatFraction(bucket HistogramBucket, value *types.Float64Field, inclusive, greaterThan bool) float64 {
	lower, okLower := bucket.LowerBound.(*types.Float64Field)
	upper, okUpper := bucket.UpperBound.(*types.Float64Field)

	if !okLower || !okUpper {
		return 0.5 // Type mismatch, use default
	}

	bucketRange := upper.Value - lower.Value
	if bucketRange == 0 || math.IsInf(bucketRange, 0) || math.IsNaN(bucketRange) {
		// All values in bucket are the same or invalid range
		if inclusive && value.Value == lower.Value {
			return 1.0
		}
		return 0.0
	}

	// Position of value within bucket (0.0 to 1.0)
	position := (value.Value - lower.Value) / bucketRange
	position = math.Max(0.0, math.Min(1.0, position)) // Clamp to [0,1]

	if greaterThan {
		// Fraction of values > or >= value
		return 1.0 - position
	} else {
		// Fraction of values < or <= value
		return position
	}
}

// Contains checks if a value falls within the bucket's range
func (b *HistogramBucket) Contains(value types.Field) bool {
	geqLower, _ := value.Compare(primitives.GreaterThanOrEqual, b.LowerBound)
	leqUpper, _ := value.Compare(primitives.LessThanOrEqual, b.UpperBound)
	return geqLower && leqUpper
}

// countDistinct counts the number of distinct values in a sorted slice
func countDistinct(values []types.Field) int {
	if len(values) == 0 {
		return 0
	}

	distinct := 1
	for i := 1; i < len(values); i++ {
		notEqual, _ := values[i].Compare(primitives.NotEqual, values[i-1])
		if notEqual {
			distinct++
		}
	}
	return distinct
}
