package catalog

import (
	"fmt"
	"math"
	"sort"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// ColumnStatistics represents detailed statistics for a single column
type ColumnStatistics struct {
	TableID        int           // Table identifier
	ColumnName     string        // Column name
	ColumnIndex    int           // Column position in table (0-based)
	DistinctCount  int64         // Number of distinct values
	NullCount      int64         // Number of NULL values
	MinValue       types.Field   // Minimum value in column
	MaxValue       types.Field   // Maximum value in column
	AvgWidth       int           // Average width in bytes (for variable length types)
	Histogram      *Histogram    // Value distribution histogram
	MostCommonVals []types.Field // Most common values (MCVs)
	MCVFreqs       []float64     // Frequencies of MCVs (0.0-1.0)
	LastUpdated    time.Time     // Last statistics update time
}

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

		if ltUpper {
			// value < bucket.UpperBound: all values in bucket are >= value
			selectivity += bucket.BucketFraction
		} else if gtLower {
			// value > bucket.LowerBound: no values in bucket satisfy predicate
			continue
		} else {
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

		if ltLower {
			// value < bucket.LowerBound: no values in bucket satisfy predicate
			continue
		} else if gtUpper {
			// value > bucket.UpperBound: all values in bucket are <= value
			selectivity += bucket.BucketFraction
		} else {
			// value is within bucket: estimate fraction
			fraction := h.estimateFractionInBucket(bucket, value, inclusive, false)
			selectivity += bucket.BucketFraction * fraction
		}
	}

	return math.Min(selectivity, 1.0)
}

// estimateFractionInBucket estimates what fraction of a bucket satisfies the predicate
// greaterThan: true for > and >=, false for < and <=
func (h *Histogram) estimateFractionInBucket(bucket HistogramBucket, value types.Field, inclusive bool, greaterThan bool) float64 {
	// For numeric types, interpolate based on value range
	// For other types, assume uniform distribution (0.5)

	// Simplified: assume uniform distribution within bucket
	// In a real implementation, you'd handle different field types differently
	if greaterThan {
		// Estimate fraction greater than value
		return 0.5
	}
	// Estimate fraction less than value
	return 0.5
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

// findMostCommonValues finds the N most common values and their frequencies
func findMostCommonValues(values []types.Field, n int) ([]types.Field, []float64) {
	if len(values) == 0 {
		return nil, nil
	}

	// Count frequencies
	freqMap := make(map[string]int)
	valueMap := make(map[string]types.Field)

	for _, val := range values {
		key := val.String()
		freqMap[key]++
		valueMap[key] = val
	}

	// Sort by frequency
	type valueFreq struct {
		value types.Field
		freq  int
	}

	freqs := make([]valueFreq, 0, len(freqMap))
	for key, freq := range freqMap {
		freqs = append(freqs, valueFreq{valueMap[key], freq})
	}

	sort.Slice(freqs, func(i, j int) bool {
		return freqs[i].freq > freqs[j].freq
	})

	// Take top N
	if len(freqs) > n {
		freqs = freqs[:n]
	}

	totalCount := float64(len(values))
	mcvs := make([]types.Field, len(freqs))
	mcvFreqs := make([]float64, len(freqs))

	for i, vf := range freqs {
		mcvs[i] = vf.value
		mcvFreqs[i] = float64(vf.freq) / totalCount
	}

	return mcvs, mcvFreqs
}

// CollectColumnStatistics collects statistics for a specific column in a table
func (sc *SystemCatalog) CollectColumnStatistics(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
	columnIndex int,
	histogramBuckets int,
	mcvCount int,
) (*ColumnStatistics, error) {
	_, err := sc.cache.GetDbFile(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get db file: %w", err)
	}

	// Collect all values from the column
	var values []types.Field
	nullCount := int64(0)
	totalWidth := 0

	err = sc.iterateTable(tableID, tx, func(t *tuple.Tuple) error {
		if columnIndex >= t.TupleDesc.NumFields() {
			return fmt.Errorf("column index %d out of range (tuple has %d fields)", columnIndex, t.TupleDesc.NumFields())
		}

		field, err := t.GetField(columnIndex)
		if err != nil {
			return err
		}

		if field == nil {
			nullCount++
		} else {
			values = append(values, field)
			// Track average width for variable-length types
			if strField, ok := field.(*types.StringField); ok {
				totalWidth += len(strField.Value)
			} else {
				// Fixed-width types
				totalWidth += int(field.Type().Size())
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate table: %w", err)
	}

	// Calculate statistics
	stats := &ColumnStatistics{
		TableID:     tableID,
		ColumnName:  columnName,
		ColumnIndex: columnIndex,
		NullCount:   nullCount,
		LastUpdated: time.Now(),
	}

	if len(values) == 0 {
		// All NULLs or empty table
		stats.DistinctCount = 0
		stats.AvgWidth = 0
		return stats, nil
	}

	// Sort for histogram and distinct count
	sort.Slice(values, func(i, j int) bool {
		result, _ := values[i].Compare(primitives.LessThan, values[j])
		return result
	})

	// Calculate distinct count
	stats.DistinctCount = int64(countDistinct(values))

	// Min and max values
	stats.MinValue = values[0]
	stats.MaxValue = values[len(values)-1]

	// Average width
	stats.AvgWidth = totalWidth / len(values)

	// Build histogram
	stats.Histogram = NewHistogram(values, histogramBuckets)

	// Find most common values
	stats.MostCommonVals, stats.MCVFreqs = findMostCommonValues(values, mcvCount)

	return stats, nil
}

// UpdateColumnStatistics updates statistics for all columns in a table
func (sc *SystemCatalog) UpdateColumnStatistics(tx *transaction.TransactionContext, tableID int) error {
	// Get table metadata to know what columns exist
	// For now, we'll use a simplified approach
	// In a real implementation, you'd query CATALOG_COLUMNS

	// This is a placeholder - in practice, you'd iterate through columns from the catalog
	// For now, just return nil to allow compilation
	// TODO: Implement full column iteration from CATALOG_COLUMNS system table
	return nil
}

/*
// Example of what the full implementation would look like when CATALOG_COLUMNS is integrated:
func (sc *SystemCatalog) UpdateColumnStatisticsComplete(tx *transaction.TransactionContext, tableID int) error {
	// Collect statistics for each column
	for i := 0; i < numColumns; i++ {
		columnName := getColumnName(i)

		stats, err := sc.CollectColumnStatistics(tx, tableID, columnName, i, 10, 5)
		if err != nil {
			return fmt.Errorf("failed to collect statistics for column %s: %w", columnName, err)
		}

		// Store the statistics
		if err := sc.storeColumnStatistics(tx, stats); err != nil {
			return fmt.Errorf("failed to store statistics for column %s: %w", columnName, err)
		}
	}

	return nil
}
*/

// storeColumnStatistics stores column statistics (placeholder - will be implemented with system table)
func (sc *SystemCatalog) storeColumnStatistics(tx *transaction.TransactionContext, stats *ColumnStatistics) error {
	// TODO: Store in CATALOG_COLUMN_STATISTICS system table
	// For now, just cache in memory
	return nil
}

// GetColumnStatistics retrieves statistics for a specific column
func (sc *SystemCatalog) GetColumnStatistics(
	tx *transaction.TransactionContext,
	tableID int,
	columnName string,
) (*ColumnStatistics, error) {
	// TODO: Retrieve from CATALOG_COLUMN_STATISTICS system table
	// For now, return nil (will trigger collection)
	return nil, fmt.Errorf("column statistics not found")
}
