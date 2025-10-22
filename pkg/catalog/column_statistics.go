package catalog

import (
	"storemy/pkg/catalog/operations"
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/types"
	"time"
)

// ColumnStatistics represents detailed statistics for a single column.
// This is the high-level API type returned to database clients.
// It wraps the system table data with runtime computed histogram and MCV data.
type ColumnStatistics struct {
	TableID        int                    // Table identifier
	ColumnName     string                 // Column name
	ColumnIndex    int                    // Column position in table (0-based)
	DistinctCount  int64                  // Number of distinct values
	NullCount      int64                  // Number of NULL values
	MinValue       types.Field            // Minimum value in column
	MaxValue       types.Field            // Maximum value in column
	AvgWidth       int                    // Average width in bytes (for variable length types)
	Histogram      *statistics.Histogram  // Value distribution histogram
	MostCommonVals []types.Field          // Most common values (MCVs)
	MCVFreqs       []float64              // Frequencies of MCVs (0.0-1.0)
	LastUpdated    time.Time              // Last statistics update time
}

// toColumnStatistics converts operations.ColStatsInfo to catalog.ColumnStatistics.
// It uses the embedded colStats field which is accessible through promotion.
func toColumnStatistics(info *operations.ColStatsInfo) *ColumnStatistics {
	if info == nil {
		return nil
	}

	// Access embedded colStats fields directly through field promotion
	return &ColumnStatistics{
		TableID:        info.TableID,
		ColumnName:     info.ColumnName,
		ColumnIndex:    info.ColumnIndex,
		DistinctCount:  info.DistinctCount,
		NullCount:      info.NullCount,
		MinValue:       stringToField(info.MinValue),
		MaxValue:       stringToField(info.MaxValue),
		AvgWidth:       info.AvgWidth,
		Histogram:      info.Histogram,
		MostCommonVals: info.MostCommonVals,
		MCVFreqs:       info.MCVFreqs,
		LastUpdated:    info.LastUpdated,
	}
}

// NewHistogram creates a histogram from a list of values.
// This is a convenience wrapper around statistics.NewHistogram for backward compatibility.
func NewHistogram(values []types.Field, bucketCount int) *statistics.Histogram {
	return statistics.NewHistogram(values, bucketCount)
}
