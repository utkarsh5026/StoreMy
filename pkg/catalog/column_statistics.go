package catalog

import (
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"time"
)

// ColumnStatistics represents detailed statistics for a single column.
// This is the high-level API type returned to database clients.
// It wraps the system table data with runtime computed histogram and MCV data.
type ColumnStatistics struct {
	TableID        primitives.FileID     // Table identifier
	ColumnName     string                // Column name
	ColumnIndex    primitives.ColumnID   // Column position in table (0-based)
	DistinctCount  uint64                // Number of distinct values
	NullCount      uint64                // Number of NULL values
	MinValue       types.Field           // Minimum value in column
	MaxValue       types.Field           // Maximum value in column
	AvgWidth       uint64                // Average width in bytes (for variable length types)
	Histogram      *statistics.Histogram // Value distribution histogram
	MostCommonVals []types.Field         // Most common values (MCVs)
	MCVFreqs       []float64             // Frequencies of MCVs (0.0-1.0)
	LastUpdated    time.Time             // Last statistics update time
}

// NewHistogram creates a histogram from a list of values.
// This is a convenience wrapper around statistics.NewHistogram for backward compatibility.
func NewHistogram(values []types.Field, bucketCount int) *statistics.Histogram {
	return statistics.NewHistogram(values, bucketCount)
}
