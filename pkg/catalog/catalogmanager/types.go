package catalogmanager

import (
	"storemy/pkg/catalog/systable"
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"time"
)

// TableStatistics is an alias for systable.TableStatistics used in public API methods.
type TableStatistics = systable.TableStatistics

// IndexStatistics is an alias for systable.IndexStatisticsRow used in public API methods.
type IndexStatistics = systable.IndexStatisticsRow

// ColumnStatistics represents detailed statistics for a single column.
// Used by the query optimizer for selectivity estimation and cardinality calculation.
type ColumnStatistics struct {
	TableID        primitives.FileID     // Table identifier
	ColumnName     string                // Column name
	ColumnIndex    primitives.ColumnID   // Column position in table (0-based)
	DistinctCount  uint64                // Number of distinct values (NDV)
	NullCount      uint64                // Number of NULL values
	MinValue       types.Field           // Minimum value in column
	MaxValue       types.Field           // Maximum value in column
	AvgWidth       uint64                // Average width in bytes (for variable length types)
	Histogram      *statistics.Histogram // Value distribution histogram
	MostCommonVals []types.Field         // Most common values (MCVs)
	MCVFreqs       []float64             // Frequencies of MCVs (0.0-1.0)
	LastUpdated    time.Time             // Last statistics update timestamp
}

// IndexInfo represents basic index metadata.
// This is a lightweight view of index information for query planning.
type IndexInfo struct {
	IndexID    primitives.FileID   // Unique index identifier
	TableID    primitives.FileID   // Table this index belongs to
	IndexName  string              // Index name
	IndexType  index.IndexType     // Index type (B-Tree, Hash, etc.)
	ColumnName string              // Column being indexed
	FilePath   primitives.Filepath // Path to index file on disk
}
