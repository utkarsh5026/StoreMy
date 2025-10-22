package operations

import (
	"fmt"
	"sort"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

const (
	// DefaultHistogramBuckets is the default number of histogram buckets for column statistics
	DefaultHistogramBuckets = 10
	// DefaultMCVCount is the default number of most common values to track
	DefaultMCVCount = 5
)

type colStats = systemtable.ColumnStatisticsRow

// ColStatsInfo contains column statistics along with histogram and most common values
type ColStatsInfo struct {
	colStats
	Histogram      *statistics.Histogram
	MostCommonVals []types.Field
	MCVFreqs       []float64
}

type ColStatsOperations struct {
	fileGetter FileGetter
	BaseOperations[*colStats]
	colOps *ColumnOperations
}

// NewColStatsOperations creates a new ColStatsOperations instance.
//
// Parameters:
//   - access: CatalogAccess interface providing both read and write capabilities
//   - colStatsTableID: The table ID of the CATALOG_COLUMN_STATISTICS system table
//   - fileGetter: Function to retrieve database files by table ID
//   - colOps: ColumnOperations instance for accessing column metadata
//
// Returns a configured ColStatsOperations ready to collect and manage column statistics.
func NewColStatsOperations(
	access catalogio.CatalogAccess,
	colStatsTableID int,
	fileGetter FileGetter,
	colOps *ColumnOperations,
) *ColStatsOperations {
	base := NewBaseOperations(
		access,
		colStatsTableID,
		systemtable.ColumnStats.Parse,
		func(cs *colStats) *Tuple {
			return systemtable.ColumnStats.CreateTuple(cs)
		},
	)

	return &ColStatsOperations{
		fileGetter:     fileGetter,
		BaseOperations: *base,
		colOps:         colOps,
	}
}

// CollectColumnStatistics collects statistics for a specific column in a table.
// It scans all tuples in the table and computes:
// - Null count, distinct count, min/max values, average width
// - Histogram distribution
// - Most common values (MCV) and their frequencies
func (co *ColStatsOperations) CollectColumnStatistics(tx TxContext, colName string, tableID, columnIndex, histogramBuckets, mcvCount int) (*ColStatsInfo, error) {
	// Verify table exists
	if _, err := co.fileGetter(tableID); err != nil {
		return nil, fmt.Errorf("failed to get db file: %w", err)
	}

	var (
		values     []types.Field
		nullCount  int64
		totalWidth int
	)

	// Scan table and collect column values
	err := co.Reader().IterateTable(tableID, tx, func(t *tuple.Tuple) error {
		if columnIndex >= t.TupleDesc.NumFields() {
			return fmt.Errorf("column index %d out of range (tuple has %d fields)", columnIndex, t.TupleDesc.NumFields())
		}

		field, err := t.GetField(columnIndex)
		if err != nil {
			return err
		}

		if field == nil {
			nullCount++
			return nil
		}

		values = append(values, field)

		// Calculate field width
		if strField, ok := field.(*types.StringField); ok {
			totalWidth += len(strField.Value)
		} else {
			totalWidth += int(field.Type().Size())
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate table: %w", err)
	}

	// Initialize base statistics
	stats := &colStats{
		TableID:     tableID,
		ColumnName:  colName,
		ColumnIndex: columnIndex,
		NullCount:   nullCount,
		LastUpdated: time.Now(),
	}

	// Handle empty column (all nulls)
	if len(values) == 0 {
		stats.DistinctCount = 0
		stats.AvgWidth = 0
		return &ColStatsInfo{colStats: *stats}, nil
	}

	// Sort values for min/max and distinct counting
	sort.Slice(values, func(i, j int) bool {
		result, _ := values[i].Compare(primitives.LessThan, values[j])
		return result
	})

	// Compute derived statistics
	stats.DistinctCount = int64(countDistinct(values))
	stats.MinValue = values[0].String()
	stats.MaxValue = values[len(values)-1].String()
	stats.AvgWidth = totalWidth / len(values)

	// Build comprehensive statistics info
	statsInfo := &ColStatsInfo{
		colStats:  *stats,
		Histogram: statistics.NewHistogram(values, histogramBuckets),
	}
	statsInfo.MostCommonVals, statsInfo.MCVFreqs = findMostCommonValues(values, mcvCount)

	return statsInfo, nil
}

// UpdateColumnStatistics updates statistics for all columns in a table
func (co *ColStatsOperations) UpdateColumnStatistics(tx TxContext, tableID int) error {
	columns, err := co.colOps.LoadColumnMetadata(tx, tableID)
	if err != nil {
		return fmt.Errorf("failed to load column metadata: %w", err)
	}

	for _, col := range columns {
		stats, err := co.CollectColumnStatistics(tx, col.Name, tableID, col.Position, DefaultHistogramBuckets, DefaultMCVCount)
		if err != nil {
			return fmt.Errorf("failed to collect statistics for column %s: %w", col.Name, err)
		}

		if err := co.storeColumnStatistics(tx, &stats.colStats); err != nil {
			return fmt.Errorf("failed to store statistics for column %s: %w", col.Name, err)
		}
	}

	return nil
}

// storeColumnStatistics stores column statistics in CATALOG_COLUMN_STATISTICS system table
func (co *ColStatsOperations) storeColumnStatistics(tx TxContext, stats *colStats) error {
	err := co.DeleteBy(tx, func(cs *colStats) bool {
		return cs.TableID == stats.TableID && cs.ColumnName == stats.ColumnName
	})

	if err != nil {
		return fmt.Errorf("failed to delete old statistics: %w", err)
	}

	if err := co.Insert(tx, stats); err != nil {
		return fmt.Errorf("failed to insert new statistics: %w", err)
	}

	return nil
}

// GetColumnStatistics retrieves statistics for a specific column from CATALOG_COLUMN_STATISTICS
func (co *ColStatsOperations) GetColumnStatistics(tx TxContext, tableID int, columnName string) (*colStats, error) {
	stats, err := co.FindOne(tx, func(cs *colStats) bool {
		return cs.TableID == tableID && cs.ColumnName == columnName
	})

	if err != nil {
		return nil, fmt.Errorf("column statistics not found for table %d, column %s", tableID, columnName)
	}

	return stats, nil
}

// countDistinct counts unique values in a sorted slice of fields.
// Assumes values are already sorted for efficient O(n) counting.
func countDistinct(sortedVals []types.Field) int {
	if len(sortedVals) == 0 {
		return 0
	}

	distinctCount := 1
	for i := 1; i < len(sortedVals); i++ {
		notEqual, _ := sortedVals[i].Compare(primitives.NotEqual, sortedVals[i-1])
		if notEqual {
			distinctCount++
		}
	}

	return distinctCount
}

// valueFrequency holds a value and its occurrence count for MCV computation
type valueFrequency struct {
	value types.Field
	count int
}

// findMostCommonValues finds the top N most common values and their relative frequencies.
// Returns two parallel slices: the values and their frequencies (as percentages of total).
func findMostCommonValues(values []types.Field, n int) ([]types.Field, []float64) {
	if len(values) == 0 {
		return nil, nil
	}

	// Build frequency map: value string -> (Field, count)
	freqMap := make(map[string]int)
	valueMap := make(map[string]types.Field)

	for _, val := range values {
		key := val.String()
		freqMap[key]++
		if _, exists := valueMap[key]; !exists {
			valueMap[key] = val
		}
	}

	// Convert to slice for sorting
	frequencies := make([]valueFrequency, 0, len(freqMap))
	for key, count := range freqMap {
		frequencies = append(frequencies, valueFrequency{
			value: valueMap[key],
			count: count,
		})
	}

	// Sort by frequency (descending)
	sort.Slice(frequencies, func(i, j int) bool {
		return frequencies[i].count > frequencies[j].count
	})

	// Limit to top N
	if len(frequencies) > n {
		frequencies = frequencies[:n]
	}

	// Extract results and calculate relative frequencies
	totalCount := float64(len(values))
	mcvValues := make([]types.Field, len(frequencies))
	mcvFreqs := make([]float64, len(frequencies))

	for i, vf := range frequencies {
		mcvValues[i] = vf.value
		mcvFreqs[i] = float64(vf.count) / totalCount
	}

	return mcvValues, mcvFreqs
}
