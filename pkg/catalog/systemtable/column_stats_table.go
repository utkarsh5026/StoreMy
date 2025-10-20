package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// ColumnStatisticsRow represents a row in the CATALOG_COLUMN_STATISTICS table
// Note: Histogram and MCV data are stored separately due to size constraints
type ColumnStatisticsRow struct {
	TableID       int       // Table identifier
	ColumnName    string    // Column name
	ColumnIndex   int       // Column position (0-based)
	DistinctCount int64     // Number of distinct values
	NullCount     int64     // Number of NULL values
	MinValue      string    // Minimum value (as string)
	MaxValue      string    // Maximum value (as string)
	AvgWidth      int       // Average width in bytes
	LastUpdated   time.Time // Last update timestamp
}

type ColumnStatsTable struct{}

// Schema returns the schema for the CATALOG_COLUMN_STATISTICS system table
func (cst *ColumnStatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, cst.TableName()).
		AddColumn("table_id", types.IntType).
		AddColumn("column_name", types.StringType).
		AddColumn("column_index", types.IntType).
		AddColumn("distinct_count", types.IntType).
		AddColumn("null_count", types.IntType).
		AddColumn("min_value", types.StringType).
		AddColumn("max_value", types.StringType).
		AddColumn("avg_width", types.IntType).
		AddColumn("last_updated", types.IntType).
		Build()

	return sch
}

func (cst *ColumnStatsTable) FileName() string {
	return "catalog_column_statistics.dat"
}

func (cst *ColumnStatsTable) TableName() string {
	return "CATALOG_COLUMN_STATISTICS"
}

func (cst *ColumnStatsTable) PrimaryKey() string {
	return "table_id" // Composite key with column_name in practice
}

func (cst *ColumnStatsTable) TableIDIndex() int {
	return 0
}

// GetTableID extracts the table ID from a column statistics tuple
func (cst *ColumnStatsTable) GetTableID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != 9 {
		return 0, fmt.Errorf("invalid tuple: expected 9 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	if tableID == InvalidTableID {
		return 0, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	return tableID, nil
}

// Parse converts a tuple into a ColumnStatisticsRow
func (cst *ColumnStatsTable) Parse(t *tuple.Tuple) (*ColumnStatisticsRow, error) {
	tableID, err := cst.GetTableID(t)
	if err != nil {
		return nil, err
	}

	columnName := getStringField(t, 1)
	columnIndex := getIntField(t, 2)
	distinctCount := int64(getIntField(t, 3))
	nullCount := int64(getIntField(t, 4))
	minValue := getStringField(t, 5)
	maxValue := getStringField(t, 6)
	avgWidth := getIntField(t, 7)
	lastUpdatedUnix := getIntField(t, 8)

	// Validation
	if columnName == "" {
		return nil, fmt.Errorf("invalid column_name: cannot be empty")
	}

	if columnIndex < 0 {
		return nil, fmt.Errorf("invalid column_index %d: must be non-negative", columnIndex)
	}

	if distinctCount < 0 {
		return nil, fmt.Errorf("invalid distinct_count %d: must be non-negative", distinctCount)
	}

	if nullCount < 0 {
		return nil, fmt.Errorf("invalid null_count %d: must be non-negative", nullCount)
	}

	if avgWidth < 0 {
		return nil, fmt.Errorf("invalid avg_width %d: must be non-negative", avgWidth)
	}

	result := &ColumnStatisticsRow{
		TableID:       tableID,
		ColumnName:    columnName,
		ColumnIndex:   columnIndex,
		DistinctCount: distinctCount,
		NullCount:     nullCount,
		MinValue:      minValue,
		MaxValue:      maxValue,
		AvgWidth:      avgWidth,
		LastUpdated:   time.Unix(int64(lastUpdatedUnix), 0),
	}

	return result, nil
}

// CreateTuple creates a tuple for the column statistics table
func (cst *ColumnStatsTable) CreateTuple(stats *ColumnStatisticsRow) *tuple.Tuple {
	return tuple.NewBuilder(cst.Schema().TupleDesc).
		AddInt(int64(stats.TableID)).
		AddString(stats.ColumnName).
		AddInt(int64(stats.ColumnIndex)).
		AddInt(int64(stats.DistinctCount)).
		AddInt(int64(stats.NullCount)).
		AddString(stats.MinValue).
		AddString(stats.MaxValue).
		AddInt(int64(stats.AvgWidth)).
		AddInt(int64(stats.LastUpdated.Unix())).
		MustBuild()
}
