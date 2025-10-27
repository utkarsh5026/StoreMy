package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// ColumnStatisticsRow represents a row in the CATALOG_COLUMN_STATISTICS table
// Note: Histogram and MCV data are stored separately due to size constraints
type ColumnStatisticsRow struct {
	TableID       primitives.FileID   // Table identifier
	ColumnName    string              // Column name
	ColumnIndex   primitives.ColumnID // Column position (0-based)
	DistinctCount uint64              // Number of distinct values
	NullCount     uint64              // Number of NULL values
	MinValue      string              // Minimum value (as string)
	MaxValue      string              // Maximum value (as string)
	AvgWidth      uint64              // Average width in bytes
	LastUpdated   time.Time           // Last update timestamp
}

type ColumnStatsTable struct{}

// Schema returns the schema for the CATALOG_COLUMN_STATISTICS system table
func (cst *ColumnStatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, cst.TableName()).
		AddColumn("table_id", types.Uint64Type).
		AddColumn("column_name", types.StringType).
		AddColumn("column_index", types.Uint32Type).
		AddColumn("distinct_count", types.Uint64Type).
		AddColumn("null_count", types.Uint64Type).
		AddColumn("min_value", types.StringType).
		AddColumn("max_value", types.StringType).
		AddColumn("avg_width", types.Uint64Type).
		AddColumn("last_updated", types.Int64Type).
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
func (cst *ColumnStatsTable) GetTableID(t *tuple.Tuple) (primitives.FileID, error) {
	if t.TupleDesc.NumFields() != 9 {
		return 0, fmt.Errorf("invalid tuple: expected 9 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := primitives.FileID(getUint64Field(t, 0))
	if tableID == InvalidTableID {
		return 0, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	return tableID, nil
}

// Parse converts a tuple into a ColumnStatisticsRow
func (cst *ColumnStatsTable) Parse(t *tuple.Tuple) (*ColumnStatisticsRow, error) {
	p := tuple.NewParser(t).ExpectFields(9)

	tableID := primitives.FileID(p.ReadUint64())
	columnName := p.ReadString()
	columnIndex := primitives.ColumnID(p.ReadUint32())
	distinctCount := p.ReadUint64()
	nullCount := p.ReadUint64()
	minValue := p.ReadString()
	maxValue := p.ReadString()
	avgWidth := p.ReadUint64()
	lastUpdated := p.ReadTimestamp()

	if err := p.Error(); err != nil {
		return nil, err
	}

	if tableID == InvalidTableID {
		return nil, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	if columnName == "" {
		return nil, fmt.Errorf("invalid column_name: cannot be empty")
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
		LastUpdated:   lastUpdated,
	}

	return result, nil
}

// CreateTuple creates a tuple for the column statistics table
func (cst *ColumnStatsTable) CreateTuple(stats *ColumnStatisticsRow) *tuple.Tuple {
	return tuple.NewBuilder(cst.Schema().TupleDesc).
		AddUint64(uint64(stats.TableID)).
		AddString(stats.ColumnName).
		AddUint32(uint32(stats.ColumnIndex)).
		AddUint64(stats.DistinctCount).
		AddUint64(stats.NullCount).
		AddString(stats.MinValue).
		AddString(stats.MaxValue).
		AddUint64(stats.AvgWidth).
		AddInt64(stats.LastUpdated.Unix()).
		MustBuild()
}
