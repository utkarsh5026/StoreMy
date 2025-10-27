package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// TableStatistics holds statistics about a table for query optimization
type TableStatistics struct {
	TableID        primitives.FileID     // Table identifier
	Cardinality    uint64                // Number of tuples in the table
	PageCount      primitives.PageNumber // Number of pages used by the table
	AvgTupleSize   uint64                // Average tuple size in bytes
	LastUpdated    time.Time             // When statistics were last updated
	DistinctValues uint64                // Approximate number of distinct values (for primary key)
	NullCount      uint64                // Number of null values
	MinValue       string                // Min value for primary key (if applicable)
	MaxValue       string                // Max value for primary key (if applicable)
}

type StatsTable struct {
}

// Schema returns the schema for the CATALOG_STATS system table.
func (st *StatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, st.TableName()).
		AddPrimaryKey("table_id", types.Uint64Type).
		AddColumn("cardinality", types.Uint64Type).
		AddColumn("page_count", types.Uint64Type).
		AddColumn("avg_tuple_size", types.Uint64Type).
		AddColumn("last_updated", types.Int64Type).
		AddColumn("distinct_values", types.Uint64Type).
		AddColumn("null_count", types.Uint64Type).
		AddColumn("min_value", types.StringType).
		AddColumn("max_value", types.StringType).
		Build()

	return sch
}

func (st *StatsTable) FileName() string {
	return "catalog_statistics.dat"
}

func (st *StatsTable) TableName() string {
	return "CATALOG_STATISTICS"
}

func (st *StatsTable) PrimaryKey() string {
	return "table_id"
}

func (tt *StatsTable) TableIDIndex() int {
	return 0
}

func (st *StatsTable) Parse(t *tuple.Tuple) (*TableStatistics, error) {
	// Parse fields sequentially using the parser
	p := tuple.NewParser(t).ExpectFields(9)

	tableID := primitives.FileID(p.ReadUint64())
	cardinality := p.ReadUint64()
	pageCount := p.ReadUint64()
	avgTupleSize := p.ReadUint64()
	lastUpdated := p.ReadTimestamp()
	distinctValues := p.ReadUint64()
	nullCount := p.ReadUint64()
	minValue := p.ReadString()
	maxValue := p.ReadString()

	// Check for parsing errors (field count, type mismatches, etc.)
	if err := p.Error(); err != nil {
		return nil, err
	}

	// Custom validation logic
	if tableID == InvalidTableID {
		return nil, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	if cardinality > 0 && avgTupleSize == 0 {
		return nil, fmt.Errorf("invalid avg_tuple_size %d: must be positive when cardinality > 0", avgTupleSize)
	}

	if distinctValues > cardinality {
		return nil, fmt.Errorf("invalid distinct_values %d: cannot exceed cardinality %d", distinctValues, cardinality)
	}

	if nullCount > cardinality {
		return nil, fmt.Errorf("invalid null_count %d: cannot exceed cardinality %d", nullCount, cardinality)
	}

	result := &TableStatistics{
		TableID:        tableID,
		Cardinality:    cardinality,
		PageCount:      primitives.PageNumber(pageCount),
		AvgTupleSize:   avgTupleSize,
		LastUpdated:    lastUpdated,
		DistinctValues: distinctValues,
		NullCount:      nullCount,
		MinValue:       minValue,
		MaxValue:       maxValue,
	}

	return result, nil
}

// createStatisticsTuple creates a tuple for the statistics table
func (st *StatsTable) CreateTuple(stats *TableStatistics) *tuple.Tuple {
	return tuple.NewBuilder(st.Schema().TupleDesc).
		AddUint64(uint64(stats.TableID)).
		AddUint64(stats.Cardinality).
		AddUint64(uint64(stats.PageCount)).
		AddUint64(stats.AvgTupleSize).
		AddInt64(stats.LastUpdated.Unix()).
		AddUint64(stats.DistinctValues).
		AddUint64(stats.NullCount).
		AddString(stats.MinValue).
		AddString(stats.MaxValue).
		MustBuild()
}
