package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// TableStatistics holds statistics about a table for query optimization
type TableStatistics struct {
	TableID        int       // Table identifier
	Cardinality    int       // Number of tuples in the table
	PageCount      int       // Number of pages used by the table
	AvgTupleSize   int       // Average tuple size in bytes
	LastUpdated    time.Time // When statistics were last updated
	DistinctValues int       // Approximate number of distinct values (for primary key)
	NullCount      int       // Number of null values
	MinValue       string    // Min value for primary key (if applicable)
	MaxValue       string    // Max value for primary key (if applicable)
}

type StatsTable struct {
}

// Schema returns the schema for the CATALOG_STATS system table.
func (st *StatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, st.TableName()).
		AddPrimaryKey("table_id", types.IntType).
		AddColumn("cardinality", types.IntType).
		AddColumn("page_count", types.IntType).
		AddColumn("avg_tuple_size", types.IntType).
		AddColumn("last_updated", types.IntType).
		AddColumn("distinct_values", types.IntType).
		AddColumn("null_count", types.IntType).
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

func (st *StatsTable) GetTableID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != 9 {
		return 0, fmt.Errorf("invalid tuple: expected 9 fields, got %d", t.TupleDesc.NumFields())
	}

	tableID := getIntField(t, 0)
	// Allow any table ID (including negative for generated IDs), but not InvalidTableID (-1)
	// which is reserved for system table schemas
	if tableID == InvalidTableID {
		return 0, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
	}

	return tableID, nil
}

func (st *StatsTable) Parse(t *tuple.Tuple) (*TableStatistics, error) {
	// Parse fields sequentially using the parser
	p := tuple.NewParser(t).ExpectFields(9)

	tableID := p.ReadInt()
	cardinality := p.ReadInt()
	pageCount := p.ReadInt()
	avgTupleSize := p.ReadInt()
	lastUpdated := p.ReadTimestamp()
	distinctValues := p.ReadInt()
	nullCount := p.ReadInt()
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

	if cardinality < 0 {
		return nil, fmt.Errorf("invalid cardinality %d: must be non-negative", cardinality)
	}

	if pageCount < 0 {
		return nil, fmt.Errorf("invalid page_count %d: must be non-negative", pageCount)
	}

	if cardinality > 0 && avgTupleSize <= 0 {
		return nil, fmt.Errorf("invalid avg_tuple_size %d: must be positive when cardinality > 0", avgTupleSize)
	}

	if distinctValues > cardinality {
		return nil, fmt.Errorf("invalid distinct_values %d: cannot exceed cardinality %d", distinctValues, cardinality)
	}

	if distinctValues < 0 {
		return nil, fmt.Errorf("invalid distinct_values %d: must be non-negative", distinctValues)
	}

	if nullCount > cardinality {
		return nil, fmt.Errorf("invalid null_count %d: cannot exceed cardinality %d", nullCount, cardinality)
	}

	if nullCount < 0 {
		return nil, fmt.Errorf("invalid null_count %d: must be non-negative", nullCount)
	}

	result := &TableStatistics{
		TableID:        tableID,
		Cardinality:    cardinality,
		PageCount:      pageCount,
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
		AddInt(int64(stats.TableID)).
		AddInt(int64(stats.Cardinality)).
		AddInt(int64(stats.PageCount)).
		AddInt(int64(stats.AvgTupleSize)).
		AddInt(int64(stats.LastUpdated.Unix())).
		AddInt(int64(stats.DistinctValues)).
		AddInt(int64(stats.NullCount)).
		AddString(stats.MinValue).
		AddString(stats.MaxValue).
		MustBuild()
}
