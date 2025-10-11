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
	columns := make([]schema.ColumnMetadata, 0, 9)

	col0, _ := schema.NewColumnMetadata("table_id", types.IntType, 0, SystemTableStatisticsID, true, false)
	columns = append(columns, *col0)

	col1, _ := schema.NewColumnMetadata("cardinality", types.IntType, 1, SystemTableStatisticsID, false, false)
	columns = append(columns, *col1)

	col2, _ := schema.NewColumnMetadata("page_count", types.IntType, 2, SystemTableStatisticsID, false, false)
	columns = append(columns, *col2)

	col3, _ := schema.NewColumnMetadata("avg_tuple_size", types.IntType, 3, SystemTableStatisticsID, false, false)
	columns = append(columns, *col3)

	col4, _ := schema.NewColumnMetadata("last_updated", types.IntType, 4, SystemTableStatisticsID, false, false)
	columns = append(columns, *col4)

	col5, _ := schema.NewColumnMetadata("distinct_values", types.IntType, 5, SystemTableStatisticsID, false, false)
	columns = append(columns, *col5)

	col6, _ := schema.NewColumnMetadata("null_count", types.IntType, 6, SystemTableStatisticsID, false, false)
	columns = append(columns, *col6)

	col7, _ := schema.NewColumnMetadata("min_value", types.StringType, 7, SystemTableStatisticsID, false, false)
	columns = append(columns, *col7)

	col8, _ := schema.NewColumnMetadata("max_value", types.StringType, 8, SystemTableStatisticsID, false, false)
	columns = append(columns, *col8)

	sch, _ := schema.NewSchema(SystemTableStatisticsID, st.TableName(), columns)
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
	if tableID <= 0 {
		return 0, fmt.Errorf("invalid table_id %d: must be positive", tableID)
	}

	return tableID, nil
}

func (st *StatsTable) Parse(t *tuple.Tuple) (*TableStatistics, error) {
	tableID, err := st.GetTableID(t)
	if err != nil {
		return nil, err
	}
	cardinality := getIntField(t, 1)
	pageCount := getIntField(t, 2)
	avgTupleSize := getIntField(t, 3)
	lastUpdatedUnix := getIntField(t, 4)
	distinctValues := getIntField(t, 5)
	nullCount := getIntField(t, 6)
	minValue := getStringField(t, 7)
	maxValue := getStringField(t, 8)

	if cardinality < 0 {
		return nil, fmt.Errorf("invalid cardinality %d: must be non-negative", cardinality)
	}

	if pageCount < 0 {
		return nil, fmt.Errorf("invalid page_count %d: must be non-negative", pageCount)
	}

	if cardinality > 0 && avgTupleSize <= 0 {
		return nil, fmt.Errorf("invalid avg_tuple_size %d: must be positive when cardinality > 0", avgTupleSize)
	}

	if lastUpdatedUnix < 0 {
		return nil, fmt.Errorf("invalid last_updated %d: timestamp cannot be negative", lastUpdatedUnix)
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
		LastUpdated:    time.Unix(int64(lastUpdatedUnix), 0),
		DistinctValues: distinctValues,
		NullCount:      nullCount,
		MinValue:       minValue,
		MaxValue:       maxValue,
	}

	return result, nil
}

// createStatisticsTuple creates a tuple for the statistics table
func (st *StatsTable) CreateTuple(stats *TableStatistics) *tuple.Tuple {
	t := tuple.NewTuple(st.Schema().TupleDesc)
	t.SetField(0, types.NewIntField(int64(stats.TableID)))
	t.SetField(1, types.NewIntField(int64(stats.Cardinality)))
	t.SetField(2, types.NewIntField(int64(stats.PageCount)))
	t.SetField(3, types.NewIntField(int64(stats.AvgTupleSize)))
	t.SetField(4, types.NewIntField(int64(stats.LastUpdated.Unix())))
	t.SetField(5, types.NewIntField(int64(stats.DistinctValues)))
	t.SetField(6, types.NewIntField(int64(stats.NullCount)))
	t.SetField(7, types.NewStringField(stats.MinValue, types.StringMaxSize))
	t.SetField(8, types.NewStringField(stats.MaxValue, types.StringMaxSize))
	return t
}
