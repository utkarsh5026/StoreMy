package systemtable

import (
	"fmt"
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
func (st *StatsTable) Schema() *tuple.TupleDescription {
	fieldTypes := []types.Type{
		types.IntType,    // table_id
		types.IntType,    // cardinality
		types.IntType,    // page_count
		types.IntType,    // avg_tuple_size
		types.IntType,    // last_updated (Unix timestamp as int32)
		types.IntType,    // distinct_values
		types.IntType,    // null_count
		types.StringType, // min_value
		types.StringType, // max_value
	}

	fieldNames := []string{
		"table_id",
		"cardinality",
		"page_count",
		"avg_tuple_size",
		"last_updated",
		"distinct_values",
		"null_count",
		"min_value",
		"max_value",
	}

	desc, _ := tuple.NewTupleDesc(fieldTypes, fieldNames)
	return desc
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
	t := tuple.NewTuple(st.Schema())
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
