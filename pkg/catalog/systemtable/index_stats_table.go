package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// IndexStatisticsRow represents a row in the CATALOG_INDEX_STATISTICS table
type IndexStatisticsRow struct {
	IndexID          int             // Index identifier
	TableID          int             // Table this index belongs to
	IndexName        string          // Index name
	IndexType        index.IndexType // "btree", "hash", etc.
	ColumnName       string          // Indexed column
	NumEntries       int64           // Number of entries in index
	NumPages         int             // Number of pages
	BTreeHeight      int             // Tree height (for B-tree)
	DistinctKeys     int64           // Number of distinct keys
	ClusteringFactor float64         // 0.0-1.0: table ordering by index
	AvgKeySize       int             // Average key size in bytes
	LastUpdated      time.Time       // Last update timestamp
}

type IndexStatsTable struct{}

// Schema returns the schema for the CATALOG_INDEX_STATISTICS system table
func (ist *IndexStatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, ist.TableName()).
		AddPrimaryKey("index_id", types.IntType).
		AddColumn("table_id", types.IntType).
		AddColumn("index_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("column_name", types.StringType).
		AddColumn("num_entries", types.IntType).
		AddColumn("num_pages", types.IntType).
		AddColumn("height", types.IntType).
		AddColumn("distinct_keys", types.IntType).
		// Clustering factor stored as int (0-1000000) for precision
		AddColumn("clustering_factor", types.IntType).
		AddColumn("avg_key_size", types.IntType).
		AddColumn("last_updated", types.IntType).
		Build()

	return sch
}

func (ist *IndexStatsTable) FileName() string {
	return "catalog_index_statistics.dat"
}

func (ist *IndexStatsTable) TableName() string {
	return "CATALOG_INDEX_STATISTICS"
}

func (ist *IndexStatsTable) PrimaryKey() string {
	return "index_id"
}

func (ist *IndexStatsTable) TableIDIndex() int {
	return 0
}

// GetIndexID extracts the index ID from an index statistics tuple
func (ist *IndexStatsTable) GetIndexID(t *tuple.Tuple) (int, error) {
	if t.TupleDesc.NumFields() != 12 {
		return 0, fmt.Errorf("invalid tuple: expected 12 fields, got %d", t.TupleDesc.NumFields())
	}

	indexID := getIntField(t, 0)
	if indexID <= 0 {
		return 0, fmt.Errorf("invalid index_id: must be positive")
	}

	return indexID, nil
}

// Parse converts a tuple into an IndexStatisticsRow
func (ist *IndexStatsTable) Parse(t *tuple.Tuple) (*IndexStatisticsRow, error) {
	indexID, err := ist.GetIndexID(t)
	if err != nil {
		return nil, err
	}

	tableID := getIntField(t, 1)
	indexName := getStringField(t, 2)
	indexType := getStringField(t, 3)
	columnName := getStringField(t, 4)
	numEntries := int64(getIntField(t, 5))
	numPages := getIntField(t, 6)
	height := getIntField(t, 7)
	distinctKeys := int64(getIntField(t, 8))
	clusteringFactorInt := getIntField(t, 9)
	avgKeySize := getIntField(t, 10)
	lastUpdatedUnix := getIntField(t, 11)

	// Validation
	if tableID <= 0 {
		return nil, fmt.Errorf("invalid table_id: must be positive")
	}

	if indexName == "" {
		return nil, fmt.Errorf("invalid index_name: cannot be empty")
	}

	if indexType == "" {
		return nil, fmt.Errorf("invalid index_type: cannot be empty")
	}

	idxType, err := index.ParseIndexType(indexType)
	if err != nil {
		return nil, fmt.Errorf("error in parsing the index type must be HASH or BTREE")
	}

	if columnName == "" {
		return nil, fmt.Errorf("invalid column_name: cannot be empty")
	}

	if numEntries < 0 {
		return nil, fmt.Errorf("invalid num_entries %d: must be non-negative", numEntries)
	}

	if numPages < 0 {
		return nil, fmt.Errorf("invalid num_pages %d: must be non-negative", numPages)
	}

	if height < 0 {
		return nil, fmt.Errorf("invalid height %d: must be non-negative", height)
	}

	if distinctKeys < 0 {
		return nil, fmt.Errorf("invalid distinct_keys %d: must be non-negative", distinctKeys)
	}

	if distinctKeys > numEntries {
		return nil, fmt.Errorf("invalid distinct_keys %d: cannot exceed num_entries %d", distinctKeys, numEntries)
	}

	// Convert clustering factor from int (0-1000000) to float64 (0.0-1.0)
	clusteringFactor := float64(clusteringFactorInt) / 1000000.0
	if clusteringFactor < 0.0 || clusteringFactor > 1.0 {
		return nil, fmt.Errorf("invalid clustering_factor: must be between 0.0 and 1.0")
	}

	if avgKeySize < 0 {
		return nil, fmt.Errorf("invalid avg_key_size %d: must be non-negative", avgKeySize)
	}

	result := &IndexStatisticsRow{
		IndexID:          indexID,
		TableID:          tableID,
		IndexName:        indexName,
		IndexType:        idxType,
		ColumnName:       columnName,
		NumEntries:       numEntries,
		NumPages:         numPages,
		BTreeHeight:      height,
		DistinctKeys:     distinctKeys,
		ClusteringFactor: clusteringFactor,
		AvgKeySize:       avgKeySize,
		LastUpdated:      time.Unix(int64(lastUpdatedUnix), 0),
	}

	return result, nil
}

// CreateTuple creates a tuple for the index statistics table
func (ist *IndexStatsTable) CreateTuple(stats *IndexStatisticsRow) *tuple.Tuple {
	clusteringFactorInt := int64(stats.ClusteringFactor * 1000000.0)
	return tuple.NewBuilder(ist.Schema().TupleDesc).
		AddInt(int64(stats.IndexID)).
		AddInt(int64(stats.TableID)).
		AddString(stats.IndexName).
		AddString(string(stats.IndexType)).
		AddString(stats.ColumnName).
		AddInt(int64(stats.NumEntries)).
		AddInt(int64(stats.NumPages)).
		AddInt(int64(stats.BTreeHeight)).
		AddInt(int64(stats.DistinctKeys)).
		AddInt(clusteringFactorInt).
		AddInt(int64(stats.AvgKeySize)).
		AddInt(int64(stats.LastUpdated.Unix())).
		MustBuild()
}
