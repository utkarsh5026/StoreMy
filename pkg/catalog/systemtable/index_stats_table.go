package systemtable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// IndexStatisticsRow represents a row in the CATALOG_INDEX_STATISTICS table
type IndexStatisticsRow struct {
	IndexID          primitives.FileID     // Index identifier
	TableID          primitives.FileID     // Table this index belongs to
	IndexName        string                // Index name
	IndexType        index.IndexType       // "btree", "hash", etc.
	ColumnName       string                // Indexed column
	NumEntries       uint64                // Number of entries in index
	NumPages         primitives.PageNumber // Number of pages
	BTreeHeight      uint32                // Tree height (for B-tree)
	DistinctKeys     uint64                // Number of distinct keys
	ClusteringFactor float64               // 0.0-1.0: table ordering by index
	AvgKeySize       uint64                // Average key size in bytes
	LastUpdated      time.Time             // Last update timestamp
}

type IndexStatsTable struct{}

// Schema returns the schema for the CATALOG_INDEX_STATISTICS system table
func (ist *IndexStatsTable) Schema() *schema.Schema {
	sch, _ := schema.NewSchemaBuilder(InvalidTableID, ist.TableName()).
		AddPrimaryKey("index_id", types.Uint64Type).
		AddColumn("table_id", types.Uint64Type).
		AddColumn("index_name", types.StringType).
		AddColumn("index_type", types.StringType).
		AddColumn("column_name", types.StringType).
		AddColumn("num_entries", types.Uint64Type).
		AddColumn("num_pages", types.Uint64Type).
		AddColumn("height", types.Uint32Type).
		AddColumn("distinct_keys", types.Uint64Type).
		// Clustering factor stored as int (0-1000000) for precision
		AddColumn("clustering_factor", types.IntType).
		AddColumn("avg_key_size", types.Uint64Type).
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
func (ist *IndexStatsTable) GetIndexID(t *tuple.Tuple) (primitives.FileID, error) {
	if t.TupleDesc.NumFields() != 12 {
		return 0, fmt.Errorf("invalid tuple: expected 12 fields, got %d", t.TupleDesc.NumFields())
	}

	indexID := primitives.FileID(getUint64Field(t, 0))
	if indexID == 0 {
		return 0, fmt.Errorf("invalid index_id: must be positive")
	}

	return indexID, nil
}

// Parse converts a tuple into an IndexStatisticsRow
func (ist *IndexStatsTable) Parse(t *tuple.Tuple) (*IndexStatisticsRow, error) {
	// Parse fields sequentially using the parser
	p := tuple.NewParser(t).ExpectFields(12)

	indexID := primitives.FileID(p.ReadUint64())
	tableID := primitives.FileID(p.ReadUint64())
	indexName := p.ReadString()
	indexType := p.ReadString()
	columnName := p.ReadString()
	numEntries := p.ReadUint64()
	numPages := primitives.PageNumber(p.ReadUint64())
	height := p.ReadUint32()
	distinctKeys := p.ReadUint64()
	clusteringFactor := p.ReadScaledFloat(1000000)
	avgKeySize := p.ReadUint64()
	lastUpdated := p.ReadTimestamp()

	// Check for parsing errors (field count, type mismatches, etc.)
	if err := p.Error(); err != nil {
		return nil, err
	}

	// Custom validation logic
	if indexID == 0 {
		return nil, fmt.Errorf("invalid index_id: must be positive")
	}

	if tableID == 0 {
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

	if distinctKeys > numEntries {
		return nil, fmt.Errorf("invalid distinct_keys %d: cannot exceed num_entries %d", distinctKeys, numEntries)
	}

	if clusteringFactor < 0.0 || clusteringFactor > 1.0 {
		return nil, fmt.Errorf("invalid clustering_factor: must be between 0.0 and 1.0")
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
		LastUpdated:      lastUpdated,
	}

	return result, nil
}

// CreateTuple creates a tuple for the index statistics table
func (ist *IndexStatsTable) CreateTuple(stats *IndexStatisticsRow) *tuple.Tuple {
	clusteringFactorInt := int64(stats.ClusteringFactor * 1000000.0)
	return tuple.NewBuilder(ist.Schema().TupleDesc).
		AddUint64(uint64(stats.IndexID)).
		AddUint64(uint64(stats.TableID)).
		AddString(stats.IndexName).
		AddString(string(stats.IndexType)).
		AddString(stats.ColumnName).
		AddUint64(stats.NumEntries).
		AddUint64(uint64(stats.NumPages)).
		AddUint32(stats.BTreeHeight).
		AddUint64(stats.DistinctKeys).
		AddInt(clusteringFactorInt).
		AddUint64(stats.AvgKeySize).
		AddInt(int64(stats.LastUpdated.Unix())).
		MustBuild()
}
