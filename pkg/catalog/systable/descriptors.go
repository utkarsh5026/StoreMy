package systable

import (
	"fmt"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
)

// SystemTableDescriptor holds all static, compile-time metadata for a system table.
// It has NO dependency on CatalogAccess or any I/O layer.
type SystemTableDescriptor[T any] struct {
	name          string
	primaryKey    string
	tableIdIndex  int
	schemaFn      func() *schema.Schema
	createTupleFn func(td *tuple.TupleDescription, data T) *tuple.Tuple
	parseTupleFn  func(t *tuple.Tuple) (T, error)
}

func (std *SystemTableDescriptor[T]) Schema() *schema.Schema {
	return std.schemaFn()
}

func (std *SystemTableDescriptor[T]) TableName() string {
	return std.name
}

func (std *SystemTableDescriptor[T]) PrimaryKey() string {
	return std.primaryKey
}

func (std *SystemTableDescriptor[T]) TableIDIndex() int {
	return std.tableIdIndex
}

func (std *SystemTableDescriptor[T]) CreateTuple(data T) *tuple.Tuple {
	return std.createTupleFn(std.Schema().TupleDesc, data)
}

func (std *SystemTableDescriptor[T]) ParseTuple(t *tuple.Tuple) (T, error) {
	return std.parseTupleFn(t)
}

var TablesTableDescriptor = SystemTableDescriptor[TableMetadata]{
	name:         "CATALOG_TABLES",
	primaryKey:   "table_id",
	tableIdIndex: 0,
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_TABLES").
			AddPrimaryKey("table_id", types.Uint64Type).
			AddColumn("table_name", types.StringType).
			AddColumn("file_path", types.StringType).
			AddColumn("primary_key", types.StringType).
			Build()
		return sch
	},
	createTupleFn: func(td *tuple.TupleDescription, d TableMetadata) *tuple.Tuple {
		return tuple.NewBuilder(td).
			AddUint64(uint64(d.TableID)).
			AddString(d.TableName).
			AddString(string(d.FilePath)).
			AddString(d.PrimaryKeyCol).
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (TableMetadata, error) {
		p := tuple.NewParser(t).ExpectFields(4)

		tableID := primitives.FileID(p.ReadUint64())
		tableName := p.ReadString()
		filePath := p.ReadString()
		primaryKey := p.ReadString()

		var zero TableMetadata
		if err := p.Error(); err != nil {
			return zero, err
		}

		if tableID == InvalidTableID {
			return zero, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
		}

		if tableName == "" {
			return zero, fmt.Errorf("table_name cannot be empty")
		}

		if filePath == "" {
			return zero, fmt.Errorf("file_path cannot be empty")
		}

		return TableMetadata{
			TableID:       tableID,
			TableName:     tableName,
			FilePath:      primitives.Filepath(filePath),
			PrimaryKeyCol: primaryKey,
		}, nil
	},
}

var ColumnsTableDescriptor = SystemTableDescriptor[schema.ColumnMetadata]{
	name:         "CATALOG_COLUMNS",
	primaryKey:   "", // composite key of (table_id, column_name)
	tableIdIndex: 0,
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_COLUMNS").
			AddColumn("table_id", types.Uint64Type).
			AddColumn("column_name", types.StringType).
			AddColumn("type_id", types.IntType).
			AddColumn("position", types.Uint32Type).
			AddColumn("is_primary_key", types.BoolType).
			AddColumn("is_auto_increment", types.BoolType).
			AddColumn("next_auto_value", types.Uint64Type).
			Build()

		return sch
	},
	createTupleFn: func(td *tuple.TupleDescription, c schema.ColumnMetadata) *tuple.Tuple {
		return tuple.NewBuilder(td).
			AddUint64(uint64(c.TableID)).
			AddString(c.Name).
			AddInt(int64(c.FieldType)).
			AddUint32(uint32(c.Position)).
			AddBool(c.IsPrimary).
			AddBool(c.IsAutoInc).
			AddUint64((c.NextAutoValue)). // Start auto-increment at 1
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (schema.ColumnMetadata, error) {
		p := tuple.NewParser(t).ExpectFields(7)

		tableID := p.ReadUint64()
		name := p.ReadString()
		typeID := p.ReadInt()
		position := p.ReadUint32()
		isPrimary := p.ReadBool()
		isAutoInc := p.ReadBool()
		nextAutoValue := p.ReadUint64()

		var zero schema.ColumnMetadata
		if err := p.Error(); err != nil {
			return zero, err
		}

		if name == "" {
			return zero, fmt.Errorf("column name cannot be empty")
		}

		fieldType := types.Type(typeID)
		if !types.IsValidType(fieldType) {
			return zero, fmt.Errorf("invalid type_id %d: not a recognized type", typeID)
		}

		if isAutoInc {
			if fieldType != types.IntType {
				return zero, fmt.Errorf("auto-increment column must be INT type, got type_id %d", typeID)
			}

			if nextAutoValue < 1 {
				return zero, fmt.Errorf("invalid next_auto_value %d: must be >= 1", nextAutoValue)
			}
		}

		return schema.ColumnMetadata{
			Name:          name,
			FieldType:     fieldType,
			Position:      primitives.ColumnID(position),
			IsPrimary:     isPrimary,
			IsAutoInc:     isAutoInc,
			NextAutoValue: nextAutoValue,
			TableID:       primitives.FileID(tableID),
		}, nil
	},
}

var IndexesTableDescriptor = SystemTableDescriptor[IndexMetadata]{
	name:         "CATALOG_INDEXES",
	primaryKey:   "index_id",
	tableIdIndex: 2, // table_id is at position 2
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_INDEXES").
			AddPrimaryKey("index_id", types.Uint64Type).
			AddColumn("index_name", types.StringType).
			AddColumn("table_id", types.Uint64Type).
			AddColumn("column_name", types.StringType).
			AddColumn("index_type", types.StringType).
			AddColumn("file_path", types.StringType).
			AddColumn("created_at", types.Int64Type).
			Build()
		return sch
	},
	createTupleFn: func(td *tuple.TupleDescription, im IndexMetadata) *tuple.Tuple {
		return tuple.NewBuilder(td).
			AddUint64(uint64(im.IndexID)).
			AddString(im.IndexName).
			AddUint64(uint64(im.TableID)).
			AddString(im.ColumnName).
			AddString(string(im.IndexType)).
			AddString(string(im.FilePath)).
			AddInt64(im.CreatedAt.Unix()).
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (IndexMetadata, error) {
		p := tuple.NewParser(t).ExpectFields(7)

		indexID := primitives.FileID(p.ReadUint64())
		indexName := p.ReadString()
		tableID := primitives.FileID(p.ReadUint64())
		columnName := p.ReadString()
		indexTypeStr := p.ReadString()
		filePath := p.ReadString()
		createdAt := p.ReadTimestamp()

		var zero IndexMetadata
		if err := p.Error(); err != nil {
			return zero, err
		}

		if indexID == 0 {
			return zero, fmt.Errorf("invalid index_id %d: must be positive", indexID)
		}

		if indexName == "" {
			return zero, fmt.Errorf("index_name cannot be empty")
		}

		if tableID == InvalidTableID {
			return zero, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
		}

		if columnName == "" {
			return zero, fmt.Errorf("column_name cannot be empty")
		}

		indexType := index.IndexType(indexTypeStr)
		if indexType != index.HashIndex && indexType != index.BTreeIndex {
			return zero, fmt.Errorf("invalid index_type %s: must be HASH or BTREE", indexTypeStr)
		}

		if filePath == "" {
			return zero, fmt.Errorf("file_path cannot be empty")
		}

		return IndexMetadata{
			IndexID:    indexID,
			IndexName:  indexName,
			TableID:    tableID,
			ColumnName: columnName,
			IndexType:  indexType,
			FilePath:   primitives.Filepath(filePath),
			CreatedAt:  createdAt,
		}, nil
	},
}

var CatalogStatisticsTableDescriptor = SystemTableDescriptor[TableStatistics]{
	name:         "CATALOG_STATISTICS",
	primaryKey:   "table_id",
	tableIdIndex: 0,
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_STATISTICS").
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
	},
	createTupleFn: func(td *tuple.TupleDescription, s TableStatistics) *tuple.Tuple {
		return tuple.NewBuilder(td).
			AddUint64(uint64(s.TableID)).
			AddUint64(s.Cardinality).
			AddUint64(uint64(s.PageCount)).
			AddUint64(s.AvgTupleSize).
			AddInt64(s.LastUpdated.Unix()).
			AddUint64(s.DistinctValues).
			AddUint64(s.NullCount).
			AddString(s.MinValue).
			AddString(s.MaxValue).
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (TableStatistics, error) {
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

		var zero TableStatistics
		if err := p.Error(); err != nil {
			return zero, err
		}

		if tableID == InvalidTableID {
			return zero, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
		}

		if cardinality > 0 && avgTupleSize == 0 {
			return zero, fmt.Errorf("invalid avg_tuple_size %d: must be positive when cardinality > 0", avgTupleSize)
		}

		if distinctValues > cardinality {
			return zero, fmt.Errorf("invalid distinct_values %d: cannot exceed cardinality %d", distinctValues, cardinality)
		}

		if nullCount > cardinality {
			return zero, fmt.Errorf("invalid null_count %d: cannot exceed cardinality %d", nullCount, cardinality)
		}

		return TableStatistics{
			TableID:        tableID,
			Cardinality:    cardinality,
			PageCount:      primitives.PageNumber(pageCount),
			AvgTupleSize:   avgTupleSize,
			LastUpdated:    lastUpdated,
			DistinctValues: distinctValues,
			NullCount:      nullCount,
			MinValue:       minValue,
			MaxValue:       maxValue,
		}, nil
	},
}

var ColumnStatisticsTableDescriptor = SystemTableDescriptor[ColumnStatisticsRow]{
	name:         "CATALOG_COLUMN_STATISTICS",
	primaryKey:   "", // composite key of (table_id, column_name)
	tableIdIndex: 0,
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_COLUMN_STATISTICS").
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
	},
	createTupleFn: func(td *tuple.TupleDescription, c ColumnStatisticsRow) *tuple.Tuple {
		return tuple.NewBuilder(td).
			AddUint64(uint64(c.TableID)).
			AddString(c.ColumnName).
			AddUint32(uint32(c.ColumnIndex)).
			AddUint64(c.DistinctCount).
			AddUint64(c.NullCount).
			AddString(c.MinValue).
			AddString(c.MaxValue).
			AddUint64(c.AvgWidth).
			AddInt64(c.LastUpdated.Unix()).
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (ColumnStatisticsRow, error) {
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

		var zero ColumnStatisticsRow
		if err := p.Error(); err != nil {
			return zero, err
		}

		if tableID == InvalidTableID {
			return zero, fmt.Errorf("invalid table_id: cannot be InvalidTableID (%d)", InvalidTableID)
		}

		if columnName == "" {
			return zero, fmt.Errorf("invalid column_name: cannot be empty")
		}

		return ColumnStatisticsRow{
			TableID:       tableID,
			ColumnName:    columnName,
			ColumnIndex:   columnIndex,
			DistinctCount: distinctCount,
			NullCount:     nullCount,
			MinValue:      minValue,
			MaxValue:      maxValue,
			AvgWidth:      avgWidth,
			LastUpdated:   lastUpdated,
		}, nil
	},
}

var IndexStatisticsTableDescriptor = SystemTableDescriptor[IndexStatisticsRow]{
	name:         "CATALOG_INDEX_STATISTICS",
	primaryKey:   "index_id",
	tableIdIndex: 0,
	schemaFn: func() *schema.Schema {
		sch, _ := schema.NewSchemaBuilder(InvalidTableID, "CATALOG_INDEX_STATISTICS").
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
	},
	createTupleFn: func(td *tuple.TupleDescription, s IndexStatisticsRow) *tuple.Tuple {
		clusteringFactorInt := int64(s.ClusteringFactor * 1000000.0)
		return tuple.NewBuilder(td).
			AddUint64(uint64(s.IndexID)).
			AddUint64(uint64(s.TableID)).
			AddString(s.IndexName).
			AddString(string(s.IndexType)).
			AddString(s.ColumnName).
			AddUint64(s.NumEntries).
			AddUint64(uint64(s.NumPages)).
			AddUint32(s.BTreeHeight).
			AddUint64(s.DistinctKeys).
			AddInt(clusteringFactorInt).
			AddUint64(s.AvgKeySize).
			AddInt(int64(s.LastUpdated.Unix())).
			MustBuild()
	},
	parseTupleFn: func(t *tuple.Tuple) (IndexStatisticsRow, error) {
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

		var zero IndexStatisticsRow
		if err := p.Error(); err != nil {
			return zero, err
		}

		if indexID == 0 {
			return zero, fmt.Errorf("invalid index_id: must be positive")
		}

		if tableID == 0 {
			return zero, fmt.Errorf("invalid table_id: must be positive")
		}

		if indexName == "" {
			return zero, fmt.Errorf("invalid index_name: cannot be empty")
		}

		if indexType == "" {
			return zero, fmt.Errorf("invalid index_type: cannot be empty")
		}

		idxType, err := index.ParseIndexType(indexType)
		if err != nil {
			return zero, fmt.Errorf("error in parsing the index type must be HASH or BTREE")
		}

		if columnName == "" {
			return zero, fmt.Errorf("invalid column_name: cannot be empty")
		}

		if distinctKeys > numEntries {
			return zero, fmt.Errorf("invalid distinct_keys %d: cannot exceed num_entries %d", distinctKeys, numEntries)
		}

		if clusteringFactor < 0.0 || clusteringFactor > 1.0 {
			return zero, fmt.Errorf("invalid clustering_factor: must be between 0.0 and 1.0")
		}

		return IndexStatisticsRow{
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
		}, nil
	},
}

// SystemTableBase defines common operations for all system tables
type SystemTableBase interface {
	Schema() *schema.Schema
	TableName() string
	PrimaryKey() string
	TableIDIndex() int
}

// AllSystemTables returns all system table descriptors
var AllSystemTables = []SystemTableBase{
	&TablesTableDescriptor,
	&ColumnsTableDescriptor,
	&IndexesTableDescriptor,
	&CatalogStatisticsTableDescriptor,
	&ColumnStatisticsTableDescriptor,
	&IndexStatisticsTableDescriptor,
}
