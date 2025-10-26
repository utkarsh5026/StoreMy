package catalogmanager

import (
	"fmt"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/optimizer/statistics"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"time"
)

// Type aliases for convenience
type (
	TableStatistics   = systemtable.TableStatistics
	AutoIncrementInfo = *operations.AutoIncrementInfo
	TID               = *primitives.TransactionID
	TxContext         = *transaction.TransactionContext
	TableSchema       = *schema.Schema
	Tuple             = *tuple.Tuple
	IndexStatistics   = systemtable.IndexStatisticsRow
)

// SystemTableIDs tracks the file IDs of all system catalog tables.
// These IDs are assigned during initialization when heap files are created.
//
// System tables:
//   - CATALOG_TABLES: table metadata
//   - CATALOG_COLUMNS: column definitions
//   - CATALOG_STATISTICS: table statistics
//   - CATALOG_INDEXES: index metadata
//   - CATALOG_COLUMN_STATISTICS: column-level statistics
//   - CATALOG_INDEX_STATISTICS: index statistics
type SystemTableIDs struct {
	TablesTableID           int
	ColumnsTableID          int
	StatisticsTableID       int
	IndexesTableID          int
	ColumnStatisticsTableID int
	IndexStatisticsTableID  int
}

// GetSysTable returns the SystemTable interface for a given system table ID.
// This is used to access system table-specific methods like TableIDIndex().
func (st *SystemTableIDs) GetSysTable(id int) (systemtable.SystemTable, error) {
	switch id {
	case st.TablesTableID:
		return systemtable.Tables, nil
	case st.ColumnsTableID:
		return systemtable.Columns, nil
	case st.StatisticsTableID:
		return systemtable.Stats, nil
	case st.IndexesTableID:
		return systemtable.Indexes, nil
	case st.ColumnStatisticsTableID:
		return systemtable.ColumnStats, nil
	case st.IndexStatisticsTableID:
		return systemtable.IndexStats, nil
	default:
		return nil, fmt.Errorf("unknown system table ID: %d", id)
	}
}

// SetSystemTableID sets the appropriate system table ID field based on table name.
// Called during initialization to track the IDs of all system catalog tables.
func (st *SystemTableIDs) SetSystemTableID(tableName string, tableID int) {
	switch tableName {
	case systemtable.Tables.TableName():
		st.TablesTableID = tableID
	case systemtable.Columns.TableName():
		st.ColumnsTableID = tableID
	case systemtable.Stats.TableName():
		st.StatisticsTableID = tableID
	case systemtable.Indexes.TableName():
		st.IndexesTableID = tableID
	case systemtable.ColumnStats.TableName():
		st.ColumnStatisticsTableID = tableID
	case systemtable.IndexStats.TableName():
		st.IndexStatisticsTableID = tableID
	}
}

// ColumnStatistics represents detailed statistics for a single column.
// Used by the query optimizer for selectivity estimation and cardinality calculation.
type ColumnStatistics struct {
	TableID        int                     // Table identifier
	ColumnName     string                  // Column name
	ColumnIndex    int                     // Column position in table (0-based)
	DistinctCount  int64                   // Number of distinct values (NDV)
	NullCount      int64                   // Number of NULL values
	MinValue       types.Field             // Minimum value in column
	MaxValue       types.Field             // Maximum value in column
	AvgWidth       int                     // Average width in bytes (for variable length types)
	Histogram      *statistics.Histogram   // Value distribution histogram
	MostCommonVals []types.Field           // Most common values (MCVs)
	MCVFreqs       []float64               // Frequencies of MCVs (0.0-1.0)
	LastUpdated    time.Time               // Last statistics update timestamp
}

// IndexInfo represents basic index metadata.
// This is a lightweight view of index information for query planning.
type IndexInfo struct {
	IndexID    int             // Unique index identifier
	TableID    int             // Table this index belongs to
	IndexName  string          // Index name
	IndexType  index.IndexType // Index type (B-Tree, Hash, etc.)
	ColumnName string          // Column being indexed
	FilePath   string          // Path to index file on disk
}
