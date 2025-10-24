package catalog

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
)

// System tables initialized:
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
//
// Parameters:
//   - id: System table ID
//
// Returns the corresponding SystemTable interface or an error if the ID is invalid.
func (sc *SystemTableIDs) GetSysTable(id int) (systemtable.SystemTable, error) {
	switch id {
	case sc.TablesTableID:
		return systemtable.Tables, nil
	case sc.ColumnsTableID:
		return systemtable.Columns, nil
	case sc.StatisticsTableID:
		return systemtable.Stats, nil
	case sc.IndexesTableID:
		return systemtable.Indexes, nil
	case sc.ColumnStatisticsTableID:
		return systemtable.ColumnStats, nil
	case sc.IndexStatisticsTableID:
		return systemtable.IndexStats, nil
	default:
		return nil, fmt.Errorf("unknown system table ID: %d", id)
	}
}

// SetSystemTableID sets the appropriate system table ID field based on table name.
// This is called during initialization to track the IDs of all system catalog tables.
//
// Parameters:
//   - tableName: Name of the system table
//   - tableID: Heap file ID assigned to this system table
func (sc *SystemTableIDs) SetSystemTableID(tableName string, tableID int) {
	switch tableName {
	case systemtable.Tables.TableName():
		sc.TablesTableID = tableID
	case systemtable.Columns.TableName():
		sc.ColumnsTableID = tableID
	case systemtable.Stats.TableName():
		sc.StatisticsTableID = tableID
	case systemtable.Indexes.TableName():
		sc.IndexesTableID = tableID
	case systemtable.ColumnStats.TableName():
		sc.ColumnStatisticsTableID = tableID
	case systemtable.IndexStats.TableName():
		sc.IndexStatisticsTableID = tableID
	}
}
