# System Table Package

The `systemtable` package provides the implementation of system catalog tables for the StoreMy database. System tables store metadata about database objects such as tables, columns, indexes, and statistics.

## Overview

System tables are special tables that:
- Store metadata about the database schema and objects
- Are managed internally by the database engine
- Use the same storage engine (heap pages) as user tables
- Have reserved table IDs and are initialized at database startup
- Are not directly modifiable by users

## Available System Tables

All system tables are accessible through global instances defined in [commons.go](commons.go):

| Table | Instance | Purpose |
|-------|----------|---------|
| `CATALOG_TABLES` | `Tables` | Stores metadata about all tables (table_id, table_name, file_path, primary_key) |
| `CATALOG_COLUMNS` | `Columns` | Stores column definitions and metadata |
| `CATALOG_STATS` | `Stats` | Stores general table statistics |
| `CATALOG_INDEXES` | `Indexes` | Stores index definitions and metadata |
| `CATALOG_COLUMN_STATISTICS` | `ColumnStats` | Stores column-level statistics (distinct count, null count, min/max values, etc.) |
| `CATALOG_INDEX_STATISTICS` | `IndexStats` | Stores index-level statistics (num entries, height, clustering factor, etc.) |

Access all system tables via: `systemtable.AllSystemTables`

## SystemTable Interface

All system tables implement the `SystemTable` interface:

```go
type SystemTable interface {
    Schema() *schema.Schema      // Returns table schema definition
    TableName() string           // Returns canonical table name
    FileName() string            // Returns physical storage file name
    PrimaryKey() string          // Returns primary key column name
    TableIDIndex() int           // Returns index of table_id column
}
```

## Usage Examples

### Accessing System Tables

```go
import "storemy/pkg/catalog/systemtable"

// Access a specific system table
tablesTable := systemtable.Tables
columnsTable := systemtable.Columns
columnStats := systemtable.ColumnStats
indexStats := systemtable.IndexStats

// Iterate over all system tables
for _, sysTable := range systemtable.AllSystemTables {
    schema := sysTable.Schema()
    tableName := sysTable.TableName()
    // ... use table
}
```

### Working with Table Metadata (CATALOG_TABLES)

```go
// Create a table metadata entry
tableMetadata := systemtable.TableMetadata{
    TableID:       100,
    TableName:     "users",
    FilePath:      "/data/users.dat",
    PrimaryKeyCol: "user_id",
}

// Convert to tuple for storage
tuple := systemtable.Tables.CreateTuple(tableMetadata)

// Parse tuple back to metadata
parsedMetadata, err := systemtable.Tables.Parse(tuple)
if err != nil {
    // handle error
}
```

### Working with Column Statistics (CATALOG_COLUMN_STATISTICS)

```go
// Create column statistics
colStats := &systemtable.ColumnStatisticsRow{
    TableID:       100,
    ColumnName:    "age",
    ColumnIndex:   2,
    DistinctCount: 50,
    NullCount:     5,
    MinValue:      "18",
    MaxValue:      "65",
    AvgWidth:      4,
    LastUpdated:   time.Now(),
}

// Convert to tuple for storage
tuple := systemtable.ColumnStats.CreateTuple(colStats)

// Parse tuple back to statistics
parsedStats, err := systemtable.ColumnStats.Parse(tuple)
if err != nil {
    // handle error
}
```

### Working with Index Statistics (CATALOG_INDEX_STATISTICS)

```go
// Create index statistics
indexStats := &systemtable.IndexStatisticsRow{
    IndexID:          1,
    TableID:          100,
    IndexName:        "idx_user_email",
    IndexType:        "btree",
    ColumnName:       "email",
    NumEntries:       10000,
    NumPages:         50,
    Height:           3,
    DistinctKeys:     9950,
    ClusteringFactor: 0.85,  // 0.0-1.0 range
    AvgKeySize:       32,
    LastUpdated:      time.Now(),
}

// Convert to tuple for storage
tuple := systemtable.IndexStats.CreateTuple(indexStats)

// Parse tuple back to statistics
parsedStats, err := systemtable.IndexStats.Parse(tuple)
if err != nil {
    // handle error
}
```

## Implementation Details

### Constants

- **`InvalidTableID = -1`**: Reserved value for system table schemas and uninitialized table IDs

### Schema Building

System tables use the schema builder pattern:

```go
func (tt *TablesTable) Schema() *schema.Schema {
    sch, _ := schema.NewSchemaBuilder(InvalidTableID, tt.TableName()).
        AddPrimaryKey("table_id", types.IntType).
        AddColumn("table_name", types.StringType).
        AddColumn("file_path", types.StringType).
        AddColumn("primary_key", types.StringType).
        Build()
    return sch
}
```

### Tuple Conversion

Each system table provides methods to convert between domain objects and tuples:
- **`CreateTuple()`**: Converts domain object to tuple for storage
- **`Parse()`**: Converts tuple back to domain object
- **`GetID()` / `GetTableID()` / `GetIndexID()`**: Extracts primary key from tuple

### Data Type Storage

Some data types are stored with special encoding:
- **Timestamps**: Stored as Unix timestamps (int64)
- **Floating point** (clustering factor): Stored as integer (0-1000000) representing 0.0-1.0 range for precision

### Validation

All `Parse()` methods include validation:
- Non-negative checks for counts and sizes
- Range validation (e.g., distinct_keys ≤ num_entries)
- Non-empty string checks for required fields
- Reserved value checks (e.g., InvalidTableID)

## File Organization

```
pkg/catalog/systemtable/
├── commons.go                 # Interface definition and global instances
├── tables_table.go            # CATALOG_TABLES implementation
├── columms_table.go           # CATALOG_COLUMNS implementation
├── stats_table.go             # CATALOG_STATS implementation
├── indexes_table.go           # CATALOG_INDEXES implementation
├── column_stats_table.go      # CATALOG_COLUMN_STATISTICS implementation
├── index_stats_table.go       # CATALOG_INDEX_STATISTICS implementation
├── utils.go                   # Helper functions (getIntField, getStringField, etc.)
└── README.md                  # This file
```

## Best Practices

1. **Always validate input** when parsing tuples - use the provided Parse() methods
2. **Use global instances** (e.g., `systemtable.Tables`) rather than creating new instances
3. **Handle errors** from Parse() methods - they perform validation
4. **Use InvalidTableID** constant instead of hardcoding -1
5. **Update timestamps** when modifying statistics
6. **Maintain consistency** between related tables (e.g., CATALOG_TABLES and CATALOG_COLUMNS)

## Integration Points

System tables integrate with:
- **Catalog Manager**: Manages system table registration and initialization
- **Schema Package**: Defines column types and table structure
- **Tuple Package**: Handles tuple creation and field access
- **Storage Engine**: Persists system table data using heap files
- **Query Optimizer**: Reads statistics for query planning

## Notes

- System table names use the `CATALOG_` prefix convention
- Physical files use `.dat` extension (e.g., `catalog_tables.dat`)
- All system tables use integer primary keys
- Statistics tables support incremental updates via timestamps
