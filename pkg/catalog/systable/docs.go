// Package systable defines the six system catalog tables that StoreMy uses to
// persist database metadata, schema information, and optimizer statistics across
// restarts.
//
// # Architecture
//
// Every catalog table is modelled the same way:
//
//  1. A [SystemTableDescriptor] holds all compile-time knowledge about the table:
//     its name, on-disk filename, schema, primary key, and the functions that
//     serialize/deserialize domain values to and from raw [tuple.Tuple]s.
//     Descriptors are pure, goroutine-safe package-level variables.
//
//  2. A concrete table type (e.g. [TablesTable], [ColumnsTable]) embeds
//     [BaseOperations][T], a generic CRUD layer built on top of
//     [catalogio.CatalogAccess]. The table type adds domain-specific helpers
//     (e.g. GetByName, IncrementAutoIncrementValue) on top of the generic
//     Iterate / FindOne / FindAll / Insert / DeleteBy / Upsert / UpdateBy methods.
//
// This two-layer design keeps the descriptor layer free of I/O while allowing
// the operation layer to provide idiomatic, type-safe access without boilerplate.
//
// # System Tables
//
// The six catalog tables are registered in [AllSystemTables] and are created or
// verified automatically during database initialization.
//
// CATALOG_TABLES (managed by [TablesTable])
//
//	table_id  | table_name | file_path | primary_key
//	----------+------------+-----------+------------
//	uint64 PK | string     | string    | string
//
// Maps each table's numeric ID to its canonical name, heap-file path, and
// single-column primary key name (empty for tables with composite keys).
//
// CATALOG_COLUMNS (managed by [ColumnsTable])
//
//	table_id | column_name | type_id | position | is_primary_key | is_auto_increment | next_auto_value
//	---------+-------------+---------+----------+----------------+-------------------+----------------
//	uint64   | string      | int     | uint32   | bool           | bool              | uint64
//
// Stores one row per column per table. The effective primary key is the composite
// (table_id, column_name). Auto-increment state is maintained here; callers must
// use [ColumnsTable.IncrementAutoIncrementValue] to advance the counter safely.
//
// CATALOG_INDEXES (managed by [IndexesTable])
//
//	index_id  | index_name | table_id | column_name | index_type | file_path | created_at
//	----------+------------+----------+-------------+------------+-----------+-----------
//	uint64 PK | string     | uint64   | string      | string     | string    | int64
//
// Tracks every secondary index created by users. index_type is either "BTREE" or
// "HASH". table_id sits at column position 2 (not 0) because index_id owns the
// primary-key slot.
//
// CATALOG_STATISTICS (managed by [StatsTable])
//
//	table_id  | cardinality | page_count | avg_tuple_size | last_updated | distinct_values | null_count | min_value | max_value
//	----------+-------------+------------+----------------+--------------+-----------------+------------+-----------+---------
//	uint64 PK | uint64      | uint64     | uint64         | int64        | uint64          | uint64     | string    | string
//
// Holds table-level statistics consumed by the cost-based query optimizer.
// Populated by [StatsTable.UpdateTableStatistics], which performs a full heap scan.
//
// CATALOG_COLUMN_STATISTICS (managed by [ColumnStatsTable])
//
//	table_id | column_name | column_index | distinct_count | null_count | min_value | max_value | avg_width | last_updated
//	---------+-------------+--------------+----------------+------------+-----------+-----------+-----------+------------
//	uint64   | string      | uint32       | uint64         | uint64     | string    | string    | uint64    | int64
//
// Per-column statistics for selectivity estimation. The effective primary key is
// (table_id, column_name). In addition to the persisted row, [ColStatsInfo]
// carries typed min/max [types.Field] values, an equi-depth [statistics.Histogram],
// and the top-N most-common-value list — all computed in memory by
// [ColumnStatsTable.CollectColumnStatistics].
//
// CATALOG_INDEX_STATISTICS (managed by [IndexStatsTable])
//
//	index_id  | table_id | index_name | index_type | column_name | num_entries | num_pages | height | distinct_keys | clustering_factor | avg_key_size | last_updated
//	----------+----------+------------+------------+-------------+-------------+-----------+--------+---------------+-------------------+--------------+------------
//	uint64 PK | uint64   | string     | string     | string      | uint64      | uint64    | uint32 | uint64        | int (×1 000 000)  | uint64       | int
//
// Per-index statistics for index-scan cost estimation. clustering_factor is
// stored as a scaled integer (value × 1_000_000) to avoid floating-point
// serialization; it is automatically converted to float64 on read.
//
// # Generic CRUD — BaseOperations[T]
//
// [BaseOperations] provides the following methods, all of which respect the
// caller-supplied transaction context:
//
//   - Iterate — applies a function to every entity; supports early termination via [ErrSuccess]
//   - FindOne — returns the first entity matching a predicate (uses early termination)
//   - FindAll — returns all entities matching a predicate
//   - Insert  — serializes and appends a new entity
//   - DeleteBy — removes all entities matching a predicate (two-phase: collect then delete)
//   - Upsert  — delete-then-insert; inserts if not found
//   - UpdateBy — delete-then-insert for each matching entity (MVCC-safe update)
//
// # Initialization
//
// [AllSystemTables] is the ordered slice of every [SystemTable] descriptor.
// Catalog bootstrapping code iterates this slice to create files and register
// schemas without knowing the concrete descriptor types.
//
// # Constants and Sentinels
//
//   - [InvalidTableID] — the zero [primitives.FileID]; no live table may carry this ID
//   - [ErrSuccess] — returned from a process function to signal early-termination success
//   - [DefaultHistogramBuckets] — default bucket count for equi-depth histograms (10)
//   - [DefaultMCVCount] — default most-common-value list length (5)
package systable
