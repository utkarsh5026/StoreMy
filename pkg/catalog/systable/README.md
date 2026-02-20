# pkg/catalog/systable

Package `systable` defines the six system catalog tables that StoreMy uses to
persist database metadata, schema information, and optimizer statistics across
restarts.

---

## Overview

Every system table is modelled as two layers:

| Layer | Type | Responsibility |
|-------|------|----------------|
| Descriptor | `SystemTableDescriptor[T]` | Compile-time metadata: name, schema, primary key, tuple serialization/deserialization |
| Operations | `TablesTable`, `ColumnsTable`, … | Runtime CRUD against `catalogio.CatalogAccess`, plus domain-specific helpers |

Both layers are generic over `T` — the Go type a row deserializes into
(e.g. `TableMetadata`, `schema.ColumnMetadata`, `IndexMetadata`).

---

## System Tables

### CATALOG_TABLES — `TablesTable`

Maps every table known to the catalog to its numeric ID, canonical name, heap-file
path, and single-column primary key.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | `uint64` | Primary key |
| `table_name` | `string` | Case-insensitive in lookups |
| `file_path` | `string` | Heap file name (e.g. `users.dat`) |
| `primary_key` | `string` | Column name, or empty for composite keys |

Key methods: `GetByID`, `GetByName`, `GetAll`, `DeleteTable`

---

### CATALOG_COLUMNS — `ColumnsTable`

Stores one row per column for every table, including auto-increment state.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | `uint64` | Part of composite PK |
| `column_name` | `string` | Part of composite PK |
| `type_id` | `int` | `types.Type` enum value |
| `position` | `uint32` | 0-based column index |
| `is_primary_key` | `bool` | |
| `is_auto_increment` | `bool` | |
| `next_auto_value` | `uint64` | ≥ 1 when `is_auto_increment` is true |

Key methods: `LoadColumnMetadata`, `InsertColumns`, `GetAutoIncrementColumn`,
`IncrementAutoIncrementValue`

---

### CATALOG_INDEXES — `IndexesTable`

Tracks every secondary index created by users.

| Column | Type | Notes |
|--------|------|-------|
| `index_id` | `uint64` | Primary key |
| `index_name` | `string` | |
| `table_id` | `uint64` | Column position 2 (not 0) |
| `column_name` | `string` | Indexed column |
| `index_type` | `string` | `"BTREE"` or `"HASH"` |
| `file_path` | `string` | Index file on disk |
| `created_at` | `int64` | Unix timestamp |

Key methods: `GetIndexesByTable`, `GetIndexByName`, `GetIndexByID`,
`DeleteIndexFromCatalog`

---

### CATALOG_STATISTICS — `StatsTable`

Table-level statistics consumed by the cost-based query optimizer.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | `uint64` | Primary key |
| `cardinality` | `uint64` | Row count |
| `page_count` | `uint64` | Heap pages |
| `avg_tuple_size` | `uint64` | Bytes |
| `last_updated` | `int64` | Unix timestamp |
| `distinct_values` | `uint64` | Distinct values in primary key column |
| `null_count` | `uint64` | |
| `min_value` | `string` | Primary key min (serialized) |
| `max_value` | `string` | Primary key max (serialized) |

Key methods: `UpdateTableStatistics` (triggers a full heap scan),
`GetTableStatistics`

---

### CATALOG_COLUMN_STATISTICS — `ColumnStatsTable`

Per-column statistics for selectivity estimation.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | `uint64` | Part of composite PK |
| `column_name` | `string` | Part of composite PK |
| `column_index` | `uint32` | 0-based column position |
| `distinct_count` | `uint64` | |
| `null_count` | `uint64` | |
| `min_value` | `string` | Serialized minimum |
| `max_value` | `string` | Serialized maximum |
| `avg_width` | `uint64` | Average field width in bytes |
| `last_updated` | `int64` | Unix timestamp |

In addition to the persisted row, `CollectColumnStatistics` returns a
`ColStatsInfo` which carries typed `types.Field` min/max values, an equi-depth
`statistics.Histogram`, and a most-common-values (MCV) list — all computed
in memory during the scan.

Key methods: `CollectColumnStatistics`, `UpdateColumnStatistics`,
`GetColumnStatistics`

---

### CATALOG_INDEX_STATISTICS — `IndexStatsTable`

Per-index statistics for index-scan cost estimation.

| Column | Type | Notes |
|--------|------|-------|
| `index_id` | `uint64` | Primary key |
| `table_id` | `uint64` | |
| `index_name` | `string` | |
| `index_type` | `string` | `"BTREE"` or `"HASH"` |
| `column_name` | `string` | |
| `num_entries` | `uint64` | |
| `num_pages` | `uint64` | |
| `height` | `uint32` | B+ tree height; 1 for hash indexes |
| `distinct_keys` | `uint64` | |
| `clustering_factor` | `int` | Stored as `value × 1 000 000`; auto-decoded to `float64` |
| `avg_key_size` | `uint64` | Bytes |
| `last_updated` | `int` | Unix timestamp |

The **clustering factor** measures how well the table's physical row order
matches the index key order (0.0 = random, 1.0 = perfectly clustered). It is
computed by simulating an index scan and counting page-boundary crossings.

Key methods: `CollectIndexStatistics`, `UpdateIndexStatistics`,
`StoreIndexStatistics`, `GetIndexStatistics`

---

## Generic CRUD — `BaseOperations[T]`

All table types embed `BaseOperations[T]`, which provides:

| Method | Description |
|--------|-------------|
| `Iterate(tx, fn)` | Visit every entity; return `ErrSuccess` from `fn` to stop early |
| `FindOne(tx, pred)` | First entity matching predicate (uses early termination) |
| `FindAll(tx, pred)` | All entities matching predicate |
| `Insert(tx, entity)` | Serialize and append a new row |
| `DeleteBy(tx, pred)` | Remove all matching rows (collect then delete) |
| `Upsert(tx, pred, entity)` | Delete matching rows then insert; insert if none found |
| `UpdateBy(tx, pred, fn)` | Delete + re-insert each matching row after applying `fn` |

All operations are MVCC-safe (delete-then-insert pattern) and respect the
caller's transaction context.

---

## Initialization

`AllSystemTables` is the ordered `[]SystemTable` slice used by catalog
bootstrapping code to create or verify all six catalog files and register their
schemas during database startup.

`InvalidTableID` (value `0`) is the sentinel for an uninitialized `FileID`; no
live table may carry this value.

---

## File Layout

| File | Contents |
|------|----------|
| `docs.go` | Package-level godoc |
| `commons.go` | `BaseOperations[T]`, `TxContext`, `ErrSuccess`, `InvalidTableID` |
| `descriptors.go` | `SystemTableDescriptor[T]`, `SystemTable` interface, all six descriptor vars, `AllSystemTables` |
| `tables.go` | `TablesTable`, `TableMetadata` |
| `columns.go` | `ColumnsTable`, `AutoIncrementInfo` |
| `indexes.go` | `IndexesTable`, `IndexMetadata` |
| `stats.go` | `StatsTable`, `TableStatistics`, `StatsCacheSetter`, `FileGetter` |
| `column_stats.go` | `ColumnStatsTable`, `ColumnStatisticsRow`, `ColStatsInfo` |
| `index_stats.go` | `IndexStatsTable`, `IndexStatisticsRow` |
